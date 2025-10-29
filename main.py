import os
import uuid
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import httpx  # Import httpx to catch its specific errors
from dateutil import parser as date_parser # For robust date parsing

# --- Load Environment Variables ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

# --- Initialize Supabase Client ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# --- Initialize FastAPI App ---
app = FastAPI(title="Calendar Post Processor")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Enable CORS ---
# ❗️ Make sure your React app's URL is in origins
origins = [
    "http://localhost:8080", # Your local dev environment
    # "https://your-production-app.com" # Add your deployed frontend URL
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Model ---
class CalendarPayload(BaseModel):
    calendarRowId: str

# --- Helper: Validate UUID ---
def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

# --- Helper: Batch Upsert Function with Retries ---
# This is the new, robust function
@retry(
    stop=stop_after_attempt(3), # Try up to 3 times
    wait=wait_fixed(2),         # Wait 2 seconds between failures
    retry=retry_if_exception_type((httpx.ReadError, httpx.ConnectError)) # Only retry on these errors
)
def upsert_batch(batch: list):
    """
    Tries to upsert a single batch of rows to Supabase.
    Will retry on ReadError or ConnectError.
    """
    log.info(f"Attempting to upsert batch of {len(batch)} rows...")
    try:
        # --- FIX: Use the new 'post_id' column for conflict resolution ---
        response = supabase.from_("individual_calander_posts") \
                           .upsert(batch, on_conflict="post_id") \
                           .execute()
        
        # Check for Supabase API errors (e.g., policy violations)
        if response.data is None and response.error:
             log.error(f"SupABASE API error: {response.error.message}")
             # Don't retry API errors, they are permanent
             raise HTTPException(status_code=400, detail=f"Supabase error: {response.error.message}")
        
        log.info(f"Successfully upserted batch of {len(response.data)} rows.")
        return response.data
    
    except httpx.ReadError as e:
        log.warning(f"ReadError during batch upsert (will retry): {e}")
        raise # Reraise to trigger tenacity retry
    
    except httpx.ConnectError as e:
        log.warning(f"ConnectError during batch upsert (will retry): {e}")
        raise # Reraise to trigger tenacity retry
    
    except Exception as e:
        log.error(f"Non-retriable error during batch upsert: {e}")
        # Re-raise as a standard exception
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# --- API Endpoint (Updated) ---
@app.post("/split-calendar")
async def process_approved_posts(payload: CalendarPayload):
    calendar_id = payload.calendarRowId
    log.info(f"Processing calendar_data row: {calendar_id}")

    try:
        # 1. Fetch calendar_data row
        response = supabase.from_("calendar_data") \
                           .select("user_id, platform, calendar_data") \
                           .eq("id", calendar_id) \
                           .single() \
                           .execute()

        if response.data is None:
            raise HTTPException(status_code=404, detail="Calendar data not found.")

        row = response.data
        calendar_json = row.get("calendar_data", {})
        content_items = calendar_json.get("content_items", [])

        # 2. Filter approved posts
        approved_posts = [item for item in content_items if item.get("status") == "ApprovedCalendar"]
        if not approved_posts:
            log.info(f"No approved posts found for calendar {calendar_id}.")
            return {"message": "No approved posts to process."}
        
        log.info(f"Found {len(approved_posts)} approved posts to process.")

        # 3. Prepare rows for insert
        new_rows = []
        for item in approved_posts:
            # --- FIX: Get the original post ID from the JSON ---
            post_id = item.get("id")
            
            # If the ID is missing or empty, we can't save it
            if not post_id:
                log.warning(f"Missing ID in item. Skipping item. Original item: {item}")
                continue # Skip this item
            # --- End of fix ---

            # --- Extract month and year ---
            scheduled_str = item.get("scheduled_datetime")
            month = None
            year = None

            if scheduled_str:
                try:
                    # Use dateutil.parser for robust parsing of ISO strings
                    dt = date_parser.isoparse(scheduled_str)
                    # --- FIX: Change month to the full month name (string) ---
                    month = dt.strftime("%B") # e.g., "October"
                    year = dt.year            # e.g., 2025
                except (ValueError, TypeError) as e:
                    log.warning(f"Could not parse scheduled_datetime: {scheduled_str} for item {post_id}. Error: {e}")
            else:
                log.warning(f"Missing scheduled_datetime for item {post_id}")

            # --- FIX 3: Explicitly generate and send BOTH 'id' (UUID) and 'post_id' (text) ---
            new_uuid = str(uuid.uuid4()) # Generate a new UUID for the primary key

            new_rows.append({
                "id": new_uuid,     
                "post_id": post_id,
                "parent_calendar_id": calendar_id,
                "user_id": row.get("user_id"),
                "platform": row.get("platform"),
                "status": item.get("status"),
                "content_type": item.get("content_type"),
                "image_link": item.get("image_link"),
                "scheduled_datetime": scheduled_str,
                "storage_path": item.get("storage_path"),
                "original_json": item,
                "month": month,
                "year": year,
            })

        # 4. Upsert in batches
        BATCH_SIZE = 50  # Send 50 posts at a time. You can tune this.
        total_saved_count = 0
        
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i:i + BATCH_SIZE]
            log.info(f"Processing batch {i // BATCH_SIZE + 1}...")
            
            try:
                saved_data = upsert_batch(batch) # Use the new retry-enabled function
                total_saved_count += len(saved_data)
            except Exception as e:
                # If retries fail, log the error and stop
                # --- FIX: Corrected typo BLAST_BATCH_SIZE -> BATCH_SIZE ---
                log.error(f"Failed to process batch {i // BATCH_SIZE + 1}: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to save batch: {str(e)}")

        return {
            "message": "Successfully processed approved posts.",
            "processed_row_id": calendar_id,
            "approved_posts_found": len(approved_posts),
            "posts_saved_count": total_saved_count
        }

    except Exception as e:
        log.exception(f"Unhandled error in /split-calendar for {calendar_id}")
        if isinstance(e, HTTPException):
            raise e # Re-raise if it's an error we already handled
        raise HTTPException(status_code=500, detail=str(e))

