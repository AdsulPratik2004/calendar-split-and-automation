import os
import uuid
import logging
import json
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from pydantic import BaseModel, ValidationError
from supabase import Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import httpx 
from dateutil import parser as date_parser
from auth import token_required
from datetime import datetime, timezone  # <-- FIX 1: Import timezone

# --- Load Environment Variables ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not SUPABASE_ANON_KEY:
    raise EnvironmentError("SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, and SUPABASE_ANON_KEY must be set in .env")

# --- Initialize Flask App ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Dynamic CORS from .env file ---
default_origins = "http://localhost:8080,https://app.digibility.ai"
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", default_origins).split(',')
log.info(f"Allowing origins: {CORS_ORIGINS}")
CORS(app, origins=CORS_ORIGINS, supports_credentials=True)

# --- Pydantic Model (for validation) ---
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
@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),          # Wait 2 seconds between failures
    retry=retry_if_exception_type((httpx.ReadError, httpx.ConnectError))  # Only retry on these errors
)
def upsert_batch(batch: list, client: Client):
    """
    Tries to upsert a single batch of rows to Supabase using the provided client.
    Will retry on ReadError or ConnectError.
    """
    # FIX 2: Corrected table name typo 'calander' to 'calendar'
    log.info(f"💾 [BATCH] Attempting to upsert {len(batch)} rows to 'individual_calendar_posts' table...")
    try:
        # FIX 2: Corrected table name typo 'calander' to 'calendar'
        response = client.from_("posts") \
                         .upsert(batch, on_conflict="post_id") \
                         .execute()
        
        if response.data is None and response.error:
             log.error(f"❌ [BATCH] Supabase API error: {response.error.message}")
             # Raise a specific error to be caught by the main route
             raise Exception(f"Supabase error: {response.error.message}")
        
        log.info(f"✅ [BATCH] Successfully upserted {len(response.data)} rows to database")
        return response.data
    
    except httpx.ReadError as e:
        log.warning(f"⚠️  [BATCH] ReadError during batch upsert (will retry): {e}")
        raise  # Reraise to trigger tenacity retry
    
    except httpx.ConnectError as e:
        log.warning(f"⚠️  [BATCH] ConnectError during batch upsert (will retry): {e}")
        raise  # Reraise to trigger tenacity retry
    
    except Exception as e:
        log.error(f"❌ [BATCH] Non-retriable error during batch upsert: {e}")
        raise  # Re-raise to be caught by the main route


# --- API Endpoint (Flask) ---
@app.route("/split-calendar", methods=["POST"])
@token_required  # <-- Authorization handled by auth.py
def process_approved_posts():
    """
    Process approved calendar posts and split them into individual posts.
    Requires authentication token in Authorization header.
    """
    log.info("📋 [MAIN] ===== Starting Calendar Processing Operation =====")
    
    # --- Auth is already handled by the decorator ---
    # We can now access the user info and client from `g`
    current_user_id = g.current_user_id
    current_user_role = g.current_user_role
    supabase_client = g.supabase_client  # This is the RLS-aware client

    log.info(f"👤 [MAIN] Authenticated user: {current_user_id} (Role: {current_user_role})")
    log.info(f"🔧 [MAIN] Using Supabase client: {'ADMIN (RLS bypassed)' if current_user_role == 'admin' else 'USER (RLS enabled)'}")

    # --- Get Payload ---
    log.info("📥 [MAIN] Step 1: Extracting and validating request payload...")
    data = request.get_json()
    if not data:
        log.error("❌ [MAIN] Invalid JSON payload received")
        return jsonify({"error": "Invalid JSON payload"}), 400

    try:
        payload = CalendarPayload(**data)
        calendar_id = payload.calendarRowId
        log.info(f"✅ [MAIN] Payload validated - Calendar ID: {calendar_id}")
    except ValidationError as e:
        log.error(f"❌ [MAIN] Invalid payload format: {e}")
        return jsonify({"error": "Invalid payload: calendarRowId is required"}), 400

    log.info(f"🚀 [MAIN] Processing calendar_data row: {calendar_id} for user: {current_user_id}")

    try:
        # 1. Fetch the target calendar row
        log.info(f"📂 [MAIN] Step 2: Fetching calendar data for ID: {calendar_id}...")
        # We use the client from 'g' which is RLS-aware (or admin)
        calendar_res = supabase_client.from_("calendar_data") \
                                      .select("user_id, platform, calendar_data") \
                                      .eq("id", calendar_id) \
                                      .single() \
                                      .execute()

        if calendar_res.data is None:
            # For 'user' role, this means RLS blocked it.
            # For 'admin' role, this means it truly doesn't exist.
            log.error(f"❌ [MAIN] Calendar data not found or access denied for ID: {calendar_id}")
            return jsonify({"error": "Calendar data not found or access denied"}), 404
        
        target_owner_id = calendar_res.data.get("user_id")
        platform = calendar_res.data.get("platform")
        log.info(f"✅ [MAIN] Calendar data fetched successfully")
        log.info(f"📊 [MAIN] Calendar owner: {target_owner_id}, Platform: {platform}")
        
        if current_user_role == 'admin':
            log.info(f"👑 [MAIN] Admin {current_user_id} accessing row owned by {target_owner_id}")
        else:
            log.info(f"👤 [MAIN] User {current_user_id} accessing their own row")

        row = calendar_res.data
        calendar_json = row.get("calendar_data", {})
        content_items = calendar_json.get("content_items", [])
        log.info(f"📝 [MAIN] Total content items in calendar: {len(content_items) if content_items else 0}")

        # 2. Filter approved posts
        log.info(f"🔍 [MAIN] Step 3: Filtering approved posts...")
        approved_posts = [post for post in content_items if post.get("status") == "ApprovedCalendar"]
        if not approved_posts:
            log.info(f"ℹ️  [MAIN] No approved posts found for calendar {calendar_id}")
            return jsonify({"message": "No approved posts to process."}), 200
        
        log.info(f"✅ [MAIN] Found {len(approved_posts)} approved posts to process")

        # 3. Prepare rows for insert
        log.info(f"🔨 [MAIN] Step 4: Preparing {len(approved_posts)} posts for insertion...")
        new_rows = []
        skipped_count = 0
        for idx, post in enumerate(approved_posts, 1):
            post_id = post.get("id")
            
            if not post_id:
                log.warning(f"⚠️  [MAIN] Post #{idx}: Missing ID, skipping")
                skipped_count += 1
                continue 

            scheduled_str = post.get("scheduled_datetime")
            month = None
            year = None

            if scheduled_str:
                try:
                    dt = date_parser.isoparse(scheduled_str)
                    month = dt.strftime("%B") 
                    year = dt.year
                    log.debug(f"📅 [MAIN] Post #{idx} ({post_id}): Scheduled for {month} {year}")
                except (ValueError, TypeError) as e:
                    log.warning(f"⚠️  [MAIN] Post #{idx} ({post_id}): Could not parse scheduled_datetime: {scheduled_str}")
            else:
                log.warning(f"⚠️  [MAIN] Post #{idx} ({post_id}): Missing scheduled_datetime")

            image_link_to_save = post.get("image_link") 
            
            if not image_link_to_save or not isinstance(image_link_to_save, str):
                carousel_links = post.get("carousel")
                if isinstance(carousel_links, list) and len(carousel_links) > 0:
                    image_link_to_save = json.dumps(carousel_links)
                    log.debug(f"🖼️  [MAIN] Post #{idx} ({post_id}): Using carousel with {len(carousel_links)} images")
                else:
                    image_link_to_save = None
                    log.warning(f"⚠️  [MAIN] Post #{idx} ({post_id}): No image_link or carousel found")

            new_uuid = str(uuid.uuid4()) 

            new_rows.append({
                "id": new_uuid,
                "post_id": post_id,
                "parent_calendar_id": calendar_id,
                "user_id": row.get("user_id"),  # Original owner's ID
                "platform": row.get("platform"),
                "status": "PendingApprovalCalendar", # post.get("status") this is for storing as it is status 
                "content_type": post.get("content_type"),
                "image_link": image_link_to_save,
                "scheduled_datetime": scheduled_str,
                "storage_path": post.get("storage_path"),
                "original_json": post,
                "created_at": datetime.now(timezone.utc).isoformat(),  # <-- FIX 1: Use timezone-aware datetime
                "updated_at": datetime.now(timezone.utc).isoformat(),  # <-- FIX 1: Use timezone-aware datetime
                "updated_by": current_user_id
            })

            

        log.info(f"✅ [MAIN] Prepared {len(new_rows)} rows for insertion (skipped: {skipped_count})")

        # 4. Upsert in batches
        log.info(f"💾 [MAIN] Step 5: Saving {len(new_rows)} rows to database in batches...")
        BATCH_SIZE = 50  
        total_saved_count = 0
        total_batches = (len(new_rows) + BATCH_SIZE - 1) // BATCH_SIZE
        log.info(f"📦 [MAIN] Will process {total_batches} batch(es) (batch size: {BATCH_SIZE})")
        
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            log.info(f"📦 [MAIN] Processing batch {batch_num}/{total_batches} ({len(batch)} rows)...")
            
            try:
                # We use the same client to write, respecting RLS for users
                # and bypassing it for admins
                saved_data = upsert_batch(batch, supabase_client) 
                total_saved_count += len(saved_data)
                log.info(f"✅ [MAIN] Batch {batch_num}/{total_batches} saved successfully ({len(saved_data)} rows)")
            except Exception as e:
                log.error(f"❌ [MAIN] Failed to process batch {batch_num}/{total_batches}: {e}")
                return jsonify({"error": f"Failed to save batch: {str(e)}"}), 500

        log.info("=" * 60)
        log.info("✅ [MAIN] ===== Operation Completed Successfully =====")
        log.info(f"✅ [MAIN] Calendar ID: {calendar_id}")
        log.info(f"✅ [MAIN] Approved posts found: {len(approved_posts)}")
        log.info(f"✅ [MAIN] Posts saved to database: {total_saved_count}")
        log.info(f"✅ [MAIN] User: {current_user_id} ({current_user_role})")
        log.info("=" * 60)

        return jsonify({
            "message": "Successfully processed approved posts.",
            "processed_row_id": calendar_id,
            "approved_posts_found": len(approved_posts),
            "posts_saved_count": total_saved_count
        }), 200

    except Exception as e:
        log.error("=" * 60)
        log.error(f"❌ [MAIN] ===== Operation Failed =====")
        log.error(f"❌ [MAIN] Calendar ID: {calendar_id}")
        log.error(f"❌ [MAIN] Error: {str(e)}")
        log.exception(f"❌ [MAIN] Unhandled error in /split-calendar")
        log.error("=" * 60)
        return jsonify({"error": str(e)}), 500

# --- Run the app with `python main.py` ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

