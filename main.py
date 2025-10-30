import os
import uuid
import logging
import json
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from pydantic import BaseModel, ValidationError
from supabase import create_client, Client
from gotrue.errors import AuthApiError
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import httpx 
from dateutil import parser as date_parser
from functools import wraps

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

# --- *** FIX 1: DYNAMIC CORS FROM .env FILE *** ---
# Reverting to the flexible .env file method.
# Defaults to localhost:8080 (for dev) and your deployed app URL.
default_origins = "http://localhost:8080,https://app.digibility.ai"
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", default_origins).split(',')
log.info(f"Allowing origins: {CORS_ORIGINS}")
CORS(app, origins=CORS_ORIGINS, supports_credentials=True)
# --- END OF FIX ---

# --- Initialize ADMIN Supabase Client ---
# This client uses the Service Role and bypasses RLS.
admin_supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


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

# --- Auth Decorator ---
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        authorization = request.headers.get("Authorization")
        if authorization is None:
            return jsonify({"error": "Authorization header is missing"}), 401
        
        token = None
        try:
            token = authorization.split(" ")[1]
            if not token:
                raise Exception("Token not found")
        except Exception:
            return jsonify({"error": "Invalid Authorization header format"}), 401

        try:
            # 1. Validate the token and get the user
            user_auth_response = admin_supabase.auth.get_user(jwt=token)
            current_user = user_auth_response.user
            if not current_user:
                raise Exception("Invalid token or user not found")

            # 2. Get the user's role from the 'profiles' table
            profile_res = admin_supabase.from_("profiles").select("role").eq("id", current_user.id).single().execute()
            
            if not profile_res.data:
                return jsonify({"error": "User profile not found"}), 404
            
            current_user_role = profile_res.data.get("role")
            log.info(f"Authenticated user {current_user.id} with role: {current_user_role}")

            # 3. Attach user info and the correct client to the request context (g)
            g.current_user_id = current_user.id
            g.current_user_role = current_user_role

            if current_user_role == 'admin':
                # Admins use the admin client to bypass RLS
                log.info("Using ADMIN client (bypasses RLS).")
                g.supabase_client = admin_supabase
            else:
                # --- *** FIX 2: CORRECT v2 AUTH FOR USERS *** ---
                # Users get a new client authenticated as themselves (respects RLS)
                log.info("Using USER client (respects RLS).")
                
                # 1. Create a client with the ANON key
                user_supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
                
                # 2. Correct v2 Syntax: Apply the user's token to the 'postgrest' client
                user_supabase.postgrest.auth(token)
                
                # 3. Attach this RLS-aware client to the request
                g.supabase_client = user_supabase
                # --- END OF FIX ---
        
        except AuthApiError as e:
            log.error(f"Supabase AuthApiError: {e.message}")
            return jsonify({"error": f"Authentication failed: {e.message}"}), 401
        except Exception as e:
            log.error(f"Error during user authentication: {e}")
            return jsonify({"error": f"Authentication failed: {str(e)}"}), 401
        
        return f(*args, **kwargs)
    return decorated_function


# --- Helper: Batch Upsert Function with Retries ---
@retry(
    stop=stop_after_attempt(3), # Try up to 3 times
    wait=wait_fixed(2),         # Wait 2 seconds between failures
    retry=retry_if_exception_type((httpx.ReadError, httpx.ConnectError)) # Only retry on these errors
)
def upsert_batch(batch: list, client: Client):
    """
    Tries to upsert a single batch of rows to Supabase using the provided client.
    Will retry on ReadError or ConnectError.
    """
    log.info(f"Attempting to upsert batch of {len(batch)} rows...")
    try:
        response = client.from_("individual_calander_posts") \
                         .upsert(batch, on_conflict="post_id") \
                         .execute()
        
        if response.data is None and response.error:
             log.error(f"SUPABASE API error: {response.error.message}")
             # Raise a specific error to be caught by the main route
             raise Exception(f"Supabase error: {response.error.message}")
        
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
        raise # Re-raise to be caught by the main route


# --- API Endpoint (Flask) ---
@app.route("/split-calendar", methods=["POST"])
@token_required # <-- This decorator now handles all auth
def process_approved_posts():
    
    # --- Auth is already handled by the decorator ---
    # We can now access the user info and client from `g`
    current_user_id = g.current_user_id
    current_user_role = g.current_user_role
    supabase_client = g.supabase_client # This is the RLS-aware client

    # --- Get Payload ---
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    try:
        payload = CalendarPayload(**data)
        calendar_id = payload.calendarRowId
    except ValidationError as e:
        return jsonify({"error": "Invalid payload: calendarRowId is required"}), 400

    log.info(f"User {current_user_id} processing calendar_data row: {calendar_id}")

    try:
        # 1. Fetch the target calendar row
        # We use the client from 'g' which is RLS-aware (or admin)
        calendar_res = supabase_client.from_("calendar_data") \
                                      .select("user_id, platform, calendar_data") \
                                      .eq("id", calendar_id) \
                                      .single() \
                                      .execute()

        if calendar_res.data is None:
            # For 'user' role, this means RLS blocked it.
            # For 'admin' role, this means it truly doesn't exist.
            return jsonify({"error": "Calendar data not found or access denied"}), 404
        
        target_owner_id = calendar_res.data.get("user_id")
        if current_user_role == 'admin':
            log.info(f"Admin {current_user_id} accessing row owned by {target_owner_id}.")
        else:
            log.info(f"User {current_user_id} accessing their own row.")

        row = calendar_res.data
        calendar_json = row.get("calendar_data", {})
        content_items = calendar_json.get("content_items", [])

        # 2. Filter approved posts
        approved_posts = [item for item in content_items if item.get("status") == "ApprovedCalendar"]
        if not approved_posts:
            log.info(f"No approved posts found for calendar {calendar_id}.")
            return jsonify({"message": "No approved posts to process."}), 200
        
        log.info(f"Found {len(approved_posts)} approved posts to process.")

        # 3. Prepare rows for insert
        new_rows = []
        for item in approved_posts:
            post_id = item.get("id")
            
            if not post_id:
                log.warning(f"Missing ID in item. Skipping item. Original item: {item}")
                continue 

            scheduled_str = item.get("scheduled_datetime")
            month = None
            year = None

            if scheduled_str:
                try:
                    dt = date_parser.isoparse(scheduled_str)
                    month = dt.strftime("%B") 
                    year = dt.year          
                except (ValueError, TypeError) as e:
                    log.warning(f"Could not parse scheduled_datetime: {scheduled_str} for item {post_id}. Error: {e}")
            else:
                log.warning(f"Missing scheduled_datetime for item {post_id}")

            image_link_to_save = item.get("image_link") 
            
            if not image_link_to_save or not isinstance(image_link_to_save, str):
                carousel_links = item.get("carousel")
                if isinstance(carousel_links, list) and len(carousel_links) > 0:
                    image_link_to_save = json.dumps(carousel_links)
                else:
                    image_link_to_save = None

            new_uuid = str(uuid.uuid4()) 

            new_rows.append({
                "id": new_uuid,      
                "post_id": post_id, 
                "parent_calendar_id": calendar_id,
                "user_id": row.get("user_id"), # This is the original owner's ID
                "platform": row.get("platform"),
                "status": item.get("status"),
                "content_type": item.get("content_type"),
                "image_link": image_link_to_save, 
                "scheduled_datetime": scheduled_str, 
                "storage_path": item.get("storage_path"),
                "original_json": item,
                "month": month,
                "year": year,
            })

        # 4. Upsert in batches
        BATCH_SIZE = 50  
        total_saved_count = 0
        
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i:i + BATCH_SIZE]
            log.info(f"Processing batch {i // BATCH_SIZE + 1}...")
            
            try:
                # We use the same client to write, respecting RLS for users
                # and bypassing it for admins
                saved_data = upsert_batch(batch, supabase_client) 
                total_saved_count += len(saved_data)
            except Exception as e:
                log.error(f"Failed to process batch {i // BATCH_SIZE + 1}: {e}")
                return jsonify({"error": f"Failed to save batch: {str(e)}"}), 500

        return jsonify({
            "message": "Successfully processed approved posts.",
            "processed_row_id": calendar_id,
            "approved_posts_found": len(approved_posts),
            "posts_saved_count": total_saved_count
        }), 200

    except Exception as e:
        log.exception(f"Unhandled error in /split-calendar for {calendar_id}")
        return jsonify({"error": str(e)}), 500

# --- Add this block to run the app with `python main.py` ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

