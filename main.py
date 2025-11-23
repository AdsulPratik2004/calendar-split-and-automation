import os
import logging
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from pydantic import ValidationError
from dotenv import load_dotenv
from auth.auth import token_required
from posts.models import CalendarPayload
from posts.processor import process_calendar_posts

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


# --- API Endpoint (Flask) ---
@app.route("/split-calendar", methods=["POST"])
@token_required  # <-- Authorization handled by auth module
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

    # --- Process calendar posts using the processor module ---
    try:
        result = process_calendar_posts(
            calendar_id=calendar_id,
            supabase_client=supabase_client,
            current_user_id=current_user_id,
            current_user_role=current_user_role
        )
        
        if not result.get("success"):
            return jsonify({"error": result.get("message", "Processing failed")}), 500
        
        # Handle case when no approved posts found
        if result.get("posts_saved_count", 0) == 0:
            return jsonify({
                "message": result.get("message", "No approved posts to process."),
                "approved_posts_found": result.get("approved_posts_found", 0),
                "posts_saved_count": result.get("posts_saved_count", 0)
            }), 200
        
        return jsonify({
            "message": result.get("message", "Successfully processed approved posts."),
            "processed_row_id": result.get("processed_row_id"),
            "approved_posts_found": result.get("approved_posts_found", 0),
            "posts_saved_count": result.get("posts_saved_count", 0)
        }), 200

    except Exception as e:
        log.error("=" * 60)
        log.error(f"❌ [MAIN] ===== Operation Failed =====")
        log.error(f"❌ [MAIN] Calendar ID: {calendar_id if 'calendar_id' in locals() else 'unknown'}")
        log.error(f"❌ [MAIN] Error: {str(e)}")
        log.exception(f"❌ [MAIN] Unhandled error in /split-calendar")
        log.error("=" * 60)
        return jsonify({"error": str(e)}), 500

# --- Run the app with `python main.py` ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

