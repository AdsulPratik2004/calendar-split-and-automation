import os
import logging
from functools import wraps
from flask import request, jsonify, g
from supabase import create_client, Client
from gotrue.errors import AuthApiError
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()
log = logging.getLogger(__name__)

# --- Load Config from Environment ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
AUTH_ENABLED = os.environ.get("AUTH_ENABLED", "True").lower() == 'true'

# --- Initialize Admin Client for Auth ---
auth_admin_client: Client = None
if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    try:
        auth_admin_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    except Exception as e:
        log.error(f"Failed to initialize admin Supabase client: {e}")
else:
    log.error("Supabase URL or Service Key is missing. Admin client not initialized.")


# --- Auth Decorator ---
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):

        # --- Handle case when AUTH is disabled (dev mode) ---
        if not AUTH_ENABLED:
            if not auth_admin_client:
                return jsonify({"error": "Auth is disabled but admin client failed to initialize"}), 500

            log.warning("!!! AUTHENTICATION IS DISABLED !!!")
            log.warning("Bypassing token check and using ADMIN privileges for this request.")
            g.current_user_id = "auth-disabled-admin"
            g.current_user_role = "admin"
            g.supabase_client = auth_admin_client
            return f(*args, **kwargs)

        # --- Require admin client to validate token ---
        if not auth_admin_client:
            return jsonify({"error": "Admin client for authentication is not initialized"}), 500

        # --- Extract Bearer token from Authorization header ---
        authorization = request.headers.get("Authorization")
        if not authorization:
            return jsonify({"error": "Authorization header is missing"}), 401

        try:
            token = authorization.split(" ")[1]
        except Exception:
            return jsonify({"error": "Invalid Authorization header format"}), 401

        try:
            # --- 1. Validate the token using admin client ---
            user_auth_response = auth_admin_client.auth.get_user(jwt=token)
            current_user = user_auth_response.user
            if not current_user:
                raise Exception("Invalid token or user not found")

            # --- 2. Get user's role from 'profiles' table ---
            profile_res = (
                auth_admin_client
                .from_("profiles")
                .select("role")
                .eq("id", current_user.id)
                .single()
                .execute()
            )

            if not profile_res.data:
                return jsonify({"error": "User profile not found"}), 404

            current_user_role = profile_res.data.get("role")
            log.info(f"Authenticated user {current_user.id} with role: {current_user_role}")

            # --- 3. Set user info in Flask context ---
            g.current_user_id = current_user.id
            g.current_user_role = current_user_role

            # --- 4. Use proper Supabase client based on role ---
            if current_user_role == "admin":
                log.info("Using ADMIN client (bypasses RLS).")
                g.supabase_client = auth_admin_client
            else:
                log.info("Using USER client (respects RLS).")
                user_supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

                # ✅ Only postgrest.auth(token) is needed for RLS
                user_supabase.postgrest.auth(token)

                g.supabase_client = user_supabase

        except AuthApiError as e:
            log.error(f"Supabase AuthApiError: {e.message}")
            return jsonify({"error": f"Authentication failed: {e.message}"}), 401
        except Exception as e:
            log.error(f"Error during user authentication: {e}")
            return jsonify({"error": f"Authentication failed: {str(e)}"}), 401

        # --- 5. Continue to the protected route ---
        return f(*args, **kwargs)

    return decorated_function
