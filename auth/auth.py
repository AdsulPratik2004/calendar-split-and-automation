import os
import logging
from functools import wraps
from flask import request, jsonify, g
from supabase import create_client, Client
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
        log.info("✅ [AUTH] Admin Supabase client initialized successfully")
    except Exception as e:
        log.error(f"❌ [AUTH] Failed to initialize admin Supabase client: {e}")
else:
    log.error("❌ [AUTH] Supabase URL or Service Key is missing. Admin client not initialized.")


# --- Auth Decorator ---
def token_required(f):
    """
    Decorator to protect routes that require authentication.
    Extracts token from Authorization header, validates it, and sets up
    the appropriate Supabase client based on user role (respects RLS).
    
    Sets in Flask context (g):
    - g.current_user_id: authenticated user ID
    - g.current_user_role: user role (admin/user)
    - g.supabase_client: RLS-aware Supabase client
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        log.info("🔐 [AUTH] ===== Starting Authentication Process =====")

        # --- Handle case when AUTH is disabled (dev mode) ---
        if not AUTH_ENABLED:
            log.warning("⚠️  [AUTH] AUTHENTICATION IS DISABLED (dev mode)")
            if not auth_admin_client:
                log.error("❌ [AUTH] Auth is disabled but admin client failed to initialize")
                return jsonify({"error": "Auth is disabled but admin client failed to initialize"}), 500

            log.info("✅ [AUTH] Bypassing token check and using ADMIN privileges")
            g.current_user_id = "auth-disabled-admin"
            g.current_user_role = "admin"
            g.supabase_client = auth_admin_client
            log.info("✅ [AUTH] Authentication bypassed - proceeding to route handler")
            return f(*args, **kwargs)

        # --- Require admin client to validate token ---
        if not auth_admin_client:
            log.error("❌ [AUTH] Admin client for authentication is not initialized")
            return jsonify({"error": "Admin client for authentication is not initialized"}), 500

        log.info("✅ [AUTH] Admin client available, proceeding with token validation")

        # --- Extract Bearer token from Authorization header ---
        log.info("📥 [AUTH] Step 1: Extracting token from Authorization header")
        authorization = request.headers.get("Authorization")
        if not authorization:
            log.error("❌ [AUTH] Authorization header is missing")
            return jsonify({"error": "Authorization header is missing"}), 401

        try:
            token = authorization.split(" ")[1]
            if not token:
                raise Exception("Token not found")
            log.info("✅ [AUTH] Token extracted successfully from header")
        except Exception:
            log.error("❌ [AUTH] Invalid Authorization header format")
            return jsonify({"error": "Invalid Authorization header format. Expected: 'Bearer <token>'"}), 401

        try:
            # --- 1. Validate the token using admin client ---
            log.info("🔍 [AUTH] Step 2: Validating token with Supabase...")
            user_auth_response = auth_admin_client.auth.get_user(jwt=token)
            current_user = user_auth_response.user
            if not current_user:
                raise Exception("Invalid token or user not found")
            log.info(f"✅ [AUTH] Token validated successfully - User ID: {current_user.id}")

            # --- 2. Get user's role from 'profiles' table ---
            log.info(f"👤 [AUTH] Step 3: Fetching user role from profiles table for user: {current_user.id}")
            profile_res = (
                auth_admin_client
                .from_("profiles")
                .select("role")
                .eq("id", current_user.id)
                .single()
                .execute()
            )

            if not profile_res.data:
                log.error(f"❌ [AUTH] User profile not found for user: {current_user.id}")
                return jsonify({"error": "User profile not found"}), 404

            current_user_role = profile_res.data.get("role")
            log.info(f"✅ [AUTH] User role retrieved: {current_user_role} for user: {current_user.id}")

            # --- 3. Set user info in Flask context ---
            log.info("💾 [AUTH] Step 4: Setting user info in Flask context")
            g.current_user_id = current_user.id
            g.current_user_role = current_user_role
            log.info(f"✅ [AUTH] User context set - ID: {g.current_user_id}, Role: {g.current_user_role}")

            # --- 4. Use proper Supabase client based on role ---
            log.info(f"🔧 [AUTH] Step 5: Setting up Supabase client for role: {current_user_role}")
            if current_user_role == "admin":
                log.info("👑 [AUTH] Using ADMIN client (bypasses RLS)")
                g.supabase_client = auth_admin_client
            else:
                log.info("👤 [AUTH] Creating USER client with RLS enabled")
                user_supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
                # Set the token for RLS to work properly
                user_supabase.postgrest.auth(token)
                g.supabase_client = user_supabase
                log.info("✅ [AUTH] USER client created with RLS enabled (token authenticated)")

            log.info("✅ [AUTH] ===== Authentication Successful =====")
            log.info(f"✅ [AUTH] User: {g.current_user_id}, Role: {g.current_user_role}, Client: {'ADMIN' if current_user_role == 'admin' else 'USER (RLS)'}")

        except Exception as e:
            # Check if exception has a message attribute, otherwise use str(e)
            error_message = getattr(e, 'message', str(e))
            log.error(f"❌ [AUTH] Error during user authentication: {error_message}")
            return jsonify({"error": f"Authentication failed: {error_message}"}), 401

        # --- 5. Continue to the protected route ---
        log.info("➡️  [AUTH] Proceeding to protected route handler...")
        return f(*args, **kwargs)

    return decorated_function
