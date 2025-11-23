"""
Core processing logic for calendar post splitting.
"""
import json
import uuid
import logging
from datetime import datetime, timezone
from supabase import Client
from dateutil import parser as date_parser
from posts.utils import upsert_batch

log = logging.getLogger(__name__)


def process_calendar_posts(calendar_id: str, supabase_client: Client, current_user_id: str, current_user_role: str):
    """
    Process approved calendar posts and split them into individual posts.
    
    Args:
        calendar_id: ID of the calendar row to process
        supabase_client: Supabase client (RLS-aware)
        current_user_id: ID of the authenticated user
        current_user_role: Role of the authenticated user (admin/user)
        
    Returns:
        dict: Result dictionary with status and data
        
    Raises:
        Exception: If processing fails
    """
    log.info("📋 [PROCESSOR] ===== Starting Calendar Processing Operation =====")
    log.info(f"🚀 [PROCESSOR] Processing calendar_data row: {calendar_id} for user: {current_user_id}")
    
    # 1. Fetch the target calendar row
    log.info(f"📂 [PROCESSOR] Step 1: Fetching calendar data for ID: {calendar_id}...")
    calendar_res = supabase_client.from_("calendar_data") \
                                  .select("user_id, platform, calendar_data") \
                                  .eq("id", calendar_id) \
                                  .single() \
                                  .execute()

    if calendar_res.data is None:
        # For 'user' role, this means RLS blocked it.
        # For 'admin' role, this means it truly doesn't exist.
        log.error(f"❌ [PROCESSOR] Calendar data not found or access denied for ID: {calendar_id}")
        raise Exception("Calendar data not found or access denied")
    
    target_owner_id = calendar_res.data.get("user_id")
    platform = calendar_res.data.get("platform")
    log.info(f"✅ [PROCESSOR] Calendar data fetched successfully")
    log.info(f"📊 [PROCESSOR] Calendar owner: {target_owner_id}, Platform: {platform}")
    
    if current_user_role == 'admin':
        log.info(f"👑 [PROCESSOR] Admin {current_user_id} accessing row owned by {target_owner_id}")
    else:
        log.info(f"👤 [PROCESSOR] User {current_user_id} accessing their own row")

    row = calendar_res.data
    calendar_json = row.get("calendar_data", {})
    content_items = calendar_json.get("content_items", [])
    log.info(f"📝 [PROCESSOR] Total content items in calendar: {len(content_items) if content_items else 0}")

    # 2. Filter approved posts
    log.info(f"🔍 [PROCESSOR] Step 2: Filtering approved posts...")
    approved_posts = [post for post in content_items if post.get("status") == "approved"]
    if not approved_posts:
        log.info(f"ℹ️  [PROCESSOR] No approved posts found for calendar {calendar_id}")
        return {
            "success": True,
            "message": "No approved posts to process.",
            "approved_posts_found": 0,
            "posts_saved_count": 0
        }
    
    log.info(f"✅ [PROCESSOR] Found {len(approved_posts)} approved posts to process")

    # 3. Prepare rows for insert
    log.info(f"🔨 [PROCESSOR] Step 3: Preparing {len(approved_posts)} posts for insertion...")
    new_rows = []
    skipped_count = 0
    for idx, post in enumerate(approved_posts, 1):
        post_id = post.get("id")
        
        if not post_id:
            log.warning(f"⚠️  [PROCESSOR] Post #{idx}: Missing ID, skipping")
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
                log.debug(f"📅 [PROCESSOR] Post #{idx} ({post_id}): Scheduled for {month} {year}")
            except (ValueError, TypeError) as e:
                log.warning(f"⚠️  [PROCESSOR] Post #{idx} ({post_id}): Could not parse scheduled_datetime: {scheduled_str}")
        else:
            log.warning(f"⚠️  [PROCESSOR] Post #{idx} ({post_id}): Missing scheduled_datetime")

        image_link_to_save = post.get("image_link") 
        
        if not image_link_to_save or not isinstance(image_link_to_save, str):
            carousel_links = post.get("carousel")
            if isinstance(carousel_links, list) and len(carousel_links) > 0:
                image_link_to_save = json.dumps(carousel_links)
                log.debug(f"🖼️  [PROCESSOR] Post #{idx} ({post_id}): Using carousel with {len(carousel_links)} images")
            else:
                image_link_to_save = None
                log.warning(f"⚠️  [PROCESSOR] Post #{idx} ({post_id}): No image_link or carousel found")

        new_uuid = str(uuid.uuid4()) 
        post['status'] = 'content_in_progress'
        log.debug(f"📝 [PROCESSOR] Post #{idx} ({post_id}): Internal JSON status set to 'content_in_progress'")

        new_rows.append({
            "id": new_uuid,
            "post_id": post_id,
            "parent_calendar_id": calendar_id,
            "user_id": row.get("user_id"),  # Original owner's ID
            "platform": row.get("platform"),
            "status": "content_in_progress",  # post.get("status") this is for storing as it is status 
            "content_type": post.get("content_type"),
            "image_link": image_link_to_save,
            "scheduled_datetime": scheduled_str,
            "storage_path": post.get("storage_path"),
            "original_json": post,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_by": current_user_id
        })

    log.info(f"✅ [PROCESSOR] Prepared {len(new_rows)} rows for insertion (skipped: {skipped_count})")

    # 4. Upsert in batches
    log.info(f"💾 [PROCESSOR] Step 4: Saving {len(new_rows)} rows to database in batches...")
    BATCH_SIZE = 50  
    total_saved_count = 0
    total_batches = (len(new_rows) + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(f"📦 [PROCESSOR] Will process {total_batches} batch(es) (batch size: {BATCH_SIZE})")
    
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        log.info(f"📦 [PROCESSOR] Processing batch {batch_num}/{total_batches} ({len(batch)} rows)...")
        
        try:
            # We use the same client to write, respecting RLS for users
            # and bypassing it for admins
            saved_data = upsert_batch(batch, supabase_client) 
            total_saved_count += len(saved_data)
            log.info(f"✅ [PROCESSOR] Batch {batch_num}/{total_batches} saved successfully ({len(saved_data)} rows)")
        except Exception as e:
            log.error(f"❌ [PROCESSOR] Failed to process batch {batch_num}/{total_batches}: {e}")
            raise Exception(f"Failed to save batch: {str(e)}")

    log.info("=" * 60)
    log.info("✅ [PROCESSOR] ===== Operation Completed Successfully =====")
    log.info(f"✅ [PROCESSOR] Calendar ID: {calendar_id}")
    log.info(f"✅ [PROCESSOR] Approved posts found: {len(approved_posts)}")
    log.info(f"✅ [PROCESSOR] Posts saved to database: {total_saved_count}")
    log.info(f"✅ [PROCESSOR] User: {current_user_id} ({current_user_role})")
    log.info("=" * 60)

    return {
        "success": True,
        "message": "Successfully processed approved posts.",
        "processed_row_id": calendar_id,
        "approved_posts_found": len(approved_posts),
        "posts_saved_count": total_saved_count
    }

