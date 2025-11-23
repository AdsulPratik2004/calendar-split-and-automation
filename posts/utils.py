"""
Utility functions for post processing.
"""
import uuid
import logging
from supabase import Client
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import httpx

log = logging.getLogger(__name__)


def is_valid_uuid(val):
    """
    Validate if a value is a valid UUID.
    
    Args:
        val: Value to validate
        
    Returns:
        bool: True if valid UUID, False otherwise
    """
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),          # Wait 2 seconds between failures
    retry=retry_if_exception_type((httpx.ReadError, httpx.ConnectError))  # Only retry on these errors
)
def upsert_batch(batch: list, client: Client):
    """
    Tries to upsert a single batch of rows to Supabase using the provided client.
    Will retry on ReadError or ConnectError.
    
    Args:
        batch: List of rows to upsert
        client: Supabase client instance
        
    Returns:
        list: Upserted data from response
        
    Raises:
        Exception: If upsert fails after retries
    """
    log.info(f"💾 [BATCH] Attempting to upsert {len(batch)} rows to 'posts' table...")
    try:
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

