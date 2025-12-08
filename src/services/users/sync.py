"""
Users sync module - handles user synchronization from ClickUp
"""
from src.integrations.clickup_api import get_users
from src.db.database import insert_user_to_db
from src.mappers.mappers import map_users_to_usertable
from src.core.logger import logger


def sync_users(api_token, org_id, conn):
    """Fetch and insert all users, return count"""
    logger.info("Fetching Users...")
    count = 0
    try:
        users = get_users(api_token)
        logger.info(f"Found {len(users)} users")
        
        for user in users:
            user_data = map_users_to_usertable(user, org_id)
            logger.debug(f"Inserting user: {user_data.get('name')} (email encrypted)")
            insert_user_to_db(user_data, conn)
            count += 1
        
        logger.info(f"Successfully processed {count} users")
    except Exception as e:
        logger.warning(f"Failed to sync users: {e}")
        logger.debug("Continuing with main sync...")
    return count
