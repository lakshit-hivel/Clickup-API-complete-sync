"""
CustomFields sync module - handles custom field synchronization from ClickUp
"""
from src.integrations.clickup_api import (get_custom_task_types, get_workspace_custom_fields, 
                                           get_space_custom_fields, get_folder_custom_fields)
from src.db.database import (insert_custom_field_to_db, insert_workspace_custom_field_to_db,
                              insert_space_custom_field_to_db, insert_folder_custom_field_to_db)
from src.mappers.mappers import (map_custom_task_type_to_custom_field, map_workspace_custom_field_to_custom_field,
                                  map_space_custom_field_to_custom_field, map_folder_custom_field_to_custom_field)
from src.core.logger import logger


def sync_custom_task_types(api_token, team_id, org_id, conn):
    """Fetch and insert custom task types, return count"""
    logger.info("Fetching Custom Task Types (Custom Fields)...")
    count = 0
    try:
        custom_task_types = get_custom_task_types(api_token, team_id)
        logger.info(f"Found {len(custom_task_types)} custom task types")
        
        for ctt in custom_task_types:
            cf_data = map_custom_task_type_to_custom_field(ctt, org_id)
            logger.debug(f"Inserting custom field: {cf_data.get('name')}")
            insert_custom_field_to_db(cf_data, conn)
            count += 1
        
        logger.info(f"Successfully processed {count} custom fields")
    except Exception as e:
        logger.warning(f"Failed to sync custom fields: {e}")
    return count


def sync_workspace_custom_fields(api_token, team_id, org_id, conn):
    """Fetch and insert workspace-level custom fields, return count"""
    logger.info("Fetching Workspace Custom Fields...")
    count = 0
    try:
        ws_fields = get_workspace_custom_fields(api_token, team_id)
        logger.info(f"Found {len(ws_fields)} workspace custom fields")
        
        for ws_field in ws_fields:
            field_data = map_workspace_custom_field_to_custom_field(ws_field, org_id)
            logger.debug(f"Inserting workspace custom field: {field_data.get('name')}")
            insert_workspace_custom_field_to_db(field_data, conn)
            count += 1
        
        logger.info(f"Successfully processed {count} workspace custom fields")
    except Exception as e:
        logger.warning(f"Failed to sync workspace custom fields: {e}")
    return count


def sync_space_custom_fields(api_token, space_id, org_id, conn):
    """Fetch and insert space-level custom fields, return count"""
    count = 0
    try:
        space_fields = get_space_custom_fields(api_token, space_id)
        logger.info(f"Found {len(space_fields)} space custom fields")
        
        for sf in space_fields:
            field_data = map_space_custom_field_to_custom_field(sf, org_id)
            logger.debug(f"Inserting space custom field: {field_data.get('name')}")
            insert_space_custom_field_to_db(field_data, conn)
            count += 1
    except Exception as e:
        logger.warning(f"Failed to sync space custom fields: {e}")
    return count


def sync_folder_custom_fields(api_token, folder_id, folder_name, org_id, conn):
    """Fetch and insert folder-level custom fields, return count"""
    count = 0
    try:
        folder_fields = get_folder_custom_fields(api_token, folder_id)
        logger.info(f"Found {len(folder_fields)} folder custom fields")
        
        for ff in folder_fields:
            field_data = map_folder_custom_field_to_custom_field(ff, org_id)
            logger.debug(f"Inserting folder custom field: {field_data.get('name')}")
            insert_folder_custom_field_to_db(field_data, conn)
            count += 1
    except Exception as e:
        logger.warning(f"Failed to sync folder custom fields for folder '{folder_name}': {e}")
    return count
