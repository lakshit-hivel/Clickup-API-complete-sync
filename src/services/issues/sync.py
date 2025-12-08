"""
Issues sync module - handles task synchronization from ClickUp
"""
from src.integrations.clickup_api import get_tasks_from_list, get_custom_list_fields
from src.db.database import insert_issue_to_db, insert_list_custom_field_to_db, insert_activity_issue_mapping
from src.mappers.mappers import map_task_to_issue, map_list_custom_field_to_custom_field, map_pr_id_to_issue_id
from src.core.logger import logger


def sync_list_custom_fields(api_token, list_id, org_id, conn):
    """Fetch and insert custom fields for a list, return count"""
    count = 0
    try:
        list_custom_fields = get_custom_list_fields(api_token, list_id)
        for cf in list_custom_fields:
            cf_data = map_list_custom_field_to_custom_field(cf, org_id)
            insert_list_custom_field_to_db(cf_data, conn)
            count += 1
        if list_custom_fields:
            logger.info(f"Inserted {len(list_custom_fields)} list custom fields")
    except Exception as e:
        logger.warning(f"Failed to sync list custom fields: {e}")
    return count


def sync_tasks(api_token, list_id, board_id, sprint_id, space_id, now, conn, org_id, date_filter=None):
    """Fetch and insert tasks for a sprint, return counts"""
    tasks_count = 0
    pr_mappings_count = 0
    
    tasks = get_tasks_from_list(api_token, list_id, date_filter)
    
    for task in tasks:
        issue_data = map_task_to_issue(task, board_id, sprint_id, space_id, now, conn, org_id, api_token)
        insert_issue_to_db(issue_data, conn)
        tasks_count += 1
        
        # Create PR mapping if applicable
        try:
            pr_mapping = map_pr_id_to_issue_id(task, conn, org_id)
            if pr_mapping:
                insert_activity_issue_mapping(pr_mapping, conn)
                pr_mappings_count += 1
        except Exception as e:
            logger.warning(f"Failed to create PR mapping for task '{task.get('name')}': {e}")
    
    logger.info(f"Inserted {tasks_count} tasks")
    return {'tasks': tasks_count, 'pr_mappings': pr_mappings_count}
