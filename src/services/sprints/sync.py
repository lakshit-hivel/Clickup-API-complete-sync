"""
Sprints sync module - handles sprint/list synchronization from ClickUp
"""
from src.integrations.clickup_api import get_folderlesslists
from src.db.database import insert_sprints_to_db, insert_folderless_list_to_db
from src.mappers.mappers import map_list_to_sprint, map_folderless_list_to_sprint
from src.services.issues.sync import sync_tasks
from src.core.logger import logger


def should_include_list(clickup_list, date_threshold_ms):
    """
    Determine if a list should be included and how to fetch its tasks.
    
    Uses sprint-level date filtering as primary filter:
    - If list has due_date >= threshold: include all tasks (no task-level date filter)
    - If list has due_date < threshold: skip list entirely
    - If list has no due_date: use task-level date filtering as fallback
    
    Args:
        clickup_list: The ClickUp list object
        date_threshold_ms: The date threshold in milliseconds
        
    Returns:
        tuple: (should_include: bool, use_task_date_filter: bool)
            - should_include: True if this list should be processed
            - use_task_date_filter: True if task-level date filtering should be applied
    """
    if date_threshold_ms is None:
        # No date filtering requested - include all
        return (True, False)
    
    due_date_ms = clickup_list.get('due_date')
    
    if due_date_ms:
        # List has a due date - check if it falls within our range
        if int(due_date_ms) >= date_threshold_ms:
            # Due date is on or after threshold - include all tasks
            return (True, False)
        else:
            # Due date is before threshold - skip this list entirely
            return (False, False)
    else:
        # No due date - use task-level filtering
        return (True, True)


def sync_sprint(clickup_list, folder_id, board_id, now, org_id, conn):
    """Insert a single sprint and return its ID"""
    list_name = clickup_list.get('name')
    sprint_data = map_list_to_sprint(clickup_list, folder_id, board_id, now, org_id)
    sprint_id = insert_sprints_to_db(sprint_data, conn)
    logger.info(f"Inserting sprint: {list_name} (id: {sprint_id})")
    return sprint_id


def sync_folderless_lists(api_token, space_id, space_name, org_id, conn, now, date_updated_gt, orphan_board_id):
    """Sync folderless lists for a space, return counts"""
    lists_count = 0
    issues_count = 0
    pr_mappings_count = 0
    list_to_sprint_id = {}
    
    logger.info(f"Fetching folderless lists from space: {space_name}")
    try:
        folderless_lists = get_folderlesslists(api_token, space_id)
        logger.info(f"Found {len(folderless_lists)} folderless lists")
        
        for fl_list in folderless_lists:
            fl_list_id = fl_list.get('id')
            fl_list_name = fl_list.get('name')
            
            should_include, use_task_filter = should_include_list(fl_list, date_updated_gt)
            if not should_include:
                logger.debug(f"Skipping folderless list '{fl_list_name}' - due date before threshold")
                continue
            
            fl_sprint_data = map_folderless_list_to_sprint(fl_list, now, org_id)
            logger.debug(f"Inserting folderless sprint: {fl_list_name}")
            
            try:
                fl_sprint_id = insert_folderless_list_to_db(fl_sprint_data, conn)
                list_to_sprint_id[fl_list_id] = fl_sprint_id
                lists_count += 1
                logger.info(f"Folderless sprint inserted with id: {fl_sprint_id}")
                
                # Fetch and insert tasks using sync_tasks helper
                task_date_filter = date_updated_gt if use_task_filter else None
                task_result = sync_tasks(api_token, fl_list_id, orphan_board_id, fl_sprint_id, space_id, now, conn, org_id, task_date_filter)
                issues_count += task_result['tasks']
                pr_mappings_count += task_result['pr_mappings']
                
            except Exception as e:
                logger.warning(f"Failed to insert folderless sprint '{fl_list_name}': {e}")
                
    except Exception as e:
        logger.warning(f"Failed to fetch folderless lists for space '{space_name}': {e}")
    
    return {
        'lists': lists_count,
        'issues': issues_count,
        'pr_mappings': pr_mappings_count,
        'list_to_sprint_id': list_to_sprint_id,
    }
