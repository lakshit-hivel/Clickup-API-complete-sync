"""
Boards sync module - handles board synchronization from ClickUp
"""
from datetime import datetime

from src.integrations.clickup_api import get_lists_from_folder
from src.db.database import (get_db_connection, insert_boards_to_db, upsert_board_sync_status,
                              get_board_by_id, get_clickup_user_integration_id)
from src.mappers.mappers import map_folder_to_board, map_board_status
from src.core.logger import logger

from src.services.sprints.sync import should_include_list, sync_sprint
from src.services.issues.sync import sync_tasks, sync_list_custom_fields


def sync_board_content(board_id, clickup_folder_id, org_id, api_token, conn, now, date_updated_gt=None):
    """
    Syncs all sprints and tasks for a single board.
    Does NOT manage DB connection or board status - caller handles those.
    """
    sprints_count = 0
    issues_count = 0
    list_custom_fields_count = 0
    pr_mappings_count = 0
    
    # Fetch lists (sprints) for this folder
    lists = get_lists_from_folder(api_token, clickup_folder_id)
    lists_with_start_date = [lst for lst in lists if lst.get('start_date')]
    logger.info(f"Found {len(lists)} lists, {len(lists_with_start_date)} with start dates")
    
    for clickup_list in lists_with_start_date:
        list_id = clickup_list.get('id')
        list_name = clickup_list.get('name')
        
        should_include, use_task_filter = should_include_list(clickup_list, date_updated_gt)
        if not should_include:
            logger.debug(f"Skipping list '{list_name}' - due date before threshold")
            continue
        
        # Insert sprint using helper
        sprint_id = sync_sprint(clickup_list, clickup_folder_id, board_id, now, org_id, conn)
        sprints_count += 1
        
        # Sync list custom fields using helper
        list_custom_fields_count += sync_list_custom_fields(api_token, list_id, org_id, conn)
        
        # Sync tasks using helper
        task_date_filter = date_updated_gt if use_task_filter else None
        space_id = clickup_list.get('space', {}).get('id') if clickup_list.get('space') else None
        task_result = sync_tasks(api_token, list_id, board_id, sprint_id, space_id, now, conn, org_id, task_date_filter)
        issues_count += task_result['tasks']
        pr_mappings_count += task_result['pr_mappings']
    
    return {
        'sprints': sprints_count,
        'issues': issues_count,
        'list_custom_fields': list_custom_fields_count,
        'pr_mappings': pr_mappings_count,
    }


def sync_single_board(board_id, org_id, api_token, date_updated_gt=None):
    """Sync sprints and tasks for a single existing board
    
    Args:
        board_id: Database board ID (id column from board table)
        org_id: Organization ID
        api_token: ClickUp API token
        date_updated_gt: Optional timestamp (ms) to filter tasks updated after this date
        
    Returns:
        dict: Summary of sync results for this board
    """
    logger.info(f"Starting Single Board Sync for board_id={board_id}, org_id={org_id}")
    
    conn = get_db_connection()
    
    try:
        # Fetch board info from database
        board_info = get_board_by_id(board_id, conn)
        if not board_info:
            raise Exception(f"Board with id={board_id} not found in database")
        
        clickup_folder_id = board_info['clickup_folder_id']
        board_name = board_info['name']
        
        if str(board_info['org_id']) != str(org_id):
            raise Exception(f"Board org_id ({board_info['org_id']}) does not match provided org_id ({org_id})")
        
        logger.info(f"Found board: {board_name} (ClickUp folder_id: {clickup_folder_id})")
        
        user_integration_id = get_clickup_user_integration_id('CLICKUP', org_id, conn)
        now = datetime.now()
        
        # Mark board sync as in progress
        folder_mock = {'id': clickup_folder_id, 'name': board_name}
        board_status_start = map_board_status(
            board=folder_mock, board_id=board_id, user_integration_id=user_integration_id,
            now=now, org_id=org_id, sync_status='IN_PROGRESS', issue_count=0, sprint_count=0,
        )
        upsert_board_sync_status(board_status_start, conn)
        
        # Call helper function to sync sprints and tasks
        result = sync_board_content(board_id, clickup_folder_id, org_id, api_token, conn, now, date_updated_gt)
        
        # Mark board sync as completed
        board_status = map_board_status(
            board=folder_mock, board_id=board_id, user_integration_id=user_integration_id,
            now=now, org_id=org_id, sync_status='COMPLETED',
            issue_count=result['issues'], sprint_count=result['sprints'],
        )
        upsert_board_sync_status(board_status, conn)
        
        summary = {
            'board_id': board_id,
            'board_name': board_name,
            **result,  # sprints, issues, list_custom_fields, pr_mappings
        }
        
        logger.info(f"Sync completed: {result['sprints']} sprints, {result['issues']} issues, {result['pr_mappings']} PR mappings")
        return summary
        
    except Exception as e:
        logger.error(f"Single board sync failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")
