"""
Sync orchestrator - coordinates the full ClickUp sync process

This module serves as the main sync service, delegating to
domain-specific sync modules (boards, sprints, issues, users, custom_fields).
"""
import sys
from datetime import datetime

from src.integrations.clickup_api import get_clickup_spaces, get_folders
from src.db.database import (get_db_connection, insert_boards_to_db, update_sync_status, 
                              upsert_board_sync_status, get_clickup_user_integration_id)
from src.mappers.mappers import map_folder_to_board, map_board_status
from src.core.logger import logger

# Import from service modules
from src.services.boards.sync import sync_board_content, sync_single_board
from src.services.sprints.sync import sync_folderless_lists
from src.services.issues.sync import sync_list_custom_fields
from src.services.users.sync import sync_users
from src.services.custom_fields.sync import (sync_custom_task_types, sync_workspace_custom_fields, 
                                               sync_space_custom_fields, sync_folder_custom_fields)

# Hardcoded board ID for folderless lists (orphan board)
ORPHAN_BOARD_ID = 10011


def sync_clickup_data(org_id, api_token, team_id, date_updated_gt=None):
    """Main sync function - fetches ClickUp data and saves to database
    
    Args:
        org_id: Organization ID to sync data for
        api_token: ClickUp API token
        team_id: ClickUp team/workspace ID
        date_updated_gt: Optional timestamp (ms) to filter tasks updated after this date
        
    Returns:
        dict: Summary of sync results
    """
    logger.info(f"Starting ClickUp Full Sync for org_id={org_id}")
    
    # Create single database connection for entire sync
    conn = get_db_connection()
    
    try:
        # Update sync status to 'sync started'
        update_sync_status(org_id, 'SYNC_STARTED', conn)

        # Fetch user_integration_id once per org (used in data_sync_process)
        user_integration_id = get_clickup_user_integration_id('CLICKUP', org_id, conn)
        now = datetime.now()
        
        # Initialize data collectors
        folder_to_board_id = {}  # Map ClickUp folder_id to database board_id
        list_to_sprint_id = {}   # Map ClickUp list_id to database sprint_id (for folderless lists)
        sprints_count = 0  # Total sprints from boards
        issues_count = 0  # Count of processed issues
        custom_fields_count = 0  # Count of processed custom fields
        workspace_custom_fields_count = 0
        space_custom_fields_count = 0
        folder_custom_fields_count = 0
        list_custom_fields_count = 0
        users_count = 0  # Count of processed users
        pr_mappings_count = 0  # Count of PR-to-issue mappings created
        folderless_lists_count = 0  # Count of processed folderless lists
        folderless_issues_count = 0  # Count of issues from folderless lists
        board_statuses = []  # Per-board issue/sprint counts
        
        # Sync using domain modules
        users_count = sync_users(api_token, org_id, conn)
        custom_fields_count = sync_custom_task_types(api_token, team_id, org_id, conn)
        workspace_custom_fields_count = sync_workspace_custom_fields(api_token, team_id, org_id, conn)
        
        # Update sync status to 'sync in progress' (users and custom fields done, now processing spaces/tasks)
        update_sync_status(org_id, 'IN_PROGRESS', conn)
        
        # Fetch spaces
        logger.info("Fetching and Inserting Data...")
        spaces = get_clickup_spaces(api_token, team_id)
        logger.info(f"Found {len(spaces)} spaces")
        
        # Process each space
        for space in spaces:
            space_id = space.get('id')
            space_name = space.get('name')
            logger.info(f"Processing space: {space_name}")
            
            # Sync space custom fields using domain module
            space_custom_fields_count += sync_space_custom_fields(api_token, space_id, org_id, conn)
            
            # Fetch folders (boards)
            folders = get_folders(api_token, space_id)
            logger.info(f"Found {len(folders)} folders (boards)")
            
            for folder in folders:
                folder_id = folder.get('id')
                folder_name = folder.get('name')
                # Per-board counters
                board_issue_count = 0
                board_sprint_count = 0
                
                # Map folder to board and insert immediately
                board_data = map_folder_to_board(folder, space_id, now, org_id)
                logger.debug(f"Inserting board: {folder_name}")
                board_id = insert_boards_to_db(board_data, conn)  # Pass connection
                folder_to_board_id[folder_id] = board_id  # Store mapping
                
                # Mark board sync as started / in progress (use DB board_id and user_integration_id FK)
                board_status_start = map_board_status(
                    board=folder,
                    board_id=board_id,
                    user_integration_id=user_integration_id,
                    now=now,
                    org_id=org_id,
                    sync_status='IN_PROGRESS',
                    issue_count=0,
                    sprint_count=0,
                )
                upsert_board_sync_status(board_status_start, conn)
                
                # Sync folder custom fields using domain module
                folder_custom_fields_count += sync_folder_custom_fields(api_token, folder_id, folder_name, org_id, conn)
                
                # Call domain module to sync sprints and tasks for this board
                logger.debug(f"Fetching sprints from folder: {folder_name}")
                result = sync_board_content(board_id, folder_id, org_id, api_token, conn, now, date_updated_gt)
                
                # Update counters from result
                board_sprint_count = result['sprints']
                board_issue_count = result['issues']
                sprints_count += result['sprints']
                list_custom_fields_count += result['list_custom_fields']
                issues_count += result['issues']
                pr_mappings_count += result['pr_mappings']
                
                # After finishing this board, record its per-board status/counts and mark as completed
                board_status = map_board_status(
                    board=folder,
                    board_id=board_id,
                    user_integration_id=user_integration_id,
                    now=now,
                    org_id=org_id,
                    sync_status='COMPLETED',
                    issue_count=board_issue_count,
                    sprint_count=board_sprint_count,
                )
                upsert_board_sync_status(board_status, conn)
                board_statuses.append(board_status)
            
            # Sync folderless lists using domain module
            fl_result = sync_folderless_lists(api_token, space_id, space_name, org_id, conn, now, date_updated_gt, ORPHAN_BOARD_ID)
            folderless_lists_count += fl_result['lists']
            folderless_issues_count += fl_result['issues']
            pr_mappings_count += fl_result['pr_mappings']
            list_to_sprint_id.update(fl_result['list_to_sprint_id'])
        
        # Build summary
        summary = {
            'users': users_count,
            'custom_fields': custom_fields_count,
            'workspace_custom_fields': workspace_custom_fields_count,
            'space_custom_fields': space_custom_fields_count,
            'folder_custom_fields': folder_custom_fields_count,
            'list_custom_fields': list_custom_fields_count,
            'boards': len(folder_to_board_id),
            'sprints': sprints_count + len(list_to_sprint_id),  # Board sprints + folderless sprints
            'folderless_lists': folderless_lists_count,
            'issues': issues_count,
            'folderless_issues': folderless_issues_count,
            'pr_mappings': pr_mappings_count,
            'board_statuses': board_statuses,
        }
        
        # Log Summary
        logger.info("=" * 60)
        logger.info("SYNC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Users: {users_count}")
        logger.info(f"Total Custom Fields (Task Types): {custom_fields_count}")
        logger.info(f"Total Workspace Custom Fields: {workspace_custom_fields_count}")
        logger.info(f"Total Space Custom Fields: {space_custom_fields_count}")
        logger.info(f"Total Boards (Folders): {len(folder_to_board_id)}")
        logger.info(f"Total Folder Custom Fields: {folder_custom_fields_count}")
        logger.info(f"Total Sprints (Lists): {sprints_count + len(list_to_sprint_id)}")
        logger.info(f"Total List Custom Fields: {list_custom_fields_count}")
        logger.info(f"Total Issues (Tasks): {issues_count}")
        logger.info(f"Total Folderless Lists: {folderless_lists_count}")
        logger.info(f"Total Folderless Issues: {folderless_issues_count}")
        logger.info(f"Total PR-to-Issue Mappings: {pr_mappings_count}")
        logger.info("=" * 60)
        logger.info("SYNC COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
        # Update sync status to 'sync completed'
        update_sync_status(org_id, 'COMPLETED', conn)
        
        return summary
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise
    finally:
        # Always close the connection when done
        if conn:
            conn.close()
            logger.debug("Database connection closed")
