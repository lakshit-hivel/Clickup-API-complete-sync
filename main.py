import sys
import traceback
from datetime import datetime

from clickup_api import (get_clickup_spaces, get_folders, get_lists_from_folder, get_tasks_from_list, 
                          get_custom_task_types, get_workspace_custom_fields, get_space_custom_fields,
                          get_folder_custom_fields, get_custom_list_fields, get_users, get_folderlesslists)
from database import (insert_boards_to_db, insert_sprints_to_db, insert_issue_to_db, get_db_connection, 
                      insert_custom_field_to_db, insert_workspace_custom_field_to_db, 
                      insert_space_custom_field_to_db, insert_folder_custom_field_to_db, 
                      insert_list_custom_field_to_db, insert_user_to_db, insert_activity_issue_mapping,
                      insert_folderless_list_to_db, update_sync_status, upsert_board_sync_status,
                      get_clickup_user_integration_id)
from mappers import (map_folder_to_board, map_list_to_sprint, map_task_to_issue, 
                     map_custom_task_type_to_custom_field, map_workspace_custom_field_to_custom_field,
                     map_space_custom_field_to_custom_field, map_folder_custom_field_to_custom_field,
                     map_list_custom_field_to_custom_field, map_users_to_usertable, map_pr_id_to_issue_id,
                     map_folderless_list_to_sprint, map_board_status)

# Hardcoded board ID for folderless lists (orphan board)
ORPHAN_BOARD_ID = 10011


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
    print(f"  Inserting sprint: {list_name} (id: {sprint_id})")
    return sprint_id


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
            print(f"    Inserted {len(list_custom_fields)} list custom fields")
    except Exception as e:
        print(f"    Warning: Failed to sync list custom fields: {e}")
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
            print(f"    Warning: Failed to create PR mapping for task '{task.get('name')}': {e}")
    
    print(f"    Inserted {tasks_count} tasks")
    return {'tasks': tasks_count, 'pr_mappings': pr_mappings_count}


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
    print(f"  Found {len(lists)} lists, {len(lists_with_start_date)} with start dates")
    
    for clickup_list in lists_with_start_date:
        list_id = clickup_list.get('id')
        list_name = clickup_list.get('name')
        
        should_include, use_task_filter = should_include_list(clickup_list, date_updated_gt)
        if not should_include:
            print(f"  Skipping list '{list_name}' - due date before threshold")
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


def sync_users(api_token, org_id, conn):
    """Fetch and insert all users, return count"""
    print("\nFetching Users...")
    count = 0
    try:
        users = get_users(api_token)
        print(f"Found {len(users)} users")
        
        for user in users:
            user_data = map_users_to_usertable(user, org_id)
            print(f"  Inserting user: {user_data.get('name')} (email encrypted)")
            insert_user_to_db(user_data, conn)
            count += 1
        
        print(f"✓ Successfully processed {count} users\n")
    except Exception as e:
        print(f"Warning: Failed to sync users: {e}")
        traceback.print_exc()
        print("Continuing with main sync...\n")
    return count


def sync_custom_task_types(api_token, team_id, org_id, conn):
    """Fetch and insert custom task types, return count"""
    print("\nFetching Custom Task Types (Custom Fields)...")
    count = 0
    try:
        custom_task_types = get_custom_task_types(api_token, team_id)
        print(f"Found {len(custom_task_types)} custom task types")
        
        for ctt in custom_task_types:
            cf_data = map_custom_task_type_to_custom_field(ctt, org_id)
            print(f"  Inserting custom field: {cf_data.get('name')}")
            insert_custom_field_to_db(cf_data, conn)
            count += 1
        
        print(f"✓ Successfully processed {count} custom fields\n")
    except Exception as e:
        print(f"Warning: Failed to sync custom fields: {e}")
        print("Continuing with main sync...\n")
    return count


def sync_workspace_custom_fields(api_token, team_id, org_id, conn):
    """Fetch and insert workspace-level custom fields, return count"""
    print("\nFetching Workspace Custom Fields...")
    count = 0
    try:
        ws_fields = get_workspace_custom_fields(api_token, team_id)
        print(f"Found {len(ws_fields)} workspace custom fields")
        
        for ws_field in ws_fields:
            field_data = map_workspace_custom_field_to_custom_field(ws_field, org_id)
            print(f"  Inserting workspace custom field: {field_data.get('name')}")
            insert_workspace_custom_field_to_db(field_data, conn)
            count += 1
        
        print(f"✓ Successfully processed {count} workspace custom fields\n")
    except Exception as e:
        print(f"Warning: Failed to sync workspace custom fields: {e}")
        print("Continuing with main sync...\n")
    return count


def sync_space_custom_fields(api_token, space_id, org_id, conn):
    """Fetch and insert space-level custom fields, return count"""
    count = 0
    try:
        space_fields = get_space_custom_fields(api_token, space_id)
        print(f"  Found {len(space_fields)} space custom fields")
        
        for sf in space_fields:
            field_data = map_space_custom_field_to_custom_field(sf, org_id)
            print(f"    Inserting space custom field: {field_data.get('name')}")
            insert_space_custom_field_to_db(field_data, conn)
            count += 1
    except Exception as e:
        print(f"  Warning: Failed to sync space custom fields: {e}")
    return count


def sync_folder_custom_fields(api_token, folder_id, folder_name, org_id, conn):
    """Fetch and insert folder-level custom fields, return count"""
    count = 0
    try:
        folder_fields = get_folder_custom_fields(api_token, folder_id)
        print(f"    Found {len(folder_fields)} folder custom fields")
        
        for ff in folder_fields:
            field_data = map_folder_custom_field_to_custom_field(ff, org_id)
            print(f"      Inserting folder custom field: {field_data.get('name')}")
            insert_folder_custom_field_to_db(field_data, conn)
            count += 1
    except Exception as e:
        print(f"    Warning: Failed to sync folder custom fields for folder '{folder_name}': {e}")
    return count


def sync_folderless_lists(api_token, space_id, space_name, org_id, conn, now, date_updated_gt, orphan_board_id):
    """Sync folderless lists for a space, return counts"""
    lists_count = 0
    issues_count = 0
    pr_mappings_count = 0
    list_to_sprint_id = {}
    
    print(f"\n  Fetching folderless lists from space: {space_name}")
    try:
        folderless_lists = get_folderlesslists(api_token, space_id)
        print(f"    Found {len(folderless_lists)} folderless lists")
        
        for fl_list in folderless_lists:
            fl_list_id = fl_list.get('id')
            fl_list_name = fl_list.get('name')
            
            should_include, use_task_filter = should_include_list(fl_list, date_updated_gt)
            if not should_include:
                print(f"    Skipping folderless list '{fl_list_name}' - due date before threshold")
                continue
            
            fl_sprint_data = map_folderless_list_to_sprint(fl_list, now, org_id)
            print(f"    Inserting folderless sprint: {fl_list_name}")
            
            try:
                fl_sprint_id = insert_folderless_list_to_db(fl_sprint_data, conn)
                list_to_sprint_id[fl_list_id] = fl_sprint_id
                lists_count += 1
                print(f"    ✓ Folderless sprint inserted with id: {fl_sprint_id}")
                
                # Fetch and insert tasks using sync_tasks helper
                task_date_filter = date_updated_gt if use_task_filter else None
                task_result = sync_tasks(api_token, fl_list_id, orphan_board_id, fl_sprint_id, space_id, now, conn, org_id, task_date_filter)
                issues_count += task_result['tasks']
                pr_mappings_count += task_result['pr_mappings']
                
            except Exception as e:
                print(f"    Warning: Failed to insert folderless sprint '{fl_list_name}': {e}")
                
    except Exception as e:
        print(f"  Warning: Failed to fetch folderless lists for space '{space_name}': {e}")
    
    return {
        'lists': lists_count,
        'issues': issues_count,
        'pr_mappings': pr_mappings_count,
        'list_to_sprint_id': list_to_sprint_id,
    }


# =============DATA SYNC FUNCTIONS HERE=============

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
    from database import get_board_by_id, get_clickup_user_integration_id
    
    print(f"\nStarting Single Board Sync for board_id={board_id}, org_id={org_id}\n")
    
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
        
        print(f"Found board: {board_name} (ClickUp folder_id: {clickup_folder_id})")
        
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
        
        print(f"\n✓ Sync completed: {result['sprints']} sprints, {result['issues']} issues, {result['pr_mappings']} PR mappings")
        return summary
        
    except Exception as e:
        print(f"\n✗ Single board sync failed: {e}")
        traceback.print_exc()
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed")


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
    print(f"Starting ClickUp Full Sync for org_id={org_id}\n")
    
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
        
        # Sync using helper functions
        users_count = sync_users(api_token, org_id, conn)
        custom_fields_count = sync_custom_task_types(api_token, team_id, org_id, conn)
        workspace_custom_fields_count = sync_workspace_custom_fields(api_token, team_id, org_id, conn)
        
        # Update sync status to 'sync in progress' (users and custom fields done, now processing spaces/tasks)
        update_sync_status(org_id, 'IN_PROGRESS', conn)
        
        # Fetch spaces
        print("\nFetching and Inserting Data...")
        spaces = get_clickup_spaces(api_token, team_id)
        print(f"Found {len(spaces)} spaces")
        
        # Process each space
        for space in spaces:
            space_id = space.get('id')
            space_name = space.get('name')
            print(f"\nProcessing space: {space_name}")
            
            # Sync space custom fields using helper
            space_custom_fields_count += sync_space_custom_fields(api_token, space_id, org_id, conn)
            
            # Fetch folders (boards)
            folders = get_folders(api_token, space_id)
            print(f"  Found {len(folders)} folders (boards)")
            
            for folder in folders:
                folder_id = folder.get('id')
                folder_name = folder.get('name')
                # Per-board counters
                board_issue_count = 0
                board_sprint_count = 0
                
                # Map folder to board and insert immediately
                board_data = map_folder_to_board(folder, space_id, now, org_id)
                print(f"  Inserting board: {folder_name}")
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
                
                # Sync folder custom fields using helper
                folder_custom_fields_count += sync_folder_custom_fields(api_token, folder_id, folder_name, org_id, conn)
                
                # Call helper function to sync sprints and tasks for this board
                print(f"    Fetching sprints from folder: {folder_name}")
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
            
            # Sync folderless lists using helper
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
        
        # Print Summary (in sync order)
        print("\n" + "="*60)
        print("SYNC SUMMARY")
        print("="*60)
        print(f"Total Users: {users_count}")
        print(f"Total Custom Fields (Task Types): {custom_fields_count}")
        print(f"Total Workspace Custom Fields: {workspace_custom_fields_count}")
        print(f"Total Space Custom Fields: {space_custom_fields_count}")
        print(f"Total Boards (Folders): {len(folder_to_board_id)}")
        print(f"Total Folder Custom Fields: {folder_custom_fields_count}")
        print(f"Total Sprints (Lists): {sprints_count + len(list_to_sprint_id)}")
        print(f"Total List Custom Fields: {list_custom_fields_count}")
        print(f"Total Issues (Tasks): {issues_count}")
        print(f"Total Folderless Lists: {folderless_lists_count}")
        print(f"Total Folderless Issues: {folderless_issues_count}")
        print(f"Total PR-to-Issue Mappings: {pr_mappings_count}")
        
        print("\n" + "="*60)
        print("✓ SYNC COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        # Update sync status to 'sync completed'
        update_sync_status(org_id, 'COMPLETED', conn)
        
        return summary
        
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        traceback.print_exc()
        raise
    finally:
        # Always close the connection when done
        if conn:
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    # For standalone execution, you can provide test values here
    # or fetch from config/environment
    from database import get_clickup_access_token
    from clickup_api import get_authorized_teams
    
    # Example: hardcoded for testing (replace with your values)
    TEST_ORG_ID = "xxxxx"
    
    conn = get_db_connection()
    try:
        api_token = get_clickup_access_token("CLICKUP", TEST_ORG_ID, conn)
        if not api_token:
            print(f"Error: No access token found for org_id={TEST_ORG_ID}")
            sys.exit(1)
    finally:
        conn.close()
    
    team_id = get_authorized_teams(api_token)
    if not team_id:
        print("Error: Could not fetch team_id from ClickUp API")
        sys.exit(1)
    
    sync_clickup_data(TEST_ORG_ID, api_token, team_id)
