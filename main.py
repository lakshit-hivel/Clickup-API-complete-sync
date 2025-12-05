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
        list_to_sprint_id = {}   # Map ClickUp list_id to database sprint_id
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
        
        # Orphan board ID for folderless lists (from your mapper)
        ORPHAN_BOARD_ID = 10011
        
        # Fetch and insert users - first operation
        print("\nFetching Users...")
        try:
            users = get_users(api_token)
            print(f"Found {len(users)} users")
            
            # Process all users
            for user in users:
                user_data = map_users_to_usertable(user, org_id)
                username = user_data.get('name')
                print(f"  Inserting user: {username} (email encrypted)")
                insert_user_to_db(user_data, conn)
                users_count += 1
            
            print(f"✓ Successfully processed {users_count} users\n")
        except Exception as e:
            print(f"Warning: Failed to sync users: {e}")
            traceback.print_exc()
            print("Continuing with main sync...\n")
        
        # Fetch and insert custom task types (custom fields)
        print("\nFetching Custom Task Types (Custom Fields)...")
        try:
            custom_task_types = get_custom_task_types(api_token, team_id)
            print(f"Found {len(custom_task_types)} custom task types")
            
            for custom_task_type in custom_task_types:
                custom_field_data = map_custom_task_type_to_custom_field(custom_task_type, org_id)
                print(f"  Inserting custom field: {custom_field_data.get('name')}")
                insert_custom_field_to_db(custom_field_data, conn)
                custom_fields_count += 1
            
            print(f"✓ Successfully processed {custom_fields_count} custom fields\n")
        except Exception as e:
            print(f"Warning: Failed to sync custom fields: {e}")
            print("Continuing with main sync...\n")
        
        # Fetch and insert workspace-level custom fields
        print("\nFetching Workspace Custom Fields...")
        try:
            workspace_custom_fields = get_workspace_custom_fields(api_token, team_id)
            print(f"Found {len(workspace_custom_fields)} workspace custom fields")
            
            for workspace_custom_field in workspace_custom_fields:
                workspace_field_data = map_workspace_custom_field_to_custom_field(workspace_custom_field, org_id)
                print(f"  Inserting workspace custom field: {workspace_field_data.get('name')}")
                insert_workspace_custom_field_to_db(workspace_field_data, conn)
                workspace_custom_fields_count += 1
            
            print(f"✓ Successfully processed {workspace_custom_fields_count} workspace custom fields\n")
        except Exception as e:
            print(f"Warning: Failed to sync workspace custom fields: {e}")
            print("Continuing with main sync...\n")
        
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
            
            # Fetch and insert space-level custom fields
            try:
                space_custom_fields = get_space_custom_fields(api_token, space_id)
                print(f"  Found {len(space_custom_fields)} space custom fields")
                
                for space_custom_field in space_custom_fields:
                    space_field_data = map_space_custom_field_to_custom_field(space_custom_field, org_id)
                    print(f"    Inserting space custom field: {space_field_data.get('name')}")
                    insert_space_custom_field_to_db(space_field_data, conn)
                    space_custom_fields_count += 1
            except Exception as e:
                print(f"  Warning: Failed to sync space custom fields for space '{space_name}': {e}")
            
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
                
                # Fetch and insert folder-level custom fields
                try:
                    folder_custom_fields = get_folder_custom_fields(api_token, folder_id)
                    print(f"    Found {len(folder_custom_fields)} folder custom fields")
                    
                    for folder_custom_field in folder_custom_fields:
                        folder_field_data = map_folder_custom_field_to_custom_field(folder_custom_field, org_id)
                        print(f"      Inserting folder custom field: {folder_field_data.get('name')}")
                        insert_folder_custom_field_to_db(folder_field_data, conn)
                        folder_custom_fields_count += 1
                except Exception as e:
                    print(f"    Warning: Failed to sync folder custom fields for folder '{folder_name}': {e}")
                
                # Fetch lists (sprints) for this folder
                print(f"    Fetching sprints from folder: {folder_name}")
                lists = get_lists_from_folder(api_token, folder_id)
                
                # Filter lists to only include those with a start_date
                lists_with_start_date = [lst for lst in lists if lst.get('start_date')]
                skipped_count = len(lists) - len(lists_with_start_date)
                
                print(f"      Found {len(lists)} lists (sprints), {len(lists_with_start_date)} with start dates")
                if skipped_count > 0:
                    print(f"      Skipped {skipped_count} lists without start dates")
                
                for clickup_list in lists_with_start_date:
                    list_id = clickup_list.get('id')
                    list_name = clickup_list.get('name')
                    
                    # Check if list should be included based on due date (sprint-level filtering)
                    should_include, use_task_filter = should_include_list(clickup_list, date_updated_gt)
                    
                    if not should_include:
                        print(f"      Skipping list '{list_name}' - due date before threshold")
                        continue
                    
                    # Map list to sprint - use database board_id
                    sprint_data = map_list_to_sprint(clickup_list, folder_id, board_id, now, org_id)
                    
                    # Insert sprint immediately and get its auto-generated id
                    print(f"      Inserting sprint: {list_name}")
                    sprint_id = insert_sprints_to_db(sprint_data, conn)
                    list_to_sprint_id[list_id] = sprint_id  # Store mapping
                    board_sprint_count += 1
                    print(f"      ✓ Sprint inserted with id: {sprint_id}")
                    
                    # Fetch and insert list-level custom fields
                    try:
                        list_custom_fields = get_custom_list_fields(api_token, list_id)
                        print(f"        Found {len(list_custom_fields)} list custom fields")
                        
                        for list_custom_field in list_custom_fields:
                            list_field_data = map_list_custom_field_to_custom_field(list_custom_field, org_id)
                            print(f"          Inserting list custom field: {list_field_data.get('name')}")
                            insert_list_custom_field_to_db(list_field_data, conn)
                            list_custom_fields_count += 1
                    except Exception as e:
                        print(f"        Warning: Failed to sync list custom fields for list '{list_name}': {e}")
                    
                    # Fetch tasks (issues) for this list
                    # Apply task-level date filter only if list has no due date
                    task_date_filter = date_updated_gt if use_task_filter else None
                    print(f"        Fetching issues from sprint: {list_name}")
                    tasks = get_tasks_from_list(api_token, list_id, task_date_filter)
                    
                    if use_task_filter:
                        print(f"          Found {len(tasks)} tasks (filtered by date - no list due date)")
                    else:
                        print(f"          Found {len(tasks)} tasks (all tasks - list due date in range)")
                    
                    for task in tasks:
                        # Map and insert task immediately so subtasks can find their parents
                        issue_data = map_task_to_issue(task, board_id, sprint_id, space_id, now, conn, org_id, api_token)
                        insert_issue_to_db(issue_data, conn)
                        issues_count += 1
                        board_issue_count += 1
                        
                        # Check if task has a PR link and create mapping
                        try:
                            pr_mapping = map_pr_id_to_issue_id(task, conn, org_id)
                            if pr_mapping:
                                insert_activity_issue_mapping(pr_mapping, conn)
                                pr_mappings_count += 1
                        except Exception as e:
                            print(f"          Warning: Failed to create PR mapping for task '{task.get('name')}': {e}")
                
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
            
            # Process folderless lists for this space
            print(f"\n  Fetching folderless lists from space: {space_name}")
            try:
                folderless_lists = get_folderlesslists(api_token, space_id)
                print(f"    Found {len(folderless_lists)} folderless lists")
                
                for fl_list in folderless_lists:
                    fl_list_id = fl_list.get('id')
                    fl_list_name = fl_list.get('name')
                    
                    # Check if folderless list should be included based on due date (sprint-level filtering)
                    should_include, use_task_filter = should_include_list(fl_list, date_updated_gt)
                    
                    if not should_include:
                        print(f"    Skipping folderless list '{fl_list_name}' - due date before threshold")
                        continue
                    
                    # Map folderless list to sprint
                    fl_sprint_data = map_folderless_list_to_sprint(fl_list, now, org_id)
                    
                    # Insert folderless list as sprint
                    print(f"    Inserting folderless sprint: {fl_list_name}")
                    try:
                        fl_sprint_id = insert_folderless_list_to_db(fl_sprint_data, conn)
                        list_to_sprint_id[fl_list_id] = fl_sprint_id
                        folderless_lists_count += 1
                        print(f"    ✓ Folderless sprint inserted with id: {fl_sprint_id}")
                        
                        # Fetch and insert tasks from this folderless list
                        # Apply task-level date filter only if list has no due date
                        task_date_filter = date_updated_gt if use_task_filter else None
                        print(f"      Fetching issues from folderless list: {fl_list_name}")
                        fl_tasks = get_tasks_from_list(api_token, fl_list_id, task_date_filter)
                        
                        if use_task_filter:
                            print(f"        Found {len(fl_tasks)} tasks (filtered by date - no list due date)")
                        else:
                            print(f"        Found {len(fl_tasks)} tasks (all tasks - list due date in range)")
                        
                        for fl_task in fl_tasks:
                            try:
                                fl_issue_data = map_task_to_issue(
                                    fl_task, 
                                    ORPHAN_BOARD_ID,  # Orphan board for folderless lists
                                    fl_sprint_id, 
                                    space_id, 
                                    now, 
                                    conn,
                                    org_id,
                                    api_token
                                )
                                insert_issue_to_db(fl_issue_data, conn)
                                folderless_issues_count += 1
                                
                                # Check if task has a PR link and create mapping
                                try:
                                    pr_mapping = map_pr_id_to_issue_id(fl_task, conn, org_id)
                                    if pr_mapping:
                                        insert_activity_issue_mapping(pr_mapping, conn)
                                        pr_mappings_count += 1
                                except Exception as e:
                                    print(f"          Warning: Failed to create PR mapping for task '{fl_task.get('name')}': {e}")
                            except Exception as e:
                                print(f"        Warning: Failed to insert task '{fl_task.get('name')}': {e}")
                                
                    except Exception as e:
                        print(f"    Warning: Failed to insert folderless sprint '{fl_list_name}': {e}")
                        
            except Exception as e:
                print(f"  Warning: Failed to fetch folderless lists for space '{space_name}': {e}")
        
        # Build summary
        summary = {
            'users': users_count,
            'custom_fields': custom_fields_count,
            'workspace_custom_fields': workspace_custom_fields_count,
            'space_custom_fields': space_custom_fields_count,
            'folder_custom_fields': folder_custom_fields_count,
            'list_custom_fields': list_custom_fields_count,
            'boards': len(folder_to_board_id),
            'sprints': len(list_to_sprint_id),
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
        print(f"Total Sprints (Lists): {len(list_to_sprint_id)}")
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
