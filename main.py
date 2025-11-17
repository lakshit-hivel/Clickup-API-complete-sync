import sys
import traceback
from datetime import datetime

from clickup_api import get_clickup_spaces, get_folders, get_lists_from_folder, get_tasks_from_list, get_custom_task_types
from database import insert_boards_to_db, insert_sprints_to_db, insert_issue_to_db, get_db_connection, insert_custom_field_to_db
from mappers import map_folder_to_board, map_list_to_sprint, map_task_to_issue, map_custom_task_type_to_custom_field


def sync_clickup_data():
    """Main sync function - fetches ClickUp data and saves to database"""
    print("Starting ClickUp Full Sync\n")
    
    # Create single database connection for entire sync
    conn = get_db_connection()
    
    try:
        now = datetime.now()
        
        # Initialize data collectors
        folder_to_board_id = {}  # Map ClickUp folder_id to database board_id
        list_to_sprint_id = {}   # Map ClickUp list_id to database sprint_id
        issues_count = 0  # Count of processed issues
        custom_fields_count = 0  # Count of processed custom fields
        
        # Fetch and insert custom task types (custom fields) - independent operation
        print("\nFetching Custom Task Types (Custom Fields)...")
        try:
            custom_task_types = get_custom_task_types()
            print(f"Found {len(custom_task_types)} custom task types")
            
            for custom_task_type in custom_task_types:
                custom_field_data = map_custom_task_type_to_custom_field(custom_task_type)
                print(f"  Inserting custom field: {custom_field_data.get('name')}")
                insert_custom_field_to_db(custom_field_data, conn)
                custom_fields_count += 1
            
            print(f"✓ Successfully processed {custom_fields_count} custom fields\n")
        except Exception as e:
            print(f"Warning: Failed to sync custom fields: {e}")
            print("Continuing with main sync...\n")
        
        # Fetch spaces
        print("\nFetching and Inserting Data...")
        spaces = get_clickup_spaces()
        print(f"Found {len(spaces)} spaces")
        
        # Process each space
        for space in spaces:
            space_id = space.get('id')
            space_name = space.get('name')
            print(f"\nProcessing space: {space_name}")
            
            # Fetch folders (boards)
            folders = get_folders(space_id)
            print(f"  Found {len(folders)} folders (boards)")
            
            for folder in folders:
                folder_id = folder.get('id')
                folder_name = folder.get('name')
                
                # Map folder to board and insert immediately
                board_data = map_folder_to_board(folder, space_id, now)
                print(f"  Inserting board: {folder_name}")
                board_id = insert_boards_to_db(board_data, conn)  # Pass connection
                folder_to_board_id[folder_id] = board_id  # Store mapping
                
                # Fetch lists (sprints) for this folder
                print(f"    Fetching sprints from folder: {folder_name}")
                lists = get_lists_from_folder(folder_id)
                
                # Filter lists to only include those with a start_date
                lists_with_start_date = [lst for lst in lists if lst.get('start_date')]
                skipped_count = len(lists) - len(lists_with_start_date)
                
                print(f"      Found {len(lists)} lists (sprints), {len(lists_with_start_date)} with start dates")
                if skipped_count > 0:
                    print(f"      Skipped {skipped_count} lists without start dates")
                
                for clickup_list in lists_with_start_date:
                    list_id = clickup_list.get('id')
                    list_name = clickup_list.get('name')
                    
                    # Map list to sprint - use database board_id
                    sprint_data = map_list_to_sprint(clickup_list, folder_id, board_id, now)
                    
                    # Insert sprint immediately and get its auto-generated id
                    print(f"      Inserting sprint: {list_name}")
                    sprint_id = insert_sprints_to_db(sprint_data, conn)
                    list_to_sprint_id[list_id] = sprint_id  # Store mapping
                    print(f"      ✓ Sprint inserted with id: {sprint_id}")
                    
                    # Fetch tasks (issues) for this list
                    print(f"        Fetching issues from sprint: {list_name}")
                    tasks = get_tasks_from_list(list_id)
                    print(f"          Found {len(tasks)} tasks (issues)")
                    
                    for task in tasks:
                        # Map and insert task immediately so subtasks can find their parents
                        issue_data = map_task_to_issue(task, board_id, sprint_id, space_id, now, conn)
                        insert_issue_to_db(issue_data, conn)
                        issues_count += 1
        
        # Summary
        print("\n" + "="*60)
        print("SYNC SUMMARY")
        print("="*60)
        print(f"Total Custom Fields: {custom_fields_count}")
        print(f"Total Boards (Folders): {len(folder_to_board_id)}")
        print(f"Total Sprints (Lists): {len(list_to_sprint_id)}")
        print(f"Total Issues (Tasks): {issues_count}")
        
        print("\n" + "="*60)
        print("✓ SYNC COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Always close the connection when done
        if conn:
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    sync_clickup_data()

