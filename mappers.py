from datetime import datetime
from config import ORG_ID
from database import find_user_by_email, get_parent_id_from_clickup_id, get_id_from_clickup_top_level_parent_id, get_custom_field_name_from_id


def map_folder_to_board(folder, space_id, now):
    """Map ClickUp Folder to Board table schema"""
    folder_id = folder.get('id')
    folder_name = folder.get('name')
    
    return {
        'entity_id': None,
        'name': folder_name,
        'display_name': None,
        'board_key': str(folder_id),
        'created_at': now,
        'modifieddate': now,
        'org_id': ORG_ID,
        'account_id': str(space_id),
        'active': not folder.get('archived', False),
        'is_deleted': folder.get('archived', False),
        'is_private': folder.get('hidden', False),
        'uuid': None,
        'avatar_uri': None,
        'self': None,
        'jira_board_id': str(folder_id),
        'auto_generated_sprint': False,
        'azure_project_id': None,
        'azure_project_name': None,
        'azure_org_name': None,
    }


def map_list_to_sprint(clickup_list, folder_id, board_id, now):
    """Map ClickUp List to Sprint table schema"""
    list_id = clickup_list.get('id')
    list_name = clickup_list.get('name')
    
    # Convert timestamps
    start_date = clickup_list.get('start_date')
    if start_date:
        start_date = datetime.fromtimestamp(int(start_date) / 1000)
    
    end_date = clickup_list.get('due_date')
    if end_date:
        end_date = datetime.fromtimestamp(int(end_date) / 1000)

    # Determine state based on dates (with None checks)
    today = datetime.now()
    if end_date and end_date < today:
        state = 'closed'
    elif start_date and start_date > today:
        state = 'future'
    elif start_date and end_date and start_date <= today <= end_date:
        state = 'active'
    else:
        state = None
    
    return {
        'created_at': now,
        'is_deleted': clickup_list.get('archived', False),
        'modifieddate': now,
        'board_id': board_id,
        'end_date': end_date,
        'complete_date': end_date if state == 'closed' else None,
        'goal': clickup_list.get('content'),
        'name': list_name,
        'sprint_jira_id': str(list_id),
        'start_date': start_date,
        'state': state,
        'org_id': ORG_ID,
        'jira_board_id': str(folder_id),
    }


def map_task_to_issue(task, board_id, sprint_id, space_id, now, conn):
    """Map ClickUp Task to Issue table schema
    
    Args:
        task: ClickUp task data
        board_id: The auto-generated board id from the database (NOT ClickUp folder_id)
        sprint_id: The auto-generated sprint id from the database (NOT ClickUp list_id)
        space_id: ClickUp space id
        now: Current timestamp
        conn: Database connection for parent lookups
    """
    task_id = task.get('id')
    
    # Convert timestamps
    created_at = task.get('date_created')
    if created_at:
        created_at = datetime.fromtimestamp(int(created_at) / 1000)
    else:
        created_at = now
    
    updated_at = task.get('date_updated')
    if updated_at:
        updated_at = datetime.fromtimestamp(int(updated_at) / 1000)
    else:
        updated_at = now
    
    due_date = task.get('due_date')
    if due_date:
        due_date = datetime.fromtimestamp(int(due_date) / 1000)
    
    resolution_date = task.get('date_closed')
    if resolution_date:
        resolution_date = datetime.fromtimestamp(int(resolution_date) / 1000)
    
    # Get priority
    priority_obj = task.get('priority')
    priority = priority_obj.get('priority') if priority_obj else None

    progress_obj = task.get('status', {})
    progress = progress_obj.get('orderindex') if progress_obj else None

    # Resolve parent_id: If task has a ClickUp parent, look up its database ID
    clickup_parent_id = task.get('parent')
    parent_id = None
    if clickup_parent_id:
        parent_id = get_parent_id_from_clickup_id(clickup_parent_id, ORG_ID, conn)
        if not parent_id:
            print(f"  Warning: Parent not found for task '{task.get('name')}' (ClickUp parent: {clickup_parent_id})")
    
    clickup_top_level_parent_id = task.get('top_level_parent')
    top_level_parent = None
    if clickup_top_level_parent_id:
        top_level_parent = get_id_from_clickup_top_level_parent_id(clickup_top_level_parent_id, ORG_ID, conn)
        if not top_level_parent:
            print(f"  Warning: Top-level parent not found for task '{task.get('name')}' (ClickUp top_level_parent: {clickup_top_level_parent_id})")
    
    # Get issue type from custom_item_id
    custom_item_id = task.get('custom_item_id')
    issue_type = None
    if custom_item_id:
        issue_type = get_custom_field_name_from_id(custom_item_id, ORG_ID, conn)
        if not issue_type:
            print(f"  Warning: Custom field not found for task '{task.get('name')}' (custom_item_id: {custom_item_id})")
    
    # Get assignee ID (if assignees exist)
    assigneeId = None
    assignees = task.get('assignees', [])
    if assignees and len(assignees) > 0:
        assigneeEmail = assignees[0].get('email')
        if assigneeEmail:
            assigneeId = find_user_by_email(assigneeEmail, ORG_ID, conn)
            if not assigneeId:
                print(f"  Warning: Assignee not found for task '{task.get('name')}' (assigneeEmail: {assigneeEmail})")
    
    # Get creator ID (if creator exists)
    creatorId = None
    creator = task.get('creator')
    if creator:
        creatorEmail = creator.get('email')
        if creatorEmail:
            creatorId = find_user_by_email(creatorEmail, ORG_ID, conn)
            if not creatorId:
                print(f"  Warning: Creator not found for task '{task.get('name')}' (creatorEmail: {creatorEmail})")

    return {
        'created_at': created_at,
        'modifieddate': updated_at,
        'board_id': board_id,
        'priority': priority,
        'resolution_date': resolution_date,
        'time_spent': task.get('time_estimate'),
        'parent_id': top_level_parent, #to be mapped with top_level_parent
        'is_deleted': task.get('archived', False),
        'assignee_id': assigneeId,
        'creator_id': creatorId,
        'due_date': due_date,
        'issue_id': str(task_id),
        'key': task.get('custom_id'),
        'parent_issue_id': parent_id, #parent
        'project_id': str(space_id),
        'issue_url': task.get('url'),
        'reporter_id': None,
        'status': task.get('status', {}).get('status') if task.get('status') else None,
        'summary': task.get('name'),
        'description': task.get('description'),
        'sprint_id': sprint_id,  # Now using the actual database sprint id (foreign key)
        'org_id': ORG_ID,
        'current_progress' : progress,
        'status_change_date' : updated_at,
        'issue_type' : issue_type,
        'parent_task_id' : parent_id #parent
        
    }


def map_custom_task_type_to_custom_field(custom_task_type):
    """Map ClickUp Custom Task Type to Custom Field table schema"""
    return {
        'jira_id': str(custom_task_type.get('id')),
        'name': custom_task_type.get('name'),
        'description': custom_task_type.get('description'),
        'org_id': ORG_ID,
    }

def map_list_custom_field_to_custom_field(list_custom_field):
    """Map ClickUp List Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(list_custom_field.get('id')),
        'name': list_custom_field.get('name'),
        'data_type': list_custom_field.get('type'),
        'org_id': ORG_ID,
    }

def map_folder_custom_field_to_custom_field(folder_custom_field):
    """Map ClickUp Folder Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(folder_custom_field.get('id')),
        'name': folder_custom_field.get('name'),
        'data_type': folder_custom_field.get('type'),
        'org_id': ORG_ID,
    }

def map_space_custom_field_to_custom_field(space_custom_field):
    """Map ClickUp Space Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(space_custom_field.get('id')),
        'name': space_custom_field.get('name'),
        'data_type': space_custom_field.get('type'),
        'org_id': ORG_ID,
    }

def map_workspace_custom_field_to_custom_field(workspace_custom_field):
    """Map ClickUp Workspace Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(workspace_custom_field.get('id')),
        'name': workspace_custom_field.get('name'),
        'data_type': workspace_custom_field.get('type'),
        'org_id': ORG_ID,
    }


def map_users_to_usertable(user):
    """Map ClickUp User to User table schema"""
    # Extract nested user object if it exists
    user_data = user.get('user', user)
    
    # Use email as fallback if username is None
    username = user_data.get('username') or user_data.get('email', 'Unknown')
    
    return {
        'type': "USER",
        'name': username,
        'email': user_data.get('email'),
        'organizationid': ORG_ID,
        'scmprovider': "CLICKUP",
        'active': True
    }