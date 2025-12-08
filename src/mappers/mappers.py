from datetime import datetime
from src.db.database import find_user_by_email, get_parent_id_from_clickup_id, get_id_from_clickup_top_level_parent_id, get_custom_field_name_from_id, get_pr_id, get_issue_id, insert_issue_to_db
from src.integrations.clickup_api import get_task_by_id
from src.core.logger import logger


def map_folder_to_board(folder, space_id, now, org_id):
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
        'org_id': org_id,
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


def map_list_to_sprint(clickup_list, folder_id, board_id, now, org_id):
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
        'org_id': org_id,
        'jira_board_id': str(folder_id),
    }


def map_folderless_list_to_sprint(folderless_list, now, org_id):
    """Map ClickUp Folderless List to Sprint table schema"""
    end_date = folderless_list.get('due_date')
    if end_date:
        end_date = datetime.fromtimestamp(int(end_date) / 1000)
    else:
        end_date = None
    
    start_date = folderless_list.get('start_date')
    if start_date:
        start_date = datetime.fromtimestamp(int(start_date) / 1000)
    else:
        start_date = None

    return {
        'created_at': now,
        'is_deleted': folderless_list.get('archived', False),
        'modifieddate': now,
        'board_id': 10011,
        'end_date': end_date,
        'complete_date': end_date,
        'goal': folderless_list.get('content'),
        'name': folderless_list.get('name'),
        'sprint_jira_id': str(folderless_list.get('id')),
        'start_date': start_date,
        'org_id': org_id,
        'jira_board_id': 'FOLDERLESS_ORPHAN',
    }


def ensure_parent_exists(clickup_parent_id, board_id, sprint_id, space_id, now, conn, org_id, api_token):
    """Ensure a parent task exists in the database, fetching and inserting it if necessary.
    
    This function recursively handles nested parents (parent of parent of parent...).
    
    Args:
        clickup_parent_id: The ClickUp ID of the parent task
        board_id: The auto-generated board id from the database
        sprint_id: The auto-generated sprint id from the database
        space_id: ClickUp space id
        now: Current timestamp
        conn: Database connection
        org_id: Organization ID
        api_token: ClickUp API token
        
    Returns:
        int: The database ID of the parent task, or None if fetch/insert fails
    """
    if not clickup_parent_id:
        return None
    
    # First, check if parent already exists in database
    parent_db_id = get_parent_id_from_clickup_id(clickup_parent_id, org_id, conn)
    if parent_db_id:
        return parent_db_id
    
    # Parent not in database - fetch it from ClickUp API
    logger.debug(f"Fetching missing parent task: {clickup_parent_id}")
    try:
        parent_task = get_task_by_id(api_token, clickup_parent_id)
    except Exception as e:
        logger.warning(f"Error fetching parent task {clickup_parent_id}: {e}")
        return None
    
    # Recursively ensure this parent's own parent exists (for deep nesting)
    parent_of_parent_clickup_id = parent_task.get('parent')
    if parent_of_parent_clickup_id:
        ensure_parent_exists(parent_of_parent_clickup_id, board_id, sprint_id, space_id, now, conn, org_id, api_token)
    
    # Also ensure top-level parent exists
    top_level_parent_clickup_id = parent_task.get('top_level_parent')
    if top_level_parent_clickup_id and top_level_parent_clickup_id != clickup_parent_id:
        ensure_parent_exists(top_level_parent_clickup_id, board_id, sprint_id, space_id, now, conn, org_id, api_token)
    
    # Now map and insert the parent task
    logger.debug(f"Inserting missing parent task: {parent_task.get('name')}")
    parent_issue_data = map_task_to_issue(parent_task, board_id, sprint_id, space_id, now, conn, org_id, api_token)
    insert_issue_to_db(parent_issue_data, conn)
    
    # Return the newly inserted parent's database ID
    return get_parent_id_from_clickup_id(clickup_parent_id, org_id, conn)


def map_task_to_issue(task, board_id, sprint_id, space_id, now, conn, org_id, api_token):
    """Map ClickUp Task to Issue table schema
    
    Args:
        task: ClickUp task data
        board_id: The auto-generated board id from the database (NOT ClickUp folder_id)
        sprint_id: The auto-generated sprint id from the database (NOT ClickUp list_id)
        space_id: ClickUp space id
        now: Current timestamp
        conn: Database connection for parent lookups
        org_id: Organization ID
        api_token: ClickUp API token
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
    # If parent doesn't exist yet, fetch and insert it first (handles deep nesting)
    clickup_parent_id = task.get('parent')
    parent_id = None
    if clickup_parent_id:
        parent_id = get_parent_id_from_clickup_id(clickup_parent_id, org_id, conn)
        if not parent_id:
            # Parent not in DB yet - fetch and insert it first
            parent_id = ensure_parent_exists(clickup_parent_id, board_id, sprint_id, space_id, now, conn, org_id, api_token)
            if not parent_id:
                logger.warning(f"Could not resolve parent for task '{task.get('name')}' (ClickUp parent: {clickup_parent_id})")
    
    # Resolve top_level_parent: ensure it exists in database
    clickup_top_level_parent_id = task.get('top_level_parent')
    top_level_parent = None
    if clickup_top_level_parent_id:
        top_level_parent = get_id_from_clickup_top_level_parent_id(clickup_top_level_parent_id, org_id, conn)
        if not top_level_parent:
            # Top-level parent not in DB yet - fetch and insert it first
            top_level_parent = ensure_parent_exists(clickup_top_level_parent_id, board_id, sprint_id, space_id, now, conn, org_id, api_token)
            if not top_level_parent:
                logger.warning(f"Could not resolve top-level parent for task '{task.get('name')}' (ClickUp top_level_parent: {clickup_top_level_parent_id})")
    
    # Get issue type from custom_item_id
    custom_item_id = task.get('custom_item_id')
    issue_type = None
    if custom_item_id and custom_item_id != 0:
        issue_type = get_custom_field_name_from_id(custom_item_id, org_id, conn)
        if not issue_type:
            logger.warning(f"Custom field not found for task '{task.get('name')}' (custom_item_id: {custom_item_id})")
    else:
        # If custom_item_id is 0 or None, default to "task"
        issue_type = "task"
    
    # Truncate task name (summary) to fit database varchar(255) limit
    summary = task.get('name', '')
    if summary and len(summary) > 255:
        summary = summary[:252] + '...'  # Truncate to 252 chars + '...' = 255

    # Get assignee ID (if assignees exist)
    assigneeId = None
    assignees = task.get('assignees', [])
    if assignees and len(assignees) > 0:
        assigneeEmail = assignees[0].get('email')
        if assigneeEmail:
            assigneeId = find_user_by_email(assigneeEmail, org_id, conn)
            if not assigneeId:
                logger.warning(f"Assignee not found for task '{task.get('name')}' (assigneeEmail: {assigneeEmail})")
    
    # Get creator ID (if creator exists)
    creatorId = None
    creator = task.get('creator')
    if creator:
        creatorEmail = creator.get('email')
        if creatorEmail:
            creatorId = find_user_by_email(creatorEmail, org_id, conn)
            if not creatorId:
                logger.warning(f"Creator not found for task '{task.get('name')}' (creatorEmail: {creatorEmail})")

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
        'summary': summary,
        'description': task.get('description'),
        'sprint_id': sprint_id,  # Now using the actual database sprint id (foreign key)
        'org_id': org_id,
        'current_progress' : progress,
        'status_change_date' : updated_at,
        'issue_type' : issue_type,
        'story_point' : task.get('points'),
        'parent_task_id' : parent_id #parent
        
    }


def map_pr_id_to_issue_id(task, conn, org_id):
    """Map PR ID to Issue ID by extracting PR link from task custom fields
    
    Args:
        task: ClickUp task data containing custom fields
        conn: Database connection
        org_id: Organization ID
        
    Returns:
        dict: Mapping of PR ID to Issue ID, or None if PR link not found or invalid
    """
    # Get the task ID (ClickUp task ID)
    task_id = task.get('id')
    if not task_id:
        logger.warning("Task has no ID, cannot create PR mapping")
        return None
    
    # Find the issue's primary key in the database
    issue_db_id = get_issue_id(str(task_id), conn)
    if not issue_db_id:
        logger.warning(f"Issue not found in database for task ID {task_id}")
        return None
    
    # Look for the "PR LINK" custom field in the task
    custom_fields = task.get('custom_fields', [])
    pr_link = None
    
    for field in custom_fields:
        if field.get('name') == 'PR LINK':
            pr_link = field.get('value')
            break
    
    if not pr_link:
        # No PR link found - this is okay, not all tasks have PRs
        return None
    
    # Get the PR's primary key from the database using the GitHub URL
    pr_db_id = get_pr_id(pr_link, conn)
    if not pr_db_id:
        logger.warning(f"PR not found in database for link {pr_link}")
        return None
    
    return {
        'activity_id': pr_db_id,
        'issue_id': issue_db_id,
        'activity_type': 'PULL REQUEST',
        'org_id': org_id,
    }


def map_custom_task_type_to_custom_field(custom_task_type, org_id):
    """Map ClickUp Custom Task Type to Custom Field table schema"""
    return {
        'jira_id': str(custom_task_type.get('id')),
        'name': custom_task_type.get('name'),
        'description': custom_task_type.get('description'),
        'org_id': org_id,
    }


def map_list_custom_field_to_custom_field(list_custom_field, org_id):
    """Map ClickUp List Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(list_custom_field.get('id')),
        'name': list_custom_field.get('name'),
        'data_type': list_custom_field.get('type'),
        'org_id': org_id,
    }


def map_folder_custom_field_to_custom_field(folder_custom_field, org_id):
    """Map ClickUp Folder Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(folder_custom_field.get('id')),
        'name': folder_custom_field.get('name'),
        'data_type': folder_custom_field.get('type'),
        'org_id': org_id,
    }


def map_space_custom_field_to_custom_field(space_custom_field, org_id):
    """Map ClickUp Space Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(space_custom_field.get('id')),
        'name': space_custom_field.get('name'),
        'data_type': space_custom_field.get('type'),
        'org_id': org_id,
    }


def map_workspace_custom_field_to_custom_field(workspace_custom_field, org_id):
    """Map ClickUp Workspace Custom Field to Custom Field table schema"""
    return {
        'jira_id': str(workspace_custom_field.get('id')),
        'name': workspace_custom_field.get('name'),
        'data_type': workspace_custom_field.get('type'),
        'org_id': org_id,
    }


def map_users_to_usertable(user, org_id):
    """Map ClickUp User to User table schema"""
    # Extract nested user object if it exists
    user_data = user.get('user', user)
    
    # Use email as fallback if username is None
    username = user_data.get('username') or user_data.get('email', 'Unknown')
    
    return {
        'type': "USER",
        'name': username,
        'email': user_data.get('email'),
        'organizationid': org_id,
        'scmprovider': "CLICKUP",
        'active': True
    }


def map_board_status(board, board_id, user_integration_id, now, org_id, sync_status, issue_count=0, sprint_count=0):
    """Map per-board sync status to data_sync_process table schema"""
    return {
        # user_integration_id should come from user_integration_details table (FK), not ClickUp id
        'user_integration_id': user_integration_id,
        'organization_id': org_id,
        'board_id': board_id,
        'sync_status': sync_status,
        'created_at': now,
        'modifieddate': now,
        'is_deleted': False,
        'issue_count': issue_count,
        'sprint_count': sprint_count,
        'sync_type': 'INITIAL',
    }