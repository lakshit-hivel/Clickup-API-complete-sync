import requests
from src.core.config import CLICKUP_API_BASE


def get_clickup_headers(api_token):
    """Return headers for ClickUp API requests"""
    return {
        'Authorization': api_token
    }


def get_authorized_teams(api_token):
    """Fetch authorized teams and return the first team_id"""
    url = f'{CLICKUP_API_BASE}/team'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    teams = data.get('teams', [])
    if teams:
        return teams[0].get('id')
    return None


def get_clickup_spaces(api_token, team_id):
    """Fetch all spaces from ClickUp team"""
    url = f'{CLICKUP_API_BASE}/team/{team_id}/space'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('spaces', [])


def get_folders(api_token, space_id):
    """Fetch all folders in a space"""
    url = f'{CLICKUP_API_BASE}/space/{space_id}/folder'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('folders', [])


def get_lists_from_folder(api_token, folder_id):
    """Fetch all lists from a folder"""
    url = f'{CLICKUP_API_BASE}/folder/{folder_id}/list'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('lists', [])


def get_tasks_from_list(api_token, list_id, date_updated_gt=None):
    """Fetch all tasks from a list with automatic pagination
    
    Args:
        api_token: ClickUp API token
        list_id: The list ID to fetch tasks from
        date_updated_gt: Optional timestamp (ms) to filter tasks updated after this date
    """
    all_tasks = []
    page_num = 0
    
    while True:
        url = f'{CLICKUP_API_BASE}/list/{list_id}/task?subtasks=true&order_by=updated&include_closed=true&page={page_num}'
        if date_updated_gt:
            url += f'&date_updated_gt={date_updated_gt}'
        response = requests.get(url, headers=get_clickup_headers(api_token))
        response.raise_for_status()
        data = response.json()
        tasks = data.get('tasks', [])
        
        all_tasks.extend(tasks)
        if len(tasks) < 100:
            break
            
        page_num += 1
    
    return all_tasks


def get_custom_task_types(api_token, team_id):
    """Fetch all custom task types"""
    url = f'{CLICKUP_API_BASE}/team/{team_id}/custom_item'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('custom_items', [])


def get_custom_list_fields(api_token, list_id):
    """Fetch all list custom fields from a list"""
    url = f'{CLICKUP_API_BASE}/list/{list_id}/field'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('fields', [])


def get_folder_custom_fields(api_token, folder_id):
    """Fetch all folder custom fields from a folder"""
    url = f'{CLICKUP_API_BASE}/folder/{folder_id}/field'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('fields', [])


def get_space_custom_fields(api_token, space_id):
    """Fetch all space custom fields from a space"""
    url = f'{CLICKUP_API_BASE}/space/{space_id}/field'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('fields', [])


def get_workspace_custom_fields(api_token, team_id):
    """Fetch all workspace custom fields from a workspace"""
    url = f'{CLICKUP_API_BASE}/team/{team_id}/field'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('fields', [])


def get_users(api_token):
    """Fetch all users from a workspace"""
    url = f'{CLICKUP_API_BASE}/team'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    teams = data.get('teams', [])
    for team in teams:
        return team.get('members', [])


def get_folderlesslists(api_token, space_id):
    """Fetch all folderless lists"""
    url = f'{CLICKUP_API_BASE}/space/{space_id}/list'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    data = response.json()
    return data.get('lists', [])


def get_task_by_id(api_token, task_id):
    """Fetch a single task by its ClickUp task ID
    
    Args:
        api_token: ClickUp API token
        task_id: The ClickUp task ID to fetch
        
    Returns:
        dict: The task data from ClickUp API
    """
    url = f'{CLICKUP_API_BASE}/task/{task_id}?include_subtasks=true'
    response = requests.get(url, headers=get_clickup_headers(api_token))
    response.raise_for_status()
    return response.json()
