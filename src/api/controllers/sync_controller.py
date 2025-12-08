"""
Sync controller - handles business logic for sync API endpoints
"""
from datetime import datetime, timedelta
from src.db.database import get_db_connection, get_clickup_access_token
from src.integrations.clickup_api import get_authorized_teams
from src.services.sync_orchestrator import sync_clickup_data
from src.services.boards.sync import sync_single_board

# Track sync jobs in memory
sync_jobs = {}


def check_sync_in_progress(key: str) -> bool:
    """Check if a sync is already in progress for the given key"""
    return key in sync_jobs and sync_jobs[key].get("status") == "running"


def get_sync_status(org_id: int) -> dict:
    """Get the status of the most recent sync job for an organization"""
    if org_id not in sync_jobs:
        return {"status": "no_sync_found", "org_id": org_id}
    
    return {"org_id": org_id, **sync_jobs[org_id]}


def run_sync_task(org_id: int, days: int):
    """Background task to run the ClickUp sync"""
    sync_jobs[org_id] = {"status": "running", "started_at": datetime.now().isoformat()}
    
    conn = None
    try:
        # 1. Get database connection
        conn = get_db_connection()
        
        # 2. Fetch api_token from DB using org_id
        api_token = get_clickup_access_token("CLICKUP", org_id, conn)
        if not api_token:
            raise Exception(f"ClickUp access token not found for org_id={org_id}")
        
        # 3. Fetch team_id from ClickUp API using api_token
        team_id = get_authorized_teams(api_token)
        if not team_id:
            raise Exception(f"No ClickUp team found for org_id={org_id}")
        
        # 4. Calculate date threshold for filtering tasks
        date_threshold = datetime.now() - timedelta(days=days)
        date_threshold_ms = int(date_threshold.timestamp() * 1000)
        
        # 5. Run the sync
        result = sync_clickup_data(
            org_id=org_id,
            api_token=api_token,
            team_id=team_id,
            date_updated_gt=date_threshold_ms
        )
        
        sync_jobs[org_id] = {
            "status": "completed",
            "started_at": sync_jobs[org_id]["started_at"],
            "completed_at": datetime.now().isoformat(),
            "result": result
        }
        
    except Exception as e:
        sync_jobs[org_id] = {
            "status": "failed",
            "started_at": sync_jobs[org_id]["started_at"],
            "failed_at": datetime.now().isoformat(),
            "error": str(e)
        }
    finally:
        if conn:
            conn.close()


def run_board_sync_task(board_id: int, org_id: int, days: int):
    """Background task to run the ClickUp sync for a single board"""
    board_key = f"board_{board_id}"
    sync_jobs[board_key] = {"status": "running", "started_at": datetime.now().isoformat()}
    
    conn = None
    try:
        # 1. Get database connection
        conn = get_db_connection()
        
        # 2. Fetch api_token from DB using org_id
        api_token = get_clickup_access_token("CLICKUP", org_id, conn)
        if not api_token:
            raise Exception(f"ClickUp access token not found for org_id={org_id}")
        
        # 3. Calculate date threshold for filtering tasks
        date_threshold = datetime.now() - timedelta(days=days)
        date_threshold_ms = int(date_threshold.timestamp() * 1000)
        
        # 4. Run the single board sync
        result = sync_single_board(
            board_id=board_id,
            org_id=org_id,
            api_token=api_token,
            date_updated_gt=date_threshold_ms
        )
        
        sync_jobs[board_key] = {
            "status": "completed",
            "started_at": sync_jobs[board_key]["started_at"],
            "completed_at": datetime.now().isoformat(),
            "result": result
        }
        
    except Exception as e:
        sync_jobs[board_key] = {
            "status": "failed",
            "started_at": sync_jobs[board_key]["started_at"],
            "failed_at": datetime.now().isoformat(),
            "error": str(e)
        }
    finally:
        if conn:
            conn.close()
