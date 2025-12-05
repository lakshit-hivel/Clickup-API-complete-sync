from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks, HTTPException
import uvicorn
from database import get_db_connection, get_clickup_access_token
from clickup_api import get_authorized_teams
from main import sync_clickup_data

app = FastAPI(title="ClickUp Sync API")

# Track sync jobs in memory
sync_jobs = {}


@app.post("/sync")
async def trigger_sync(org_id: int, days: int = 30, background_tasks: BackgroundTasks = None):
    """
    Trigger a ClickUp sync for the specified organization.
    
    - org_id: The organization ID to sync data for
    - days: Number of days to look back for updated tasks (default: 30)
    """
    
    # Check if sync is already running for this org
    if org_id in sync_jobs and sync_jobs[org_id].get("status") == "running":
        raise HTTPException(status_code=409, detail=f"Sync already in progress for org_id={org_id}")
    
    # Add sync task to background
    background_tasks.add_task(run_sync_task, org_id, days)
    
    return {
        "status": "started",
        "message": f"Sync initiated for org_id={org_id}, syncing tasks from last {days} days",
        "org_id": org_id
    }


@app.get("/sync/status/{org_id}")
async def get_sync_status(org_id: int):
    """Get the status of the most recent sync job for an organization"""
    if org_id not in sync_jobs:
        return {"status": "no_sync_found", "org_id": org_id}
    
    return {"org_id": org_id, **sync_jobs[org_id]}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
