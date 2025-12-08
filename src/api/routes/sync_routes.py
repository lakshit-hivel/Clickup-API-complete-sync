"""
Sync routes - API endpoints for sync operations
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException
from src.api.controllers.sync_controller import (
    check_sync_in_progress,
    get_sync_status,
    run_sync_task,
    run_board_sync_task
)

router = APIRouter()


@router.post("/sync")
async def trigger_sync(org_id: int, days: int = 30, background_tasks: BackgroundTasks = None):
    """
    Trigger a ClickUp sync for the specified organization.
    
    - org_id: The organization ID to sync data for
    - days: Number of days to look back for updated tasks (default: 30)
    """
    if check_sync_in_progress(org_id):
        raise HTTPException(status_code=409, detail=f"Sync already in progress for org_id={org_id}")
    
    background_tasks.add_task(run_sync_task, org_id, days)
    
    return {
        "status": "started",
        "message": f"Sync initiated for org_id={org_id}, syncing tasks from last {days} days",
        "org_id": org_id
    }


@router.post("/sync/board")
async def trigger_board_sync(board_id: int, org_id: int, days: int = 30, background_tasks: BackgroundTasks = None):
    """
    Trigger a ClickUp sync for a specific existing board.
    
    - board_id: The database ID of the board (id column from board table, NOT ClickUp folder_id)
    - org_id: The organization ID
    - days: Number of days to look back for updated tasks (default: 30)
    """
    board_key = f"board_{board_id}"
    if check_sync_in_progress(board_key):
        raise HTTPException(status_code=409, detail=f"Sync already in progress for board_id={board_id}")
    
    background_tasks.add_task(run_board_sync_task, board_id, org_id, days)
    
    return {
        "status": "started",
        "message": f"Board sync initiated for board_id={board_id}, org_id={org_id}, syncing tasks from last {days} days",
        "board_id": board_id,
        "org_id": org_id
    }


@router.get("/sync/status/{org_id}")
async def sync_status(org_id: int):
    """Get the status of the most recent sync job for an organization"""
    return get_sync_status(org_id)


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}
