"""
ClickUp Sync API - Main Application Entry Point

Run with: uv run python app.py
"""
from fastapi import FastAPI
import uvicorn
from src.api.routes.sync_routes import router as sync_router

app = FastAPI(
    title="ClickUp Sync API",
    description="API for syncing ClickUp data to the database",
    version="1.0.0"
)

# Include routers
app.include_router(sync_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
