import os
from dotenv import load_dotenv

from database import get_clickup_access_token

load_dotenv()

ORG_ID = 2133

# ClickUp Configuration
CLICKUP_API_TOKEN = get_clickup_access_token("CLICKUP", ORG_ID)
TEAM_ID = os.getenv('CLICKUP_TEAM_ID')
CLICKUP_API_BASE = 'https://api.clickup.com/api/v2'

# Database Configuration
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')