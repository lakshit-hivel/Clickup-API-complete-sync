# ClickUp Sync API 🔄

A robust API designed to synchronize data between ClickUp and a database, ensuring data consistency and enabling efficient reporting and analysis. This project automates the transfer of information, streamlining workflows and improving data accessibility.

## 🚀 Key Features

- **Data Synchronization**: Automatically syncs ClickUp data (tasks, lists, folders, spaces, etc.) with a database.
- **API Endpoints**: Provides API endpoints to trigger and monitor sync jobs.
- **Background Tasks**: Uses background tasks to execute sync operations asynchronously.
- **Error Handling**: Implements comprehensive error handling and logging.
- **Configuration**: Uses environment variables for easy configuration across different environments.
- **Database Integration**: Supports PostgreSQL database for storing synchronized data.
- **ClickUp API Integration**: Seamlessly interacts with the ClickUp API to fetch data.
- **Single Board Sync**: Allows syncing of individual boards.
- **Sync Status Tracking**: Tracks the status of sync jobs.

## 🛠️ Tech Stack

- **Backend**:
    - Python 3.x
    - FastAPI: Web framework for building APIs
    - Uvicorn: ASGI server to run the FastAPI application
- **Database**:
    - PostgreSQL: Relational database for storing synchronized data
    - psycopg2: PostgreSQL adapter for Python
- **API Integration**:
    - ClickUp API: For fetching data from ClickUp
    - requests: For making HTTP requests to the ClickUp API
- **Configuration**:
    - `os`: Access environment variables
    - `dotenv`: Load environment variables from a `.env` file
- **Other**:
    - `datetime`: For handling timestamps
    - `timedelta`: For handling dates and times
    - `src.core.logger`: For logging

## 📦 Getting Started

### Prerequisites

- Python 3.x
- PostgreSQL database
- ClickUp account with API token
- Docker (optional, for containerization)

### Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  Create a virtual environment (recommended):

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate  # On Windows
    ```

3.  Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

4.  Configure environment variables:

    - Create a `.env` file in the project root.
    - Add the following environment variables:

    ```
    CLICKUP_TEAM_ID=<your_clickup_team_id>
    DB_HOST=<your_db_host>
    DB_PORT=<your_db_port>
    DB_NAME=<your_db_name>
    DB_USER=<your_db_user>
    DB_PASSWORD=<your_db_password>
    ```

    - Replace the placeholders with your actual values.

### Running Locally

1.  Start the Uvicorn server:

    ```bash
    python app.py
    ```

2.  The API will be accessible at `http://0.0.0.0:8000`.

## 💻 Usage

### API Endpoints

-   **Health Check**: `GET /health_check` - Checks the health of the API.
-   **Trigger Sync**: `POST /trigger_sync` - Triggers a full ClickUp sync for a given organization.
    -   Request body: `{ "org_id": <organization_id>, "clickup_user_integration_id": <clickup_user_integration_id> }`
-   **Trigger Board Sync**: `POST /trigger_board_sync` - Triggers a sync for a specific board within an organization.
    -   Request body: `{ "org_id": <organization_id>, "board_id": <board_id>, "clickup_user_integration_id": <clickup_user_integration_id> }`
-   **Sync Status**: `GET /sync_status` - Gets the status of a sync job for a given organization.
    -   Query parameter: `org_id=<organization_id>`

### Example

To trigger a sync, send a POST request to `/trigger_sync` with the appropriate JSON payload:

```json
{
  "org_id": 123,
  "clickup_user_integration_id": 456
}
```

## 📂 Project Structure

```
.
├── app.py                      # Main application entry point
├── src
│   ├── api
│   │   ├── controllers
│   │   │   └── sync_controller.py  # Handles sync API logic
│   │   └── routes
│   │       └── sync_routes.py      # Defines API endpoints
│   ├── core
│   │   ├── config.py             # Configuration settings
│   │   └── logger.py             # Logging configuration
│   ├── db
│   │   └── database.py           # Database interaction functions
│   ├── integrations
│   │   └── clickup_api.py        # ClickUp API client
│   ├── mappers
│   │   └── mappers.py            # Data mapping functions
│   ├── models                    # Data models (if any)
│   │   └── models.py
│   ├── services
│   │   ├── boards
│   │   │   └── sync.py           # Sync single board
│   │   └── sync_orchestrator.py  # Orchestrates the sync process
│   └── utils                     # Utility functions (if any)
│       └── utils.py
├── .env                        # Environment variables
├── README.md                   # This file
└── requirements.txt            # Project dependencies
```

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Make your changes and commit them with descriptive messages.
4.  Submit a pull request.


## 📬 Contact

For questions or issues, please contact [Lakshit Agarwal](mailto:lakshit@hivel.ai).

## 💖 Thanks

Thank you for using the ClickUp Sync API! We appreciate your contributions and feedback.

This is written by [readme.ai](https://readme-generator-phi.vercel.app/).
