import psycopg2
import traceback
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require',
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            connect_timeout=30  # Increased timeout
        )
        print("✓ Database connection established")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


def get_parent_id_from_clickup_id(clickup_parent_id, org_id, conn):
    """
    Get database parent_id by looking up ClickUp parent ID
    Returns the auto-generated id if found, None otherwise
    """
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            SELECT id FROM insightly_jira.issue 
            WHERE issue_id = %s AND org_id = %s
            LIMIT 1
        """
        cursor.execute(query, (str(clickup_parent_id), str(org_id)))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching parent_id for ClickUp ID {clickup_parent_id}: {e}")
        conn.rollback()  # Rollback to clear failed transaction state
        return None
    finally:
        if cursor:
            cursor.close()

def get_id_from_clickup_top_level_parent_id(clickup_top_level_parent_id, org_id, conn):
    """
    Get database id by looking up ClickUp top_level_parent ID
    Returns the auto-generated id if found, None otherwise
    """
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            SELECT id FROM insightly_jira.issue 
            WHERE issue_id = %s AND org_id = %s
            LIMIT 1
        """
        cursor.execute(query, (str(clickup_top_level_parent_id), str(org_id)))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching id for ClickUp ID {clickup_top_level_parent_id}: {e}")
        conn.rollback()  # Rollback to clear failed transaction state
        return None
    finally:
        if cursor:
            cursor.close()

def insert_boards_to_db(board_data, conn):
    """Insert or update a single board and return its id"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        upsert_query = """
            INSERT INTO insightly_jira.board (
                entity_id, name, display_name, board_key, created_at, modifieddate,
                org_id, account_id, active, is_deleted, is_private, uuid,
                avatar_uri, self, jira_board_id, auto_generated_sprint,
                azure_project_id, azure_project_name, azure_org_name
            ) VALUES (
                %(entity_id)s, %(name)s, %(display_name)s, %(board_key)s, 
                %(created_at)s, %(modifieddate)s, %(org_id)s, %(account_id)s,
                %(active)s, %(is_deleted)s, %(is_private)s, %(uuid)s,
                %(avatar_uri)s, %(self)s, %(jira_board_id)s, %(auto_generated_sprint)s,
                %(azure_project_id)s, %(azure_project_name)s, %(azure_org_name)s
            )
            ON CONFLICT (jira_board_id, board_key, org_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                display_name = EXCLUDED.display_name,
                board_key = EXCLUDED.board_key,
                modifieddate = EXCLUDED.modifieddate,
                org_id = EXCLUDED.org_id,
                account_id = EXCLUDED.account_id,
                active = EXCLUDED.active,
                is_deleted = EXCLUDED.is_deleted,
                is_private = EXCLUDED.is_private,
                uuid = EXCLUDED.uuid,
                avatar_uri = EXCLUDED.avatar_uri,
                self = EXCLUDED.self,
                jira_board_id = EXCLUDED.jira_board_id,
                auto_generated_sprint = EXCLUDED.auto_generated_sprint,
                azure_project_id = EXCLUDED.azure_project_id,
                azure_project_name = EXCLUDED.azure_project_name,
                azure_org_name = EXCLUDED.azure_org_name
            RETURNING id
        """
        
        cursor.execute(upsert_query, board_data)
        board_id = cursor.fetchone()[0]
        conn.commit()
        
        return board_id
        
    except Exception as e:
        print(f"Error upserting board {board_data.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_sprints_to_db(sprint_data, conn):
    """Insert or update a single sprint and return its id"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if sprint exists
        check_query = """
            SELECT id FROM insightly_jira.sprint 
            WHERE sprint_jira_id = %(sprint_jira_id)s AND org_id = %(org_id)s AND board_id = %(board_id)s
            LIMIT 1
        """
        
        # Insert query with RETURNING id
        insert_query = """
            INSERT INTO insightly_jira.sprint (
                created_at, is_deleted, modifieddate, board_id,
                end_date, goal, name, sprint_jira_id, start_date,
                state, org_id, jira_board_id, complete_date
            ) VALUES (
                %(created_at)s, %(is_deleted)s, %(modifieddate)s, %(board_id)s,
                %(end_date)s, %(goal)s, %(name)s, %(sprint_jira_id)s, %(start_date)s,
                %(state)s, %(org_id)s, %(jira_board_id)s, %(complete_date)s
            )
            RETURNING id
        """
        
        # Update query with RETURNING id
        update_query = """
            UPDATE insightly_jira.sprint SET
                is_deleted = %(is_deleted)s,
                modifieddate = %(modifieddate)s,
                board_id = %(board_id)s,
                end_date = %(end_date)s,
                goal = %(goal)s,
                name = %(name)s,
                start_date = %(start_date)s,
                state = %(state)s,
                jira_board_id = %(jira_board_id)s,
                complete_date = %(complete_date)s
            WHERE sprint_jira_id = %(sprint_jira_id)s AND org_id = %(org_id)s AND board_id = %(board_id)s
            RETURNING id
        """
        
        # Check if sprint exists
        cursor.execute(check_query, sprint_data)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing sprint and get its id
            cursor.execute(update_query, sprint_data)
            sprint_id = cursor.fetchone()[0]
        else:
            # Insert new sprint and get its id
            cursor.execute(insert_query, sprint_data)
            sprint_id = cursor.fetchone()[0]
        
        conn.commit()
        return sprint_id
        
    except Exception as e:
        print(f"Error upserting sprint {sprint_data.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_issue_to_db(issue, conn):
    """Insert or update a single issue"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if issue exists
        check_query = """
            SELECT id FROM insightly_jira.issue 
            WHERE issue_id = %(issue_id)s AND org_id = %(org_id)s
            LIMIT 1
        """
        
        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.issue (
                created_at, modifieddate, board_id, priority,
                resolution_date, time_spent, parent_id, is_deleted,
                assignee_id, creator_id, due_date, issue_id, key,
                parent_issue_id, project_id, reporter_id, status,
                summary, description, sprint_id, issue_url, org_id, current_progress, status_change_date, issue_type, parent_task_id
            ) VALUES (
                %(created_at)s, %(modifieddate)s, %(board_id)s, %(priority)s,
                %(resolution_date)s, %(time_spent)s, %(parent_id)s, %(is_deleted)s,
                %(assignee_id)s, %(creator_id)s, %(due_date)s, %(issue_id)s, %(key)s,
                %(parent_issue_id)s, %(project_id)s, %(reporter_id)s, %(status)s,
                %(summary)s, %(description)s, %(sprint_id)s, %(issue_url)s, %(org_id)s, %(current_progress)s, %(status_change_date)s, %(issue_type)s, %(parent_task_id)s
            )
        """
        
        # Update query
        update_query = """
            UPDATE insightly_jira.issue SET
                modifieddate = %(modifieddate)s,
                board_id = %(board_id)s,
                priority = %(priority)s,
                resolution_date = %(resolution_date)s,
                time_spent = %(time_spent)s,
                parent_id = %(parent_id)s,
                is_deleted = %(is_deleted)s,
                assignee_id = %(assignee_id)s,
                creator_id = %(creator_id)s,
                due_date = %(due_date)s,
                key = %(key)s,
                parent_issue_id = %(parent_issue_id)s,
                project_id = %(project_id)s,
                reporter_id = %(reporter_id)s,
                status = %(status)s,
                summary = %(summary)s,
                description = %(description)s,
                sprint_id = %(sprint_id)s,
                issue_url = %(issue_url)s,
                current_progress = %(current_progress)s,
                status_change_date = %(status_change_date)s,
                issue_type = %(issue_type)s,
                parent_task_id = %(parent_task_id)s
            WHERE issue_id = %(issue_id)s AND org_id = %(org_id)s
        """
        
        # Check if issue exists
        cursor.execute(check_query, issue)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing issue
            cursor.execute(update_query, issue)
        else:
            # Insert new issue
            cursor.execute(insert_query, issue)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting issue {issue.get('summary')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_custom_field_to_db(custom_field, conn):
    """Insert or update a single custom field"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if custom field exists
        check_query = """
            SELECT id FROM insightly_jira.account_custom_field 
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
            LIMIT 1
        """
        
        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.account_custom_field (
                jira_id, name, description, org_id
            ) VALUES (
                %(jira_id)s, %(name)s, %(description)s, %(org_id)s
            )
        """
        
        # Update query
        update_query = """
            UPDATE insightly_jira.account_custom_field SET
                name = %(name)s,
                description = %(description)s
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
        """
        
        # Check if custom field exists
        cursor.execute(check_query, custom_field)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing custom field
            cursor.execute(update_query, custom_field)
        else:
            # Insert new custom field
            cursor.execute(insert_query, custom_field)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting custom field {custom_field.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def get_custom_field_name_from_id(custom_item_id, org_id, conn):
    """
    Get custom field name by looking up custom_item_id in account_custom_field table
    Returns the name if found, None otherwise
    """
    if not custom_item_id:
        return None
    
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            SELECT name FROM insightly_jira.account_custom_field 
            WHERE jira_id = %s AND org_id = %s
            LIMIT 1
        """
        cursor.execute(query, (str(custom_item_id), str(org_id)))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching custom field name for ID {custom_item_id}: {e}")
        conn.rollback()  # Rollback to clear failed transaction state
        return None
    finally:
        if cursor:
            cursor.close()

def insert_list_custom_field_to_db(list_custom_field, conn):
    """Insert or update a single list custom field"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if list custom field exists
        check_query = """
            SELECT jira_id FROM insightly_jira.account_custom_field 
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
            LIMIT 1
        """

        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.account_custom_field (
                jira_id, name, data_type, org_id
            ) VALUES (
                %(jira_id)s, %(name)s, %(data_type)s, %(org_id)s
            )
        """
        
        # Update query
        update_query = """ 
            UPDATE insightly_jira.account_custom_field SET
                name = %(name)s,
                data_type = %(data_type)s
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
        """
        
        # Check if list custom field exists
        cursor.execute(check_query, list_custom_field)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing list custom field
            cursor.execute(update_query, list_custom_field)
        else:
            # Insert new list custom field
            cursor.execute(insert_query, list_custom_field)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting list custom field {list_custom_field.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_folder_custom_field_to_db(folder_custom_field, conn):
    """Insert or update a single folder custom field"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if folder custom field exists
        check_query = """
            SELECT jira_id FROM insightly_jira.account_custom_field 
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
            LIMIT 1
        """
        
        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.account_custom_field (
                jira_id, name, data_type, org_id
            ) VALUES (
                %(jira_id)s, %(name)s, %(data_type)s, %(org_id)s
            )
        """
        
        # Update query
        update_query = """
            UPDATE insightly_jira.account_custom_field SET
                name = %(name)s,
                data_type = %(data_type)s
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
        """
        
        # Check if folder custom field exists
        cursor.execute(check_query, folder_custom_field)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing folder custom field
            cursor.execute(update_query, folder_custom_field)
        else:
            # Insert new folder custom field
            cursor.execute(insert_query, folder_custom_field)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting folder custom field {folder_custom_field.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_space_custom_field_to_db(space_custom_field, conn):
    """Insert or update a single space custom field"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if space custom field exists
        check_query = """
            SELECT jira_id FROM insightly_jira.account_custom_field 
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
            LIMIT 1
        """
        
        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.account_custom_field (
                jira_id, name, data_type, org_id
            ) VALUES (
                %(jira_id)s, %(name)s, %(data_type)s, %(org_id)s
            )
        """
        
        # Update query
        update_query = """
            UPDATE insightly_jira.account_custom_field SET
                name = %(name)s,
                data_type = %(data_type)s
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
        """
        
        # Check if space custom field exists
        cursor.execute(check_query, space_custom_field)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing space custom field
            cursor.execute(update_query, space_custom_field)
        else:
            # Insert new space custom field
            cursor.execute(insert_query, space_custom_field)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting space custom field {space_custom_field.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def insert_workspace_custom_field_to_db(workspace_custom_field, conn):
    """Insert or update a single workspace custom field"""
    cursor = None
    
    try:
        cursor = conn.cursor()
        
        # Check query to see if workspace custom field exists
        check_query = """
            SELECT jira_id FROM insightly_jira.account_custom_field 
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
            LIMIT 1
        """
        
        # Insert query
        insert_query = """
            INSERT INTO insightly_jira.account_custom_field (
                jira_id, name, data_type, org_id
            ) VALUES (
                %(jira_id)s, %(name)s, %(data_type)s, %(org_id)s
            )
        """
        
        # Update query
        update_query = """
            UPDATE insightly_jira.account_custom_field SET
                name = %(name)s,
                data_type = %(data_type)s
            WHERE jira_id = %(jira_id)s AND org_id = %(org_id)s
        """
        
        # Check if workspace custom field exists
        cursor.execute(check_query, workspace_custom_field)
        existing = cursor.fetchone()
        
        if existing:
            # Update existing workspace custom field
            cursor.execute(update_query, workspace_custom_field)
        else:
            # Insert new workspace custom field
            cursor.execute(insert_query, workspace_custom_field)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error upserting workspace custom field {workspace_custom_field.get('name')}: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()