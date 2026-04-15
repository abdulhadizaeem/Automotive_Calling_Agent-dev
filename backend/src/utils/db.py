import os
from datetime import datetime, timezone
import bcrypt
import urllib.parse
import json
import psycopg2
from psycopg2 import pool 
import logging
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import traceback

load_dotenv()

class PGDB:
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if PGDB._pool is not None:
            return  # Already initialized
            
        self.connection_string = os.getenv('DATABASE_URL')
        self.disable_pool = os.getenv("DISABLE_PG_POOL", "false").lower() in ("1", "true", "yes")

        # Some Postgres+SSL setups drop idle pooled connections, causing:
        # "SSL connection has been closed unexpectedly".
        # Allow disabling pooling and opening a fresh connection per request.
        if not self.disable_pool:
            PGDB._pool = pool.SimpleConnectionPool(5, 50, self.connection_string)
        else:
            PGDB._pool = None
        
        # ✅ Create tables ONCE (in correct order due to foreign keys)
        self.create_users_table()
        self.create_call_history_table()
        self.create_appointments_table()
        self.create_user_prompts_table()
        self.create_retell_webhook_dedupe_table()
        self.create_inbound_call_settings_table()
        self.ensure_call_history_schema()
        self.ensure_users_schema()
        self.ensure_appointments_schema()
        self.ensure_user_prompts_schema()

    def get_connection(self):
        """
        Get a live DB connection.
        - If pooling is disabled: open a fresh connection each call.
        - If pooling is enabled: lease from pool and probe liveness, replacing stale sockets.
        """
        if getattr(self, "disable_pool", False) or PGDB._pool is None:
            return psycopg2.connect(self.connection_string)

        conn = PGDB._pool.getconn()
        try:
            # Probe liveness (cheap, catches stale SSL sockets)
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
            return conn
        except psycopg2.Error:
            # Drop broken connection and retry once
            try:
                PGDB._pool.putconn(conn, close=True)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
            conn2 = PGDB._pool.getconn()
            try:
                with conn2.cursor() as cursor:
                    cursor.execute("SELECT 1;")
            except Exception:
                # if this also fails, let caller handle
                pass
            return conn2
    
    def release_connection(self, conn):
        """Return connection to pool"""
        try:
            if conn is None:
                return
            if getattr(self, "disable_pool", False) or PGDB._pool is None:
                try:
                    conn.close()
                finally:
                    return
            if getattr(conn, "closed", 0):
                PGDB._pool.putconn(conn, close=True)
            else:
                PGDB._pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    # ==================== TABLE CREATION METHODS ====================
    
    def create_users_table(self):
        """
        Create users table with ALL columns from production schema
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(100),
                        email VARCHAR(100) UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        is_admin BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            conn.commit()
            logging.info("✅ users table created")
        except Exception as e:
            logging.error(f"Error creating users table: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def create_call_history_table(self):
        """
        Create call_history table with ALL columns from production schema
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS call_history (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        call_id TEXT NOT NULL UNIQUE,
                        status TEXT,
                        duration DOUBLE PRECISION,
                        transcript JSONB,
                        summary TEXT,
                        recording_url TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        started_at TIMESTAMPTZ NULL,
                        ended_at TIMESTAMPTZ NULL,
                        voice_id TEXT,
                        voice_name TEXT,
                        from_number TEXT NULL,
                        to_number TEXT NULL,
                        transcript_url TEXT,
                        transcript_blob TEXT,
                        recording_blob TEXT,
                        events_log JSONB DEFAULT '[]',
                        agent_events JSONB DEFAULT '[]',
                        recording_blob_data BYTEA NULL,
                        recording_size INTEGER NULL,
                        recording_content_type VARCHAR(100) DEFAULT 'audio/ogg'
                    );
                """)
                
                # Only essential indexes for performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_events_log ON call_history USING GIN (events_log);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_agent_events ON call_history USING GIN (agent_events);")
                
            conn.commit()
            logging.info("✅ call_history table created")
        except Exception as e:
            logging.error(f"Error creating call_history table: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def create_appointments_table(self):
        """
        Create appointments table with ALL columns from production schema
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS appointments (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        appointment_date DATE NOT NULL,
                        start_time TIME NOT NULL,
                        end_time TIME NOT NULL,
                        attendee_email VARCHAR(255) NOT NULL,
                        attendee_name VARCHAR(255),
                        title TEXT NOT NULL,
                        description TEXT,
                        notes TEXT,
                        status VARCHAR(50) DEFAULT 'scheduled',
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            conn.commit()
            logging.info("✅ appointments table created")
        except Exception as e:
            logging.error(f"Error creating appointments table: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def create_user_prompts_table(self):
        """
        Create table to store per-user system prompt customizations.
        Each user has ONE active prompt stored as plain text.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_prompts (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
                        system_prompt TEXT NOT NULL DEFAULT 'You are SUMA, a helpful AI assistant. Be professional and courteous in all interactions.',
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                """)
            conn.commit()
            logging.info("✅ user_prompts table created")
        except Exception as e:
            logging.error(f"Error creating user_prompts table: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def create_retell_webhook_dedupe_table(self):
        """Idempotency for Retell POST /retell-webhook deliveries."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS retell_webhook_dedupe (
                        id SERIAL PRIMARY KEY,
                        call_id TEXT NOT NULL,
                        dedupe_key TEXT NOT NULL,
                        event TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(call_id, dedupe_key)
                    );
                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_retell_dedupe_call_id ON retell_webhook_dedupe (call_id);"
                )
            conn.commit()
            logging.info("✅ retell_webhook_dedupe table ready")
        except Exception as e:
            logging.error(f"Error creating retell_webhook_dedupe: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def create_inbound_call_settings_table(self):
        """
        Stores per-user inbound configuration needed to populate Retell dynamic variables
        (business_name, call_context, agent_name).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inbound_call_settings (
                        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                        business_name TEXT,
                        call_context TEXT,
                        agent_name TEXT,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
            conn.commit()
            logging.info("✅ inbound_call_settings table ready")
        except Exception as e:
            logging.error(f"Error creating inbound_call_settings: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def ensure_call_history_schema(self):
        """
        Add columns missing from older call_history tables (Retell logs, numbers, recordings).
        Idempotent; safe on every startup. Requires PostgreSQL 11+ (ADD COLUMN IF NOT EXISTS).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT to_regclass('public.call_history') IS NOT NULL;"
                )
                if not cursor.fetchone()[0]:
                    conn.commit()
                    logging.warning("ensure_call_history_schema: call_history table not found")
                    return

                alters = [
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS status TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS duration DOUBLE PRECISION;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS transcript JSONB;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS summary TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_url TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS voice_id TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS voice_name TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS from_number TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS to_number TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS transcript_url TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS transcript_blob TEXT;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_blob TEXT;",
                    (
                        "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS events_log JSONB "
                        "DEFAULT '[]'::jsonb;"
                    ),
                    (
                        "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS agent_events JSONB "
                        "DEFAULT '[]'::jsonb;"
                    ),
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_blob_data BYTEA;",
                    "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_size INTEGER;",
                    (
                        "ALTER TABLE call_history ADD COLUMN IF NOT EXISTS recording_content_type "
                        "VARCHAR(100) DEFAULT 'audio/ogg';"
                    ),
                ]
                for stmt in alters:
                    cursor.execute(stmt)

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_call_history_events_log "
                    "ON call_history USING GIN (events_log);"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_call_history_agent_events "
                    "ON call_history USING GIN (agent_events);"
                )
            conn.commit()
            logging.info("call_history schema migration applied (if needed)")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error migrating call_history schema: {e}")
            traceback.print_exc()
        finally:
            self.release_connection(conn)

    def ensure_users_schema(self):
        """Add columns that older deployments may lack."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass('public.users') IS NOT NULL;")
                if not cursor.fetchone()[0]:
                    conn.commit()
                    return
                for stmt in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(100);",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;",
                    (
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at "
                        "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at "
                        "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
                    ),
                ):
                    cursor.execute(stmt)
            conn.commit()
            logging.info("users schema migration applied (if needed)")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error migrating users schema: {e}")
            traceback.print_exc()
        finally:
            self.release_connection(conn)

    def ensure_appointments_schema(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass('public.appointments') IS NOT NULL;")
                if not cursor.fetchone()[0]:
                    conn.commit()
                    return
                for stmt in (
                    "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS notes TEXT;",
                    "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS description TEXT;",
                    (
                        "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS status "
                        "VARCHAR(50) DEFAULT 'scheduled';"
                    ),
                    (
                        "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS created_at "
                        "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
                    ),
                ):
                    cursor.execute(stmt)
            conn.commit()
            logging.info("appointments schema migration applied (if needed)")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error migrating appointments schema: {e}")
            traceback.print_exc()
        finally:
            self.release_connection(conn)

    def ensure_user_prompts_schema(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass('public.user_prompts') IS NOT NULL;")
                if not cursor.fetchone()[0]:
                    conn.commit()
                    return
                cursor.execute(
                    "ALTER TABLE user_prompts ADD COLUMN IF NOT EXISTS updated_at "
                    "TIMESTAMP DEFAULT NOW();"
                )
            conn.commit()
            logging.info("user_prompts schema migration applied (if needed)")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error migrating user_prompts schema: {e}")
            traceback.print_exc()
        finally:
            self.release_connection(conn)

    # ==================== USER PROMPTS METHODS ====================

    def create_default_user_prompt(self, user_id: int):
        """
        Create default prompt for a new user.
        Called automatically on user registration.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                default_prompt = """You are SUMA, an AI that makes phone calls to businesses on behalf of clients to book appointments and reservations.
                #### WHO YOU ARE:
                - You are a professional AI assistant.
                - You represent the **business or service provider**, not the end-customer.
                - You always call **on behalf of the business** to potential customers.
                - You act as the service provider’s representative, offering or confirming services.but dont say that you are from service side.

                #### WHO YOU'RE CALLING:
                - A potential customer or lead who might be interested in the business’s services.
                - Someone who may need to **visit, attend, or try the service** (e.g., take a car test drive, attend a consultation, visit a showroom, etc.).
                - They are the **recipient** of the service being offered.

                #### YOUR MISSION:
                - Clearly state that you are calling **from the service provider’s side**.
                - Your main goal is to **check if the person is available for the offered service** (for example, a car test drive or a showroom visit).
                - If they are available, schedule or book the appointment right away.

                ### CONVERSATION PROTOCOL - MANDATORY SEQUENCE

                #### STEP 1: INTRODUCTION [REQUIRED - ALWAYS START HERE]
                **Rules:**
                - Greet the person naturally and politely.
                - Use this structure: “Hi! This is [Agent Name] calling on behalf of [Business Name].”
                - Always mention you’re calling **on behalf of the service provider**.
                - Do not ask “How are you?” — keep it brief and professional.

                #### STEP 2: STATE PURPOSE [REQUIRED]
                Then immediately explain the reason for the call:
                - Example: “We’re calling to see if you’re available to come by for a test drive at our showroom.”
                - Mention the service clearly (e.g. car test drive, salon visit, consultation, demo, etc.).
                - Be concise and specific.

                **Rules:**
                - Be clear and direct about why you’re calling.
                - Don’t assume they know who you are or what the call is about.
                - State your purpose **once only**.

                #### STEP 3: LISTEN & GATHER OPTIONS [REQUIRED]
                **Actions:**
                - Let them respond and share their availability.
                - Ask clarifying questions if needed (e.g., preferred date/time).
                - Once you have a confirmed time, immediately proceed to booking.

                **Rules:**
                - Keep responses short (1–2 sentences max).
                - Never repeat yourself or restate details they already understood.
                - Confirm booking details once and move on.
                """
                cursor.execute("""
                    INSERT INTO user_prompts (user_id, system_prompt)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO NOTHING;
                """, (user_id, default_prompt))
            conn.commit()
            logging.info(f"✅ Created default prompt for user {user_id}")
        except Exception as e:
            logging.error(f"Error creating default prompt: {e}")
        finally:
            self.release_connection(conn)

    def get_user_prompt(self, user_id: int) -> dict:
        """
        Get the user's current system prompt.
        Returns None if not found (creates default if missing).
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id,
                        user_id,
                        system_prompt,
                        created_at,
                        updated_at
                    FROM user_prompts
                    WHERE user_id = %s;
                """, (user_id,))
                result = cursor.fetchone()
                
                # If no prompt exists, create default
                if not result:
                    self.create_default_user_prompt(user_id)
                    cursor.execute("""
                        SELECT 
                            id,
                            user_id,
                            system_prompt,
                            created_at,
                            updated_at
                        FROM user_prompts
                        WHERE user_id = %s;
                    """, (user_id,))
                    result = cursor.fetchone()
                
                return result
        finally:
            self.release_connection(conn)

    def update_user_system_prompt(self, user_id: int, system_prompt: str):
        """
        Update user's system prompt.
        Stores exactly what is provided - no parsing.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    UPDATE user_prompts
                    SET system_prompt = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                    RETURNING 
                        id,
                        user_id,
                        system_prompt,
                        created_at,
                        updated_at;
                """, (system_prompt, user_id))
                result = cursor.fetchone()
            
            conn.commit()
            logging.info(f"✅ Updated prompt for user {user_id}")
            return result
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating user prompt: {e}")
            raise
        finally:
            self.release_connection(conn)

    def reset_user_prompt_to_default(self, user_id: int):
        """
        Reset user's prompt to default text.
        """
        default_prompt = """You are SUMA, a professional AI assistant for business services.

Your responsibilities:
- Help schedule appointments and meetings
- Answer questions about services
- Be respectful, patient, and adapt to the business's communication style
- Always confirm important details before proceeding

Tone: Professional and friendly"""
        
        return self.update_user_system_prompt(user_id, default_prompt)

    def get_user_customization_dict(self, user_id: int) -> dict:
        """
        Get user's system prompt for use in call initiation.
        
        Returns:
            dict with key: system_prompt (full text)
        """
        prompt_data = self.get_user_prompt(user_id)
        
        if not prompt_data:
            # Return default if not found
            return {
                "system_prompt": """You are SUMA, a professional AI assistant for business services.

Your responsibilities:
- Help schedule appointments and meetings
- Answer questions about services
- Be respectful, patient, and adapt to the business's communication style
- Always confirm important details before proceeding

Tone: Professional and friendly"""
            }
        
        return {
            "system_prompt": prompt_data['system_prompt']
        }

    # ==================== RECORDING METHODS ====================

    def store_recording_blob(self, call_id: str, recording_data: bytes, content_type: str = "audio/ogg"):
        """Store actual recording bytes"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE call_history
                    SET recording_blob_data = %s,
                        recording_size = %s,
                        recording_content_type = %s
                    WHERE call_id = %s;
                """, (psycopg2.Binary(recording_data), len(recording_data), content_type, call_id))
            conn.commit()
            logging.info(f"✅ Stored {len(recording_data)} bytes for {call_id}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error storing recording: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_recording_blob(self, call_id: str, user_id: int = None):
        """
        Retrieve recording bytes from database.
        Returns: (bytes, content_type, size) or (None, None, None)
        
        Args:
            call_id: Call identifier
            user_id: User ID (optional, skip check if None for verification)
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                if user_id is not None:
                    # Normal query with user_id check
                    cursor.execute("""
                        SELECT recording_blob_data, recording_content_type, recording_size
                        FROM call_history
                        WHERE call_id = %s AND user_id = %s;
                    """, (call_id, user_id))
                else:
                    # Verification query without user_id check
                    cursor.execute("""
                        SELECT recording_blob_data, recording_content_type, recording_size
                        FROM call_history
                        WHERE call_id = %s;
                    """, (call_id,))
                
                row = cursor.fetchone()
                if row and row[0]:
                    return row[0], row[1], row[2]  # (bytes, content_type, size)
                return None, None, None
        except Exception as e:
            logging.error(f"❌ Error retrieving recording blob: {e}")
            return None, None, None
        finally:
            self.release_connection(conn)

    # ==================== USER MANAGEMENT METHODS ====================

    def register_user(self, user_data):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if email already exists
                cursor.execute("SELECT id FROM users WHERE email = %s", (user_data['email'],))
                if cursor.fetchone():
                    raise ValueError("Email already registered.")

                # Hash the password
                hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())

                # Insert user
                cursor.execute("""
                    INSERT INTO users (username, email, password_hash, is_admin)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, username, email, created_at, is_admin;
                """, (
                    user_data['username'],
                    user_data['email'],
                    hashed_password.decode('utf-8'),
                    user_data.get('is_admin', False)
                ))

                row = cursor.fetchone()
                user_id = row["id"]
                conn.commit()

                # ✅ AUTO-CREATE DEFAULT PROMPT FOR NEW USER
                self.create_default_user_prompt(user_id)
                logging.info(f"✅ Created default prompt for new user {user_id}")

                return {
                    "id": row["id"],
                    "username": row["username"],
                    "email": row["email"],
                    "created_at": row["created_at"]
                }

        except Exception as e:
            conn.rollback()
            logging.error(f"Error in register_user: {e}")
            raise
        finally:
            self.release_connection(conn)

    def login_user(self, user_data):
        """Verify user credentials by username or email and return user info."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, username, email, password_hash,first_name,last_name,created_at,is_admin
                    FROM users
                    WHERE username = %s OR email = %s
                    LIMIT 1
                """, (user_data.get("username"), user_data['email']))

                result = cursor.fetchone()

                if result and bcrypt.checkpw(user_data['password'].encode('utf-8'), result[3].encode('utf-8')):
                    return {
                        "id": result[0],
                        "username": result[1],
                        "email": result[2],
                        "created_at": result[6],
                        "is_admin": result[7]
                    }
                else:
                    raise ValueError("Invalid username or password.")
        except Exception as e:
            logging.error(f"Error during login: {str(e)}")
            raise
        finally:
            self.release_connection(conn)

    def get_user_by_id(self, user_id: int):
        """Get user by ID"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id,first_name,last_name,username,email,is_admin,created_at FROM users WHERE id = %s",
                    (user_id,)
                )
                return cursor.fetchone()
        finally:
            if conn:
                self.release_connection(conn)

    def delete_user_by_id(self, user_id):
        """
        delete user by id
        """
        conn = None 
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM users WHERE id = %s
                    """,
                    (user_id,)     
                )
            conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error deleting user {user_id}: {e}")
            if conn:
                conn.rollback()
                return False
        finally:
            if conn:
                self.release_connection(conn)

    def update_user_name_fields(self, user_id: int, first_name: str, last_name: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE users
                    SET first_name = %s,
                        last_name = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (first_name, last_name, user_id))
            conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error updating name fields: {e}")
            return False
        finally:
            self.release_connection(conn)

    def change_user_password(self, user_id: int, current_password: str, new_password: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT password_hash FROM users WHERE id = %s
                """, (user_id,))
                result = cursor.fetchone()
                if not result:
                    raise ValueError("User not found.")

                # Verify current password
                if not bcrypt.checkpw(current_password.encode(), result[0].encode()):
                    raise ValueError("Current password is incorrect.")

                # Hash new password
                new_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

                # Update
                cursor.execute("""
                    UPDATE users SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_hash, user_id))
            conn.commit()
            return True
        except Exception as e:
            logging.error(f"Password change error: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_all_users(self):
        query = """
            SELECT id, first_name, last_name, username, email, is_admin, created_at
            FROM users
            WHERE is_admin = FALSE
            ORDER BY created_at DESC
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return [
                    {
                        "id": row[0],
                        "username": row[3],
                        "email": row[4],
                        "is_admin": row[5],
                        "created_at": row[6],
                    }
                    for row in result
                ]
        finally:
            self.release_connection(conn)

    def get_all_users_paginated(self, page: int = 1, page_size: int = 10):
        query_total = "SELECT COUNT(*) FROM users WHERE is_admin = FALSE"
        query_data = """
            SELECT id, first_name, last_name, username, email, is_admin, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Total user count
                cursor.execute(query_total)
                total_users = cursor.fetchone()[0]

                # Paginated data
                offset = (page - 1) * page_size
                cursor.execute(query_data, (page_size, offset))
                rows = cursor.fetchall()

                users = [
                    {
                        "id": row[0],
                        "username": row[3],
                        "email": row[4],
                        "is_admin": row[5],
                        "created_at": row[6],
                    }
                    for row in rows
                ]

            return {
                "users": users,
                "total": total_users
            }

        except Exception as e:
            print(f"Error fetching paginated users: {e}")
            return {"users": [], "total": 0}
        finally:
            self.release_connection(conn)

    # ==================== CALL HISTORY METHODS ====================

    def insert_call_history(
        self,
        user_id: int,
        call_id: str,
        status: str = None,
        voice_id: str = None,
        voice_name: str = None,
        to_number: str = None,
        from_number: str = None,
    ):
        """
        Insert a new call history record with initial data.
        Other fields (transcript, summary, duration, etc.) will be updated later.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                values = (
                    user_id, call_id, status,
                    voice_id, voice_name, to_number, from_number
                )

                cursor.execute("""
                    INSERT INTO call_history (
                        user_id, call_id, status,
                        voice_id, voice_name, to_number, from_number
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                """, values)

                row = cursor.fetchone()
                conn.commit()
                return row[0] if row else None

        except Exception as e:
            logging.error(f"Error inserting call history: {e}")
            conn.rollback()
            raise
        finally:
            self.release_connection(conn)

    def update_call_history(self, call_id: str, updates: dict):
        """
        Update specific fields in the call_history record based on the call_id.

        Args:
            call_id (str): The unique identifier for the call.
            updates (dict): A dictionary where keys are column names and values
                            are the new values to set. e.g., {"status": "completed", "duration": 120.5}
        """
        if not updates:
            logging.warning("update_call_history called with no updates.")
            return None

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Build the SET part of the SQL query dynamically
                set_clauses = []
                param_values = []
                for key, value in updates.items():
                    
                    if not key.replace('_', '').isalnum():
                        logging.error(f"Invalid column name detected: {key}")
                        raise ValueError(f"Invalid column name: {key}")

                    # Handle JSON data specifically
                    if key == 'transcript' and value is not None:
                        set_clauses.append(f"{key} = %s")
                        param_values.append(json.dumps(value))
                    else:
                        set_clauses.append(f"{key} = %s")
                        param_values.append(value)

                if not set_clauses:
                    logging.warning("No valid fields to update.")
                    return None

                set_sql = ", ".join(set_clauses)
                sql = f"UPDATE call_history SET {set_sql} WHERE call_id = %s RETURNING id;"
                
                # Add call_id to the parameters list
                param_values.append(call_id)

                logging.debug(f"Executing SQL: {sql} with params: {param_values}")

                cursor.execute(sql, tuple(param_values))

                row = cursor.fetchone()
                conn.commit()
                logging.info(f"Updated call_history for call_id {call_id}. Updated fields: {list(updates.keys())}")
                return row[0] if row else None

        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating call history for call_id={call_id}: {e}")
            traceback.print_exc()
            raise
        finally:
            self.release_connection(conn)

    def get_call_history_by_user_id(self, user_id: int, page: int = 1, page_size: int = 10):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT COUNT(*) FROM call_history WHERE user_id = %s", (user_id,))
                total = cursor.fetchone()["count"]

                # Count completed
                cursor.execute("""
                    SELECT COUNT(*) FROM call_history 
                    WHERE user_id = %s AND status = 'completed'
                """, (user_id,))
                completed_calls = cursor.fetchone()["count"]

                not_completed_calls = total - completed_calls

                # Paginated query
                offset = (page - 1) * page_size
                cursor.execute("""
                    SELECT ch.id, ch.call_id, ch.status, ch.duration, ch.transcript,
                        ch.summary, ch.recording_url, ch.created_at, ch.started_at, ch.ended_at,
                        ch.voice_id, ch.voice_name, ch.from_number, ch.to_number,
                        ch.transcript_url, ch.transcript_blob, ch.recording_blob,
                        ch.events_log, ch.agent_events,
                        u.id AS user_id, u.username, u.email
                    FROM call_history ch
                    JOIN users u ON ch.user_id = u.id
                    WHERE ch.user_id = %s
                    ORDER BY ch.created_at DESC
                    LIMIT %s OFFSET %s
                """, (user_id, page_size, offset))

                rows = cursor.fetchall()

                # Ensure transcript is JSON
                for row in rows:
                    if isinstance(row["transcript"], str):
                        try:
                            row["transcript"] = json.loads(row["transcript"])
                        except Exception:
                            logging.warning(f"Invalid JSON in transcript for call_id={row['call_id']}")
                    for key in ("events_log", "agent_events"):
                        if isinstance(row.get(key), str):
                            try:
                                row[key] = json.loads(row[key])
                            except Exception:
                                row[key] = []

                return {
                    "calls": rows,
                    "total": total,
                    "completed_calls": completed_calls,
                    "not_completed_calls": not_completed_calls,
                    "page": page,
                    "page_size": page_size
                }
        except Exception as e:
            logging.error(f"Error fetching call history for user_id={user_id}: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_call_by_id(self, call_id: str, user_id: int):
        """Get a specific call by ID for a user"""
        query = """
            SELECT id, call_id, status, duration, transcript, recording_url, 
                transcript_url, transcript_blob, recording_blob,
                created_at, started_at, ended_at, 
                from_number, to_number, voice_name
            FROM call_history
            WHERE call_id = %s AND user_id = %s
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (call_id, user_id))
                result = cursor.fetchone()
                
                if result and isinstance(result.get("transcript"), str):
                    try:
                        result["transcript"] = json.loads(result["transcript"])
                    except:
                        pass
                
                return result
        except Exception as e:
            logging.error(f"Error getting call by ID: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_call_dashboard_detail(self, call_id: str, user_id: int):
        """Full call row including Retell webhook trail in events_log / agent_events."""
        query = """
            SELECT id, call_id, status, duration, transcript, summary, recording_url,
                transcript_url, transcript_blob, recording_blob,
                created_at, started_at, ended_at,
                from_number, to_number, voice_name, events_log, agent_events
            FROM call_history
            WHERE call_id = %s AND user_id = %s
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (call_id, user_id))
                result = cursor.fetchone()
                if result and isinstance(result.get("transcript"), str):
                    try:
                        result["transcript"] = json.loads(result["transcript"])
                    except Exception:
                        pass
                for key in ("events_log", "agent_events"):
                    if result and isinstance(result.get(key), str):
                        try:
                            result[key] = json.loads(result[key])
                        except Exception:
                            pass
                return result
        finally:
            self.release_connection(conn)

    def get_dashboard_summary_stats(self, user_id: int, days: int) -> dict:
        """Aggregates for dashboard charts (call_history + appointments)."""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*)::int AS total_calls,
                        COUNT(*) FILTER (WHERE status = 'completed')::int AS successful_calls,
                        COUNT(*) FILTER (WHERE status = 'unanswered')::int AS unanswered_calls,
                        COALESCE(SUM(duration), 0)::float AS total_duration_seconds
                    FROM call_history
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                row = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS total
                    FROM appointments
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                appt_row = cursor.fetchone() or {}
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS cnt
                    FROM (
                        SELECT from_number
                        FROM call_history
                        WHERE user_id = %s
                          AND from_number IS NOT NULL
                          AND TRIM(from_number) <> ''
                          AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                        GROUP BY from_number
                        HAVING COUNT(*) > 1
                    ) t """,
                    (user_id, max(1, min(days, 366))),
                )
                repeat_phones = cursor.fetchone() or {}
                total_calls = int(row.get("total_calls") or 0)
                unique_cursor = (
                    """
                    SELECT COUNT(DISTINCT from_number)::int AS u
                    FROM call_history
                    WHERE user_id = %s
                      AND from_number IS NOT NULL
                      AND TRIM(from_number) <> ''
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    """
                )
                cursor.execute(
                    unique_cursor,
                    (user_id, max(1, min(days, 366))),
                )
                uniq_row = cursor.fetchone() or {}
                unique_phones = int(uniq_row.get("u") or 0)
                repeat_n = int(repeat_phones.get("cnt") or 0)
                new_callers = max(0, unique_phones - repeat_n)
                total_minutes = float(row.get("total_duration_seconds") or 0) / 60.0
                cursor.execute(
                    """
                    SELECT status, COUNT(*)::int AS c
                    FROM appointments
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY status
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                appt_status_rows = cursor.fetchall() or []
                appointment_status_distribution = {
                    r["status"]: r["c"] for r in appt_status_rows if r.get("status")
                }
                return {
                    "total_calls": total_calls,
                    "successful_calls": int(row.get("successful_calls") or 0),
                    "unanswered_calls": int(row.get("unanswered_calls") or 0),
                    "total_minutes": round(total_minutes, 2),
                    "repeat_callers": repeat_n,
                    "new_callers": new_callers,
                    "total_appointments_in_period": int(appt_row.get("total") or 0),
                    "appointment_status_distribution": appointment_status_distribution,
                }
        finally:
            self.release_connection(conn)

    def get_calls_over_time(self, user_id: int, days: int) -> list[dict]:
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        (created_at AT TIME ZONE 'UTC')::date AS day,
                        COUNT(*)::int AS count
                    FROM call_history
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY day
                    ORDER BY day ASC
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                rows = cursor.fetchall() or []
                return [{"date": r["day"].isoformat(), "count": r["count"]} for r in rows]
        finally:
            self.release_connection(conn)

    def get_appointments_over_time(self, user_id: int, days: int) -> list[dict]:
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        (created_at AT TIME ZONE 'UTC')::date AS day,
                        COUNT(*)::int AS count
                    FROM appointments
                    WHERE user_id = %s
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY day
                    ORDER BY day ASC
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                rows = cursor.fetchall() or []
                return [{"date": r["day"].isoformat(), "count": r["count"]} for r in rows]
        finally:
            self.release_connection(conn)

    def get_top_repeat_callers(self, user_id: int, days: int, limit: int = 10) -> list[dict]:
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT from_number AS phone, COUNT(*)::int AS call_count
                    FROM call_history
                    WHERE user_id = %s
                      AND from_number IS NOT NULL
                      AND TRIM(from_number) <> ''
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY from_number
                    ORDER BY call_count DESC
                    LIMIT %s
                    """,
                    (user_id, max(1, min(days, 366)), max(1, min(limit, 50))),
                )
                rows = cursor.fetchall() or []
                return [
                    {"phone": r["phone"], "name": "", "call_count": r["call_count"]}
                    for r in rows
                ]
        finally:
            self.release_connection(conn)

    def get_sentiment_breakdown(self, user_id: int, days: int) -> dict[str, int]:
        import re

        rows = []
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT summary
                    FROM call_history
                    WHERE user_id = %s
                      AND summary IS NOT NULL
                      AND TRIM(summary) <> ''
                      AND created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    """,
                    (user_id, max(1, min(days, 366))),
                )
                rows = cursor.fetchall()
        finally:
            self.release_connection(conn)
        breakdown: dict[str, int] = {}
        pat = re.compile(r"Sentiment:\s*([^\n]+)", re.IGNORECASE)
        for (summary,) in rows:
            if not summary:
                continue
            m = pat.search(str(summary))
            if m:
                label = m.group(1).strip().lower()
                if len(label) > 64:
                    label = label[:64]
                breakdown[label] = breakdown.get(label, 0) + 1
            else:
                breakdown["unknown"] = breakdown.get("unknown", 0) + 1
        return breakdown

    def get_dashboard_combined_stats(self, user_id: int) -> dict:
        """Lightweight counters for dashboard header widgets."""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS total
                    FROM call_history
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                total_calls = (cursor.fetchone() or {}).get("total") or 0
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS c
                    FROM call_history
                    WHERE user_id = %s AND status = 'completed'
                    """,
                    (user_id,),
                )
                completed = (cursor.fetchone() or {}).get("c") or 0
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS c
                    FROM call_history
                    WHERE user_id = %s
                      AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'utc')
                    """,
                    (user_id,),
                )
                calls_today = (cursor.fetchone() or {}).get("c") or 0
                cursor.execute(
                    """
                    SELECT COUNT(*)::int AS c
                    FROM appointments
                    WHERE user_id = %s AND status = 'scheduled'
                    """,
                    (user_id,),
                )
                appt_scheduled = (cursor.fetchone() or {}).get("c") or 0
                return {
                    "calls": {
                        "total": total_calls,
                        "completed": completed,
                        "today": calls_today,
                    },
                    "appointments": {
                        "scheduled": appt_scheduled,
                    },
                }
        finally:
            self.release_connection(conn)

    def add_call_event(self, call_id: str, event_type: str, event_data: dict = None):
        """Add a unique event entry into call_history.events_log"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Fetch existing events
                cursor.execute("SELECT events_log FROM call_history WHERE call_id = %s", (call_id,))
                row = cursor.fetchone()
                if not row:
                    logging.warning(f"Call {call_id} not found for event {event_type}")
                    return
                
                events_log = row[0] or []
                if isinstance(events_log, str):
                    try:
                        events_log = json.loads(events_log)
                    except Exception:
                        events_log = []

                # Check for duplicate event
                if any(ev.get("event") == event_type for ev in events_log):
                    logging.info(f"Duplicate event {event_type} ignored for {call_id}")
                    return

                # Append event
                events_log.append({
                    "event": event_type,
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": event_data or {}
                })

                # Update DB
                cursor.execute(
                    "UPDATE call_history SET events_log = %s WHERE call_id = %s",
                    (json.dumps(events_log), call_id)
                )

            conn.commit()
            logging.info(f"Event '{event_type}' added to call {call_id}")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error adding call event: {e}")
        finally:
            self.release_connection(conn)

    def add_agent_event(self, call_id: str, event_type: str, event_data: dict = None, timestamp: str = None):
        """Add a unique agent event entry into call_history.agent_events"""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Fetch existing events
                cursor.execute("SELECT agent_events FROM call_history WHERE call_id = %s", (call_id,))
                row = cursor.fetchone()
                if not row:
                    logging.warning(f"Call {call_id} not found for agent event {event_type}")
                    return
                
                events_log = row[0] or []
                if isinstance(events_log, str):
                    try:
                        events_log = json.loads(events_log)
                    except Exception:
                        events_log = []

                # Check for duplicate (within 5s timestamp tolerance)
                now = datetime.now(timezone.utc)
                for ev in events_log:
                    if (ev.get("event_type") == event_type and 
                        abs((now - datetime.fromisoformat(ev.get("timestamp").replace("Z", "+00:00"))).total_seconds()) < 5):
                        logging.info(f"Duplicate agent event {event_type} ignored for {call_id}")
                        return

                # Append event
                events_log.append({
                    "event_type": event_type,
                    "event_data": event_data or {},
                    "timestamp": timestamp,
                    "received_at": datetime.now(timezone.utc).isoformat()
                })

                # Update DB
                cursor.execute(
                    "UPDATE call_history SET agent_events = %s WHERE call_id = %s",
                    (json.dumps(events_log), call_id)
                )

            conn.commit()
            logging.info(f"Agent event '{event_type}' added to call {call_id}")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error adding agent event: {e}")
            traceback.print_exc()
            raise
        finally:
            self.release_connection(conn)

    def call_exists(self, call_id: str) -> bool:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM call_history WHERE call_id = %s LIMIT 1",
                    (call_id,),
                )
                return cursor.fetchone() is not None
        finally:
            self.release_connection(conn)

    def append_events_log_entry(self, call_id: str, event_type: str, event_data: dict = None):
        """Append to events_log without deduplication (for streaming Retell events)."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT events_log FROM call_history WHERE call_id = %s",
                    (call_id,),
                )
                row = cursor.fetchone()
                if not row:
                    logging.warning(f"append_events_log_entry: call {call_id} not found")
                    return
                events_log = row[0] or []
                if isinstance(events_log, str):
                    try:
                        events_log = json.loads(events_log)
                    except Exception:
                        events_log = []
                events_log.append(
                    {
                        "event": event_type,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "data": event_data or {},
                    }
                )
                cursor.execute(
                    "UPDATE call_history SET events_log = %s WHERE call_id = %s",
                    (json.dumps(events_log), call_id),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"append_events_log_entry error: {e}")
        finally:
            self.release_connection(conn)

    def retell_webhook_event_seen(self, call_id: str, dedupe_key: str) -> bool:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 1 FROM retell_webhook_dedupe
                    WHERE call_id = %s AND dedupe_key = %s
                    LIMIT 1
                    """,
                    (call_id, dedupe_key),
                )
                return cursor.fetchone() is not None
        finally:
            self.release_connection(conn)

    def record_retell_webhook_event(self, call_id: str, dedupe_key: str, event: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO retell_webhook_dedupe (call_id, dedupe_key, event)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (call_id, dedupe_key) DO NOTHING
                    """,
                    (call_id, dedupe_key, event),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"record_retell_webhook_event: {e}")
        finally:
            self.release_connection(conn)

    def upsert_inbound_call_settings(
        self,
        user_id: int,
        business_name: str | None = None,
        call_context: str | None = None,
        agent_name: str | None = None,
    ):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    INSERT INTO inbound_call_settings (user_id, business_name, call_context, agent_name, updated_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        business_name = EXCLUDED.business_name,
                        call_context = EXCLUDED.call_context,
                        agent_name = EXCLUDED.agent_name,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING user_id, business_name, call_context, agent_name, updated_at;
                    """,
                    (user_id, business_name, call_context, agent_name),
                )
                row = cursor.fetchone()
            conn.commit()
            return row
        except Exception as e:
            conn.rollback()
            logging.error(f"upsert_inbound_call_settings error: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_inbound_call_settings(self, user_id: int) -> dict | None:
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, business_name, call_context, agent_name, updated_at
                    FROM inbound_call_settings
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                return cursor.fetchone()
        finally:
            self.release_connection(conn)

    def create_appointment(
        self,
        user_id: int,
        appointment_date: str,
        start_time: str,
        end_time: str,
        attendee_name: str,
        attendee_email: str,
        title: str,
        description: str
    ) -> int:
        """
        Create a new appointment in the database
        Returns the appointment ID
        """
        last_err: Exception | None = None
        for attempt in range(2):
            conn = self.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO appointments (
                            user_id, appointment_date, start_time, end_time,
                            attendee_name, attendee_email, title, description,
                            status, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        RETURNING id
                    """, (
                        user_id, appointment_date, start_time, end_time,
                        attendee_name, attendee_email, title, description,
                        'scheduled'
                    ))

                    appointment_id = cursor.fetchone()[0]
                    conn.commit()

                    logging.info(f"✅ Created appointment {appointment_id} for user {user_id}")
                    return appointment_id
            except psycopg2.OperationalError as e:
                last_err = e
                # stale SSL socket / dropped connection; retry once with a fresh connection
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
                if attempt == 0:
                    continue
                raise
            except Exception as e:
                last_err = e
                try:
                    conn.rollback()
                except Exception:
                    pass
                logging.error(f"❌ Error creating appointment: {e}")
                raise
            finally:
                self.release_connection(conn)

        # should never reach
        raise last_err or RuntimeError("create_appointment failed")

    def get_user_appointments(self, user_id: int, from_date: str | None = None) -> list[dict]:
        """
        Return scheduled appointments for a user (from a date onward).
        Used by GET /api/agent/get-appointments/{user_id}.
        """
        if not from_date:
            from_date = datetime.now(timezone.utc).date().isoformat()
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        appointment_date,
                        start_time,
                        end_time,
                        attendee_email,
                        attendee_name,
                        title,
                        description,
                        notes,
                        status,
                        created_at
                    FROM appointments
                    WHERE user_id = %s
                      AND appointment_date >= %s
                    ORDER BY appointment_date, start_time
                    """,
                    (user_id, from_date),
                )
                return cursor.fetchall() or []
        except Exception as e:
            logging.error(f"Error getting appointments for user_id={user_id}: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_user_appointments_dashboard(
        self,
        user_id: int,
        page: int = 1,
        page_size: int = 50,
        from_date: str | None = None,
    ) -> dict:
        """
        Dashboard appointments API helper.

        Returns:
        {
          "totals": {"total": int, "scheduled": int, "cancelled": int, "completed": int},
          "page": int,
          "page_size": int,
          "appointments": [ ... ]
        }
        """
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 50
        if page_size > 200:
            page_size = 200
        if not from_date:
            from_date = datetime.now(timezone.utc).date().isoformat()
        offset = (page - 1) * page_size

        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                      COUNT(*)::int AS total,
                      COUNT(*) FILTER (WHERE status = 'scheduled')::int AS scheduled,
                      COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                      COUNT(*) FILTER (WHERE status = 'completed')::int AS completed
                    FROM appointments
                    WHERE user_id = %s
                      AND appointment_date >= %s
                    """,
                    (user_id, from_date),
                )
                totals = cursor.fetchone() or {}

                cursor.execute(
                    """
                    SELECT
                        id,
                        appointment_date,
                        start_time,
                        end_time,
                        attendee_name,
                        attendee_email,
                        title,
                        description,
                        notes,
                        status,
                        created_at
                    FROM appointments
                    WHERE user_id = %s
                      AND appointment_date >= %s
                    ORDER BY appointment_date, start_time
                    OFFSET %s
                    LIMIT %s
                    """,
                    (user_id, from_date, offset, page_size),
                )
                appts = cursor.fetchall() or []
                return {
                    "totals": {
                        "total": int(totals.get("total") or 0),
                        "scheduled": int(totals.get("scheduled") or 0),
                        "cancelled": int(totals.get("cancelled") or 0),
                        "completed": int(totals.get("completed") or 0),
                    },
                    "page": page,
                    "page_size": page_size,
                    "appointments": appts,
                }
        except Exception as e:
            logging.error(f"Error getting dashboard appointments for user_id={user_id}: {e}")
            raise
        finally:
            self.release_connection(conn)


# import os
# from datetime import datetime
# import bcrypt
# import urllib.parse
# import json
# import psycopg2  # ✅ Keep this
# from psycopg2 import pool 
# import logging
# from psycopg2.extras import RealDictCursor
# from dotenv import load_dotenv
# import traceback

# import json
# from datetime import datetime

# load_dotenv()

# class PGDB:
#     _instance = None
#     _pool = None
    
#     def __new__(cls):
#         if cls._instance is None:
#             cls._instance = super().__new__(cls)
#         return cls._instance
    
#     def __init__(self):
#         if PGDB._pool is not None:
#             return  # Already initialized
            
#         self.connection_string = os.getenv('DATABASE_URL')
        
#         # ✅ Create pool ONCE
#         PGDB._pool = pool.SimpleConnectionPool(
#             5, 50, self.connection_string
#         )
        
#         # ✅ Create tables ONCE
#         self.create_users_table()
#         self.create_call_history_table()
#         self.create_appointments_table()
#         self.update_call_history_for_recordings() 
#         self.create_user_prompts_table()

#     def get_connection(self):
#         """Get connection from pool"""
#         return PGDB._pool.getconn()
    
#     def release_connection(self, conn):
#         """Return connection to pool"""
#         PGDB._pool.putconn(conn)

#     def create_user_prompts_table(self):
#         """
#         Create table to store per-user system prompt customizations.
#         Each user has ONE active prompt stored as plain text.
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     CREATE TABLE IF NOT EXISTS user_prompts (
#                         id SERIAL PRIMARY KEY,
#                         user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
#                         system_prompt TEXT DEFAULT 'You are SUMA, a helpful AI assistant. Be professional and courteous in all interactions.',
#                         created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
#                         updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
#                     );
#                 """)
#                 cursor.execute("""
#                     CREATE INDEX IF NOT EXISTS idx_user_prompts_user_id 
#                     ON user_prompts(user_id);
#                 """)
#             conn.commit()
#             logging.info("✅ user_prompts table created")
#         except Exception as e:
#             logging.error(f"Error creating user_prompts table: {e}")
#         finally:
#             self.release_connection(conn)


#     def create_default_user_prompt(self, user_id: int):
#         """
#         Create default prompt for a new user.
#         Called automatically on user registration.
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 default_prompt = """You are SUMA, you makes phone calls to businesses on behalf of clients to book appointments and reservations.

#                 ### IDENTITY & ROLE

#                 #### WHO YOU ARE:
#                 - You represent your client
#                 - You are the CUSTOMER calling the business
#                 - You are NOT affiliated with the business you're calling

#                 #### WHO YOU'RE CALLING:
#                 - A business employee (receptionist, host, scheduler)
#                 - Someone who has authority to book appointments
#                 - They work AT the business you're calling

#                 #### YOUR MISSION:
#                 {call_context}

#                 ### CONVERSATION PROTOCOL - MANDATORY SEQUENCE

#                 #### STEP 1: INTRODUCTION [REQUIRED - ALWAYS START HERE]
#                 **Template:** "Hi! This is {self.agent_name} calling on behalf of {self.caller_name}. How are you doing today?"

#                 **Rules:**
#                 - Use this exact greeting structure
#                 - Always mention you're calling on behalf of your client
#                 - Brief pleasantry to establish rapport
#                 - Wait for their response

#                 #### STEP 2: STATE PURPOSE [REQUIRED]
#                 **Template:** "I'm calling to [book an appointment/make a reservation] for {self.caller_name}."

#                 Then provide specifics:
#                 - What service/appointment type is needed
#                 - Any preferences (time of day, specific provider, etc.)
#                 - Duration if relevant

#                 **Rules:**
#                 - Be clear and direct about why you're calling
#                 - Don't assume they know why you're calling
#                 - Provide enough detail for them to help you

#                 #### STEP 3: LISTEN & GATHER OPTIONS [REQUIRED]
#                 **Actions:**
#                 - Let them propose available dates/times
#                 - Ask clarifying questions if needed
#                 - Take note of their suggestions"""

                
#                 cursor.execute("""
#                     INSERT INTO user_prompts (user_id, system_prompt)
#                     VALUES (%s, %s)
#                     ON CONFLICT (user_id) DO NOTHING;
#                 """, (user_id, default_prompt))
#             conn.commit()
#             logging.info(f"✅ Created default prompt for user {user_id}")
#         except Exception as e:
#             logging.error(f"Error creating default prompt: {e}")
#         finally:
#             self.release_connection(conn)


#     def get_user_prompt(self, user_id: int) -> dict:
#         """
#         Get the user's current system prompt.
#         Returns None if not found (creates default if missing).
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute("""
#                     SELECT 
#                         id,
#                         user_id,
#                         system_prompt,
#                         created_at,
#                         updated_at
#                     FROM user_prompts
#                     WHERE user_id = %s;
#                 """, (user_id,))
#                 result = cursor.fetchone()
                
#                 # If no prompt exists, create default
#                 if not result:
#                     self.create_default_user_prompt(user_id)
#                     cursor.execute("""
#                         SELECT 
#                             id,
#                             user_id,
#                             system_prompt,
#                             created_at,
#                             updated_at
#                         FROM user_prompts
#                         WHERE user_id = %s;
#                     """, (user_id,))
#                     result = cursor.fetchone()
                
#                 return result
#         finally:
#             self.release_connection(conn)


#     def update_user_system_prompt(self, user_id: int, system_prompt: str):
#         """
#         Update user's system prompt.
#         Stores exactly what is provided - no parsing.
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute("""
#                     UPDATE user_prompts
#                     SET system_prompt = %s, updated_at = CURRENT_TIMESTAMP
#                     WHERE user_id = %s
#                     RETURNING 
#                         id,
#                         user_id,
#                         system_prompt,
#                         created_at,
#                         updated_at;
#                 """, (system_prompt, user_id))
#                 result = cursor.fetchone()
            
#             conn.commit()
#             logging.info(f"✅ Updated prompt for user {user_id}")
#             return result
            
#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error updating user prompt: {e}")
#             raise
#         finally:
#             self.release_connection(conn)


#     def reset_user_prompt_to_default(self, user_id: int):
#         """
#         Reset user's prompt to default text.
#         """
#         default_prompt = """You are SUMA, a professional AI assistant for business services.

#     Your responsibilities:
#     - Help schedule appointments and meetings
#     - Answer questions about services
#     - Be respectful, patient, and adapt to the business's communication style
#     - Always confirm important details before proceeding

#     Tone: Professional and friendly"""
        
#         return self.update_user_system_prompt(user_id, default_prompt)


#     def get_user_customization_dict(self, user_id: int) -> dict:
#         """
#         Get user's system prompt for use in call initiation.
        
#         Returns:
#             dict with key: system_prompt (full text)
#         """
#         prompt_data = self.get_user_prompt(user_id)
        
#         if not prompt_data:
#             # Return default if not found
#             return {
#                 "system_prompt": """You are SUMA, a professional AI assistant for business services.

#     Your responsibilities:
#     - Help schedule appointments and meetings
#     - Answer questions about services
#     - Be respectful, patient, and adapt to the business's communication style
#     - Always confirm important details before proceeding

#     Tone: Professional and friendly"""
#             }
        
#         return {
#             "system_prompt": prompt_data['system_prompt']
#         }

#     def update_call_history_for_recordings(self):
#         """
#         Add recording_blob_data column to store actual recording bytes
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 # Check if column exists
#                 cursor.execute("""
#                     SELECT column_name 
#                     FROM information_schema.columns 
#                     WHERE table_name='call_history' 
#                     AND column_name='recording_blob_data';
#                 """)
                
#                 if not cursor.fetchone():
#                     cursor.execute("""
#                         ALTER TABLE call_history 
#                         ADD COLUMN recording_blob_data BYTEA NULL,
#                         ADD COLUMN recording_size INTEGER NULL,
#                         ADD COLUMN recording_content_type VARCHAR(100) DEFAULT 'audio/ogg';
#                     """)
#                     logging.info("✅ Added recording_blob_data column")
#                 else:
#                     logging.info("ℹ️ recording_blob_data column already exists")
#             conn.commit()
#         except Exception as e:
#             logging.error(f"Error updating call_history for recordings: {e}")
#         finally:
#             self.release_connection(conn)
   

#     def store_recording_blob(self, call_id: str, recording_data: bytes, content_type: str = "audio/ogg"):
#         """Store actual recording bytes"""
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     UPDATE call_history
#                     SET recording_blob_data = %s,
#                         recording_size = %s,
#                         recording_content_type = %s
#                     WHERE call_id = %s;
#                 """, (psycopg2.Binary(recording_data), len(recording_data), content_type, call_id))
#             conn.commit()
#             logging.info(f"✅ Stored {len(recording_data)} bytes for {call_id}")
#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error storing recording: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅

#     # ==================== GET RECORDING FROM DB ====================
#     def get_recording_blob(self, call_id: str, user_id: int = None):
#         """
#         Retrieve recording bytes from database.
#         Returns: (bytes, content_type, size) or (None, None, None)
        
#         Args:
#             call_id: Call identifier
#             user_id: User ID (optional, skip check if None for verification)
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 if user_id is not None:
#                     # Normal query with user_id check
#                     cursor.execute("""
#                         SELECT recording_blob_data, recording_content_type, recording_size
#                         FROM call_history
#                         WHERE call_id = %s AND user_id = %s;
#                     """, (call_id, user_id))
#                 else:
#                     # Verification query without user_id check
#                     cursor.execute("""
#                         SELECT recording_blob_data, recording_content_type, recording_size
#                         FROM call_history
#                         WHERE call_id = %s;
#                     """, (call_id,))
                
#                 row = cursor.fetchone()
#                 if row and row[0]:
#                     return row[0], row[1], row[2]  # (bytes, content_type, size)
#                 return None, None, None
#         except Exception as e:
#             logging.error(f"❌ Error retrieving recording blob: {e}")
#             return None, None, None
#         finally:
#             self.release_connection(conn)  

#     # ==================== MODIFIED: REGISTER USER (AUTO-CREATE PROMPT) ====================
#     def register_user(self, user_data):
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 # Check if email already exists
#                 cursor.execute("SELECT id FROM users WHERE email = %s", (user_data['email'],))
#                 if cursor.fetchone():
#                     raise ValueError("Email already registered.")

#                 # Hash the password
#                 hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())

#                 # Insert user
#                 cursor.execute("""
#                     INSERT INTO users (username, email, password_hash, is_admin)
#                     VALUES (%s, %s, %s, %s)
#                     RETURNING id, username, email, created_at, is_admin;
#                 """, (
#                     user_data['username'],
#                     user_data['email'],
#                     hashed_password.decode('utf-8'),
#                     user_data.get('is_admin', False)
#                 ))

#                 row = cursor.fetchone()
#                 user_id = row["id"]
#                 conn.commit()

#                 # ✅ AUTO-CREATE DEFAULT PROMPT FOR NEW USER
#                 self.create_user_prompt_on_register(user_id)

#                 return {
#                     "id": row["id"],
#                     "username": row["username"],
#                     "email": row["email"],
#                     "created_at": row["created_at"]
#                 }

#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error in register_user: {e}")
#             raise
#         finally:
#             self.release_connection(conn)

#     def create_users_table(self):
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     CREATE TABLE IF NOT EXISTS users (
#                         id SERIAL PRIMARY KEY,
#                         username VARCHAR(100),
#                         email VARCHAR(100) UNIQUE NOT NULL,
#                         password_hash TEXT NOT NULL,
#                         first_name VARCHAR(100),
#                         last_name VARCHAR(100),
#                         is_admin BOOLEAN DEFAULT FALSE,
#                         created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
#                         updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
#                     );
#                 """)
#             conn.commit()
#         except Exception as e:
#             logging.error(f"Error creating users table: {e}")
#         finally:
#             self.release_connection(conn)

    
#     def create_call_history_table(self):
#         """
#         Create call_history table to store call details
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     CREATE TABLE IF NOT EXISTS call_history (
#                         id SERIAL PRIMARY KEY,
#                         user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
#                         call_id TEXT NOT NULL UNIQUE,
#                         status TEXT,
#                         duration DOUBLE PRECISION,  
#                         transcript JSONB,
#                         summary TEXT,
#                         recording_url TEXT,
#                         created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
#                         started_at TIMESTAMPTZ NULL,
#                         ended_at TIMESTAMPTZ NULL,
#                         voice_id TEXT,
#                         voice_name TEXT,
#                         from_number TEXT NULL,
#                         to_number TEXT NULL,
#                         transcript_url TEXT,        -- ADDED
#                         transcript_blob TEXT,       -- ADDED
#                         recording_blob TEXT,        -- ADDED
#                         events_log JSONB DEFAULT '[]',    -- ADDED: For webhooks
#                         agent_events JSONB DEFAULT '[]'   -- ADDED: For agent reports
#                     );
#                 """)
#                 # Add indexes if missing (idempotent)
#                 cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_events_log ON call_history USING GIN (events_log);")
#                 cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_agent_events ON call_history USING GIN (agent_events);")
#             conn.commit()
#         except Exception as e:
#             logging.error(f"Error creating call_history table: {e}")
#         finally:
#             self.release_connection(conn)


#     # ============================= USERS LOGIC START =============================


#     def register_user(self, user_data):
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 # Check if email already exists
#                 cursor.execute("SELECT id FROM users WHERE email = %s", (user_data['email'],))
#                 if cursor.fetchone():
#                     raise ValueError("Email already registered.")

#                 # Hash the password
#                 hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())

#                 # Insert user
#                 cursor.execute("""
#                     INSERT INTO users (username, email, password_hash, is_admin)
#                     VALUES (%s, %s, %s, %s)
#                     RETURNING id, username, email, created_at, is_admin;
#                 """, (
#                     user_data['username'],
#                     user_data['email'],
#                     hashed_password.decode('utf-8'),
#                     user_data.get('is_admin', False)
#                 ))

#                 row = cursor.fetchone()
#                 user_id = row["id"]
#                 conn.commit()

#                 # ✅ AUTO-CREATE DEFAULT PROMPT FOR NEW USER
#                 self.create_default_user_prompt(user_id)
#                 logging.info(f"✅ Created default prompt for new user {user_id}")

#                 return {
#                     "id": row["id"],
#                     "username": row["username"],
#                     "email": row["email"],
#                     "created_at": row["created_at"]
#                 }

#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error in register_user: {e}")
#             raise
#         finally:
#             self.release_connection(conn)


#     def login_user(self, user_data):
#         """Verify user credentials by username or email and return user info."""
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT id, username, email, password_hash,first_name,last_name,created_at,is_admin
#                     FROM users
#                     WHERE username = %s OR email = %s
#                     LIMIT 1
#                 """, (user_data.get("username"), user_data['email']))

#                 result = cursor.fetchone()

#                 if result and bcrypt.checkpw(user_data['password'].encode('utf-8'), result[3].encode('utf-8')):
#                     return {
#                         "id": result[0],
#                         "username": result[1],
#                         "email": result[2],
#                         # "first_name": result[4],
#                         # "last_name": result[5],
#                         "created_at": result[6],
#                         "is_admin": result[7]
#                     }
#                 else:
#                     raise ValueError("Invalid username or password.")
#         except Exception as e:
#             logging.error(f"Error during login: {str(e)}")
#             raise
#         finally:
#             self.release_connection(conn)


#     def get_user_by_id(self, user_id: int):
#         """Get user by ID"""
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute(
#                     "SELECT id,first_name,last_name,username,email,is_admin,created_at FROM users WHERE id = %s",
#                     (user_id,)
#                 )
#                 return cursor.fetchone()
#         finally:
#             if conn:
#                 self.release_connection(conn)

#     def delete_user_by_id(self,user_id):
#         """
#         delete user by id
#         """
#         conn = None 
#         try:
#             conn = self.get_connection()
#             with conn.cursor() as cursor:
#                 cursor.execute(
#                     """
#                     DELETE FROM users WHERE id = %s
#                     """,
#                     (user_id,)     
#                 )
#             conn.commit()
#             return True
#         except Exception as e:
#             logging.error(f"Error deleting user {user_id}: {e}")
#             if conn:
#                 conn.rollback()
#                 return False
#         finally:
#             if conn:
#                 self.release_connection(conn)

#     def update_user_name_fields(self, user_id: int, first_name: str, last_name: str):
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     UPDATE users
#                     SET first_name = %s,
#                         last_name = %s,
#                         updated_at = CURRENT_TIMESTAMP
#                     WHERE id = %s
#                 """, (first_name, last_name, user_id))
#             conn.commit()
#             return True
#         except Exception as e:
#             logging.error(f"Error updating name fields: {e}")
#             return False
#         finally:
#             self.release_connection(conn)

#     def change_user_password(self, user_id: int, current_password: str, new_password: str):
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT password_hash FROM users WHERE id = %s
#                 """, (user_id,))
#                 result = cursor.fetchone()
#                 if not result:
#                     raise ValueError("User not found.")

#                 # Verify current password
#                 if not bcrypt.checkpw(current_password.encode(), result[0].encode()):
#                     raise ValueError("Current password is incorrect.")

#                 # Hash new password
#                 new_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

#                 # Update
#                 cursor.execute("""
#                     UPDATE users SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
#                     WHERE id = %s
#                 """, (new_hash, user_id))
#             conn.commit()
#             return True
#         except Exception as e:
#             logging.error(f"Password change error: {e}")
#             raise
#         finally:
#             self.release_connection(conn)

#     def get_all_users(self):
#         query = """
#             SELECT id, first_name, last_name, username, email, is_admin, created_at
#             FROM users
#             WHERE is_admin = FALSE
#             ORDER BY created_at DESC
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(query)
#                 result = cursor.fetchall()
#                 return [
#                     {
#                         "id": row[0],
#                         # "first_name": row[1],
#                         # "last_name": row[2],
#                         "username": row[3],
#                         "email": row[4],
#                         "is_admin": row[5],
#                         "created_at": row[6],
#                     }
#                     for row in result
#                 ]
#         finally:
#             self.release_connection(conn)

#     def get_all_users_paginated(self, page: int = 1, page_size: int = 10):
#         query_total = "SELECT COUNT(*) FROM users WHERE is_admin = FALSE"
#         query_data = """
#             SELECT id, first_name, last_name, username, email, is_admin, created_at
#             FROM users
#             -- WHERE is_admin = FALSE
#             ORDER BY created_at DESC
#             LIMIT %s OFFSET %s
#         """

#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 # Total user count
#                 cursor.execute(query_total)
#                 total_users = cursor.fetchone()[0]

#                 # Paginated data
#                 offset = (page - 1) * page_size
#                 cursor.execute(query_data, (page_size, offset))
#                 rows = cursor.fetchall()

#                 users = [
#                     {
#                         "id": row[0],
#                         # "first_name": row[1],
#                         # "last_name": row[2],
#                         "username": row[3],
#                         "email": row[4],
#                         "is_admin": row[5],
#                         "created_at": row[6],
#                     }
#                     for row in rows
#                 ]

#             return {
#                 "users": users,
#                 "total": total_users
#             }

#         except Exception as e:
#             print(f"Error fetching paginated users: {e}")
#             return {"users": [], "total": 0}
#         finally:
#             self.release_connection(conn)             

#     def insert_call_history(
#         self,
#         user_id: int,
#         call_id: str,
#         status: str = None,
#         voice_id: str = None,
#         voice_name: str = None,
#         to_number: str = None
#     ):
#         """
#         Insert a new call history record with initial data.
#         Other fields (transcript, summary, duration, etc.) will be updated later.
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 values = (
#                     user_id, call_id, status,
#                     voice_id, voice_name, to_number
#                 )

#                 cursor.execute("""
#                     INSERT INTO call_history (
#                         user_id, call_id, status,
#                         voice_id, voice_name, to_number
#                     )
#                     VALUES (%s,%s,%s,%s,%s,%s)
#                     RETURNING id;
#                 """, values)

#                 row = cursor.fetchone()
#                 conn.commit()
#                 return row[0] if row else None

#         except Exception as e:
#             logging.error(f"Error inserting call history: {e}")
#             conn.rollback()
#             raise
#         finally:
#             self.release_connection(conn)



#     def update_call_history(self, call_id: str, updates: dict):
#         """
#         Update specific fields in the call_history record based on the call_id.

#         Args:
#             call_id (str): The unique identifier for the call.
#             updates (dict): A dictionary where keys are column names and values
#                             are the new values to set. e.g., {"status": "completed", "duration": 120.5}
#         """
#         if not updates:
#             logging.warning("update_call_history called with no updates.")
#             return None # Or raise an error

#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 # Build the SET part of the SQL query dynamically
#                 set_clauses = []
#                 param_values = []
#                 for key, value in updates.items():
                    
#                     # ==========================================================
#                     # ⭐️ BUG FIX IS HERE ⭐️
#                     # The old logic ('if not key.isalnum() and key != '_'') was
#                     # rejecting valid keys like 'transcript_url'.
#                     # This new logic allows keys with underscores.
#                     # ==========================================================
#                     if not key.replace('_', '').isalnum():
#                         logging.error(f"Invalid column name detected: {key}")
#                         raise ValueError(f"Invalid column name: {key}")

#                     # Handle JSON data specifically
#                     if key == 'transcript' and value is not None:
#                         set_clauses.append(f"{key} = %s")
#                         param_values.append(json.dumps(value))
#                     else:
#                         set_clauses.append(f"{key} = %s")
#                         param_values.append(value)

#                 if not set_clauses:
#                     logging.warning("No valid fields to update.")
#                     return None

#                 set_sql = ", ".join(set_clauses)
#                 sql = f"UPDATE call_history SET {set_sql} WHERE call_id = %s RETURNING id;"
                
#                 # Add call_id to the parameters list
#                 param_values.append(call_id)

#                 logging.debug(f"Executing SQL: {sql} with params: {param_values}") # Optional: Log SQL for debugging

#                 cursor.execute(sql, tuple(param_values))

#                 row = cursor.fetchone()
#                 conn.commit()
#                 logging.info(f"Updated call_history for call_id {call_id}. Updated fields: {list(updates.keys())}")
#                 return row[0] if row else None

#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error updating call history for call_id={call_id}: {e}")
#             traceback.print_exc() # Print full traceback
#             raise # Re-raise the exception
#         finally:
#             self.release_connection(conn)

#     # def get_call_history_by_user_id(self, user_id: int, page: int = 1, page_size: int = 10):
#     #     """
#     #     Fetch paginated call history for a user (with JOIN on users table).
#     #     Includes call details, voice info, caller/callee numbers, and timestamps.
#     #     """
#     #     conn = self.get_connection()
#     #     try:
#     #         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#     #             # Count total records
#     #             cursor.execute("SELECT COUNT(*) FROM call_history WHERE user_id = %s", (user_id,))
#     #             total = cursor.fetchone()["count"]

#     #             # Paginated query
#     #             offset = (page - 1) * page_size
#     #             cursor.execute("""
#     #                 SELECT ch.id, ch.call_id, ch.status, ch.duration, ch.transcript,
#     #                     ch.summary, ch.recording_url, ch.created_at, ch.started_at, ch.ended_at,
#     #                     ch.voice_id, ch.voice_name, ch.from_number, ch.to_number,
#     #                     u.id AS user_id, u.username, u.email
#     #                 FROM call_history ch
#     #                 JOIN users u ON ch.user_id = u.id
#     #                 WHERE ch.user_id = %s
#     #                 ORDER BY ch.created_at DESC
#     #                 LIMIT %s OFFSET %s
#     #             """, (user_id, page_size, offset))
                
#     #             rows = cursor.fetchall()

#     #             # Ensure transcript is JSON
#     #             for row in rows:
#     #                 if isinstance(row["transcript"], str):
#     #                     try:
#     #                         row["transcript"] = json.loads(row["transcript"])
#     #                     except Exception:
#     #                         logging.warning(f"Invalid JSON in transcript for call_id={row['call_id']}")

#     #             return {"calls": rows, "total": total, "page": page, "page_size": page_size}
#     #     except Exception as e:
#     #         logging.error(f"Error fetching call history for user_id={user_id}: {e}")
#     #         raise
#     #     finally:
#     #         conn.close()

#     def get_call_history_by_user_id(self, user_id: int, page: int = 1, page_size: int = 10):
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 # Count total records
#                 cursor.execute("SELECT COUNT(*) FROM call_history WHERE user_id = %s", (user_id,))
#                 total = cursor.fetchone()["count"]

#                 # Count completed
#                 cursor.execute("""
#                     SELECT COUNT(*) FROM call_history 
#                     WHERE user_id = %s AND status = 'completed'
#                 """, (user_id,))
#                 completed_calls = cursor.fetchone()["count"]

#                 not_completed_calls = total - completed_calls

#                 # Paginated query
#                 offset = (page - 1) * page_size
#                 cursor.execute("""
#                     SELECT ch.id, ch.call_id, ch.status, ch.duration, ch.transcript,
#                         ch.summary, ch.recording_url, ch.created_at, ch.started_at, ch.ended_at,
#                         ch.voice_id, ch.voice_name, ch.from_number, ch.to_number,
#                         u.id AS user_id, u.username, u.email
#                     FROM call_history ch
#                     JOIN users u ON ch.user_id = u.id
#                     WHERE ch.user_id = %s
#                     ORDER BY ch.created_at DESC
#                     LIMIT %s OFFSET %s
#                 """, (user_id, page_size, offset))

#                 rows = cursor.fetchall()

#                 # Ensure transcript is JSON
#                 for row in rows:
#                     if isinstance(row["transcript"], str):
#                         try:
#                             row["transcript"] = json.loads(row["transcript"])
#                         except Exception:
#                             logging.warning(f"Invalid JSON in transcript for call_id={row['call_id']}")

#                 return {
#                     "calls": rows,
#                     "total": total,
#                     "completed_calls": completed_calls,
#                     "not_completed_calls": not_completed_calls,
#                     "page": page,
#                     "page_size": page_size
#                 }
#         except Exception as e:
#             logging.error(f"Error fetching call history for user_id={user_id}: {e}")
#             raise
#         finally:
#             self.release_connection(conn)


#     def create_appointment(
#         self,
#         user_id: int,
#         appointment_date: str,
#         start_time: str,
#         end_time: str,
#         attendee_email: str,
#         attendee_name: str,
#         title: str,
#         description: str = "",
#         notes: str = "" 
#     ):
#         """Create a new appointment"""
#         query = """
#             INSERT INTO appointments 
#             (user_id, appointment_date, start_time, end_time, attendee_email, attendee_name, title, description, notes)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#             RETURNING id
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(query, (
#                     user_id, appointment_date, start_time, end_time,
#                     attendee_email, attendee_name, title, description, notes
#                 ))
#                 appointment_id = cursor.fetchone()[0]
#                 conn.commit()
#                 return appointment_id
#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error creating appointment: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅ FIXED
            
                
#     def create_appointments_table(self):
#         """
#         Create appointments table to store meeting scheduling data
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute("""
#                     CREATE TABLE IF NOT EXISTS appointments (
#                         id SERIAL PRIMARY KEY,
#                         user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
#                         appointment_date DATE NOT NULL,
#                         start_time TIME NOT NULL,
#                         end_time TIME NOT NULL,
#                         attendee_email VARCHAR(255) NOT NULL,
#                         attendee_name VARCHAR(255),
#                         title TEXT NOT NULL,
#                         description TEXT,
#                         notes TEXT,  -- ✅ NEW FIELD
#                         status VARCHAR(50) DEFAULT 'scheduled',
#                         created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
#                     );
#                 """)
#             conn.commit()
#         except Exception as e:
#             logging.error(f"Error creating appointments table: {e}")
#         finally:
#             self.release_connection(conn)


#     def get_user_appointments(self, user_id: int, from_date: str = None):
#         """Get all appointments for a user from a specific date onwards"""
#         if from_date is None:
#             from_date = datetime.now().strftime("%Y-%m-%d")
        
#         query = """
#             SELECT id, appointment_date, start_time, end_time, attendee_email, 
#                 attendee_name, title, description, status, created_at
#             FROM appointments
#             WHERE user_id = %s AND appointment_date >= %s
#             ORDER BY appointment_date, start_time
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute(query, (user_id, from_date))
#                 return cursor.fetchall()
#         except Exception as e:
#             logging.error(f"Error getting appointments: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅ FIXED

#     def check_appointment_conflict(
#         self,
#         user_id: int,
#         appointment_date: str,
#         start_time: str,
#         end_time: str
#     ) -> bool:
#         """Check if there's a conflicting appointment"""
#         query = """
#             SELECT COUNT(*) as conflict_count
#             FROM appointments
#             WHERE user_id = %s 
#             AND appointment_date = %s
#             AND status = 'scheduled'
#             AND (
#                 (start_time <= %s AND end_time > %s) OR
#                 (start_time < %s AND end_time >= %s) OR
#                 (start_time >= %s AND end_time <= %s)
#             )
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(query, (
#                     user_id, appointment_date,
#                     start_time, start_time,
#                     end_time, end_time,
#                     start_time, end_time
#                 ))
#                 result = cursor.fetchone()
#                 return result[0] > 0
#         except Exception as e:
#             logging.error(f"Error checking conflict: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅ FIXED

#     def get_available_slots(
#         self,
#         user_id: int,
#         appointment_date: str,
#         business_hours_start: str = "08:00",
#         business_hours_end: str = "18:00",
#         slot_duration_minutes: int = 60
#     ):
#         """Get available time slots for a given date"""
#         query = """
#             SELECT start_time, end_time
#             FROM appointments
#             WHERE user_id = %s AND appointment_date = %s AND status = 'scheduled'
#             ORDER BY start_time
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(query, (user_id, appointment_date))
#                 booked_slots = cursor.fetchall()
            
#             return {
#                 "date": appointment_date,
#                 "booked_slots": [{"start": slot[0], "end": slot[1]} for slot in booked_slots]
#             }
#         except Exception as e:
#             logging.error(f"Error getting available slots: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅ FIXED

#     def get_call_by_id(self, call_id: str, user_id: int):
#         """Get a specific call by ID for a user"""
#         query = """
#             SELECT id, call_id, status, duration, transcript, recording_url, 
#                 transcript_url, transcript_blob, recording_blob,
#                 created_at, started_at, ended_at, 
#                 from_number, to_number, voice_name
#             FROM call_history
#             WHERE call_id = %s AND user_id = %s
#         """
#         conn = self.get_connection()
#         try:
#             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 cursor.execute(query, (call_id, user_id))
#                 result = cursor.fetchone()
                
#                 if result and isinstance(result.get("transcript"), str):
#                     try:
#                         result["transcript"] = json.loads(result["transcript"])
#                     except:
#                         pass
                
#                 return result
#         except Exception as e:
#             logging.error(f"Error getting call by ID: {e}")
#             raise
#         finally:
#             self.release_connection(conn)  # ✅ FIXED



#     def add_call_event(self, call_id: str, event_type: str, event_data: dict = None):
#         """Add a unique event entry into call_history.events_log"""
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 # Fetch existing events
#                 cursor.execute("SELECT events_log FROM call_history WHERE call_id = %s", (call_id,))
#                 row = cursor.fetchone()
#                 if not row:
#                     logging.warning(f"Call {call_id} not found for event {event_type}")
#                     return
                
#                 events_log = row[0] or []
#                 if isinstance(events_log, str):
#                     try:
#                         events_log = json.loads(events_log)
#                     except Exception:
#                         events_log = []

#                 # Check for duplicate event
#                 if any(ev.get("event") == event_type for ev in events_log):
#                     logging.info(f"Duplicate event {event_type} ignored for {call_id}")
#                     return

#                 # Append event
#                 events_log.append({
#                     "event": event_type,
#                     "timestamp": datetime.utcnow().isoformat(),
#                     "data": event_data or {}
#                 })

#                 # Update DB
#                 cursor.execute(
#                     "UPDATE call_history SET events_log = %s WHERE call_id = %s",
#                     (json.dumps(events_log), call_id)
#                 )

#             conn.commit()
#             logging.info(f"Event '{event_type}' added to call {call_id}")

#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error adding call event: {e}")
#         finally:
#             self.release_connection(conn)

#     def add_agent_event(self, call_id: str, event_type: str, event_data: dict = None, timestamp: str = None):
#         """Add a unique agent event entry into call_history.agent_events"""
#         if timestamp is None:
#             timestamp = datetime.now(timezone.utc).isoformat()
        
#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 # Fetch existing events
#                 cursor.execute("SELECT agent_events FROM call_history WHERE call_id = %s", (call_id,))
#                 row = cursor.fetchone()
#                 if not row:
#                     logging.warning(f"Call {call_id} not found for agent event {event_type}")
#                     return
                
#                 events_log = row[0] or []
#                 if isinstance(events_log, str):
#                     try:
#                         events_log = json.loads(events_log)
#                     except Exception:
#                         events_log = []

#                 # Check for duplicate (within 5s timestamp tolerance)
#                 now = datetime.now(timezone.utc)
#                 for ev in events_log:
#                     if (ev.get("event_type") == event_type and 
#                         abs((now - datetime.fromisoformat(ev.get("timestamp").replace("Z", "+00:00"))).total_seconds()) < 5):
#                         logging.info(f"Duplicate agent event {event_type} ignored for {call_id}")
#                         return

#                 # Append event
#                 events_log.append({
#                     "event_type": event_type,
#                     "event_data": event_data or {},
#                     "timestamp": timestamp,
#                     "received_at": datetime.now(timezone.utc).isoformat()
#                 })

#                 # Update DB
#                 cursor.execute(
#                     "UPDATE call_history SET agent_events = %s WHERE call_id = %s",
#                     (json.dumps(events_log), call_id)
#                 )

#             conn.commit()
#             logging.info(f"Agent event '{event_type}' added to call {call_id}")

#         except Exception as e:
#             conn.rollback()
#             logging.error(f"Error adding agent event: {e}")
#             traceback.print_exc()
#             raise
#         finally:
#             self.release_connection(conn)











































