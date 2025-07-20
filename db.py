import asyncpg
import os


class DatabaseManager:
    def __init__(self):
        self.pool = None

    async def init_pool(self):
        """Initialize connection pool"""
        db_url = os.getenv('DB_URL')
        if db_url:
            # Use DB_URL if provided
            self.pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=10
            )
        else:
            # Fall back to individual connection parameters
            self.pool = await asyncpg.create_pool(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                min_size=1,
                max_size=10
            )
        await self.create_tables()

    async def create_tables(self):
        """Create necessary tables if they don't exist"""
        async with self.pool.acquire() as conn:
            # Servers table with latest status message tracking
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS servers (
                    server_id BIGINT PRIMARY KEY,
                    name TEXT,
                    main_channel_id BIGINT,
                    table_message_id BIGINT,
                    latest_status_message_id BIGINT,
                    is_main_server BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Leagues table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS leagues (
                    league_id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    display_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Server-League associations with week tracking
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS server_leagues (
                    server_id BIGINT REFERENCES servers(server_id) ON DELETE CASCADE,
                    league_id INT REFERENCES leagues(league_id) ON DELETE CASCADE,
                    current_week INT DEFAULT 1,
                    PRIMARY KEY (server_id, league_id)
                )
            ''')

            # Users table - REMOVED server_id constraint to allow global users
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    discord_id BIGINT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # User-League associations (which leagues a user participates in globally)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_leagues (
                    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                    league_id INT REFERENCES leagues(league_id) ON DELETE CASCADE,
                    ready_status TEXT DEFAULT '',
                    PRIMARY KEY (user_id, league_id)
                )
            ''')

            # New table to track which servers users are active on
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_servers (
                    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
                    server_id BIGINT REFERENCES servers(server_id) ON DELETE CASCADE,
                    PRIMARY KEY (user_id, server_id)
                )
            ''')

    async def migrate_existing_data(self):
        """Migrate existing data to new schema - run this once"""
        async with self.pool.acquire() as conn:
            # Check if migration is needed
            old_users = await conn.fetch("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'server_id'
            """)
            
            if old_users:
                print("Migrating existing data...")
                
                try:
                    # Get all existing users with server_id
                    existing_users = await conn.fetch("SELECT user_id, username, server_id FROM users")
                    
                    # Step 1: Create backup of user-league relationships
                    user_league_backup = await conn.fetch("""
                        SELECT ul.user_id, ul.league_id, ul.ready_status, u.username, u.server_id
                        FROM user_leagues ul
                        JOIN users u ON ul.user_id = u.user_id
                    """)
                    
                    # Step 2: Drop foreign key constraints temporarily
                    await conn.execute("ALTER TABLE user_leagues DROP CONSTRAINT IF EXISTS user_leagues_user_id_fkey")
                    await conn.execute("ALTER TABLE user_servers DROP CONSTRAINT IF EXISTS user_servers_user_id_fkey")
                    
                    # Step 3: Clear existing data
                    await conn.execute("DELETE FROM user_leagues")
                    await conn.execute("DELETE FROM user_servers") 
                    await conn.execute("DELETE FROM users")
                    
                    # Step 4: Recreate users table with new structure
                    await conn.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_username_server_id_key")
                    await conn.execute("ALTER TABLE users DROP COLUMN IF EXISTS server_id")
                    await conn.execute("ALTER TABLE users ADD CONSTRAINT users_username_key UNIQUE (username)")
                    
                    # Step 5: Re-add foreign key constraints
                    await conn.execute("""
                        ALTER TABLE user_leagues 
                        ADD CONSTRAINT user_leagues_user_id_fkey 
                        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                    """)
                    await conn.execute("""
                        ALTER TABLE user_servers 
                        ADD CONSTRAINT user_servers_user_id_fkey 
                        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                    """)
                    
                    # Step 6: Re-insert users (deduplicated by username)
                    username_to_user_id = {}
                    for user in existing_users:
                        username = user['username']
                        if username not in username_to_user_id:
                            user_id = await conn.fetchval("""
                                INSERT INTO users (username) 
                                VALUES ($1) 
                                RETURNING user_id
                            """, username)
                            username_to_user_id[username] = user_id
                        
                        # Track which server they were on
                        await conn.execute("""
                            INSERT INTO user_servers (user_id, server_id) 
                            VALUES ($1, $2) 
                            ON CONFLICT DO NOTHING
                        """, username_to_user_id[username], user['server_id'])
                    
                    # Step 7: Restore user-league relationships
                    for ul in user_league_backup:
                        username = ul['username']
                        new_user_id = username_to_user_id[username]
                        
                        await conn.execute("""
                            INSERT INTO user_leagues (user_id, league_id, ready_status)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (user_id, league_id) DO UPDATE SET ready_status = $3
                        """, new_user_id, ul['league_id'], ul['ready_status'])
                    
                    print("Migration completed successfully!")
                    
                except Exception as e:
                    print(f"Migration failed: {e}")
                    raise
            else:
                print("Migration not needed - database already uses new schema")

    async def get_server_leagues(self, server_id, show_all_servers=False):
        """Get leagues for a server or all leagues if main server"""
        async with self.pool.acquire() as conn:
            if show_all_servers:
                # Main server shows all leagues that exist
                query = """
                    SELECT DISTINCT l.league_id, l.name, l.display_name, 1 as current_week
                    FROM leagues l
                    ORDER BY l.display_name
                """
                return await conn.fetch(query)
            else:
                # Individual server shows only its assigned leagues
                query = """
                    SELECT l.league_id, l.name, l.display_name, sl.current_week
                    FROM leagues l
                    JOIN server_leagues sl ON l.league_id = sl.league_id
                    WHERE sl.server_id = $1
                    ORDER BY l.display_name
                """
                return await conn.fetch(query, server_id)

    async def get_server_users(self, server_id, show_all_servers=False):
        """Get users for a server or all users if main server"""
        async with self.pool.acquire() as conn:
            if show_all_servers:
                # Main server shows all users from all servers
                query = """
                    SELECT DISTINCT u.username, us.server_id
                    FROM users u
                    JOIN user_servers us ON u.user_id = us.user_id
                    ORDER BY u.username
                """
                return await conn.fetch(query)
            else:
                # Individual server shows only users active on this server
                query = """
                    SELECT u.username, us.server_id
                    FROM users u
                    JOIN user_servers us ON u.user_id = us.user_id
                    WHERE us.server_id = $1
                    ORDER BY u.username
                """
                return await conn.fetch(query, server_id)

    async def get_user_status(self, username, league_id):
        """Get ready status for a user in a specific league (global)"""
        async with self.pool.acquire() as conn:
            query = """
                SELECT ul.ready_status
                FROM users u
                JOIN user_leagues ul ON u.user_id = ul.user_id
                WHERE u.username = $1 AND ul.league_id = $2
            """
            result = await conn.fetchval(query, username, league_id)
            return result  # Return None if user not in league, empty string if not ready, or actual status

    async def needs_migration(self):
        """Check if database needs migration"""
        async with self.pool.acquire() as conn:
            old_users = await conn.fetch("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'server_id'
            """)
            return len(old_users) > 0

    async def add_user_to_server(self, username, server_id, league_names):
        """Add a user globally and assign them to leagues"""
        async with self.pool.acquire() as conn:
            # Check if we need to migrate first
            if await self.needs_migration():
                raise Exception("Database needs migration! Run /migrate first.")
            
            # Insert user globally
            user_id = await conn.fetchval("""
                INSERT INTO users (username)
                VALUES ($1)
                ON CONFLICT (username) DO UPDATE SET username = $1
                RETURNING user_id
            """, username)
            
            # Track that user is active on this server
            await conn.execute("""
                INSERT INTO user_servers (user_id, server_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            """, user_id, server_id)
            
            # Assign user to leagues (globally, not per-server)
            valid_leagues = []
            invalid_leagues = []
            
            for league_name in league_names:
                # Check if league exists (don't require server assignment)
                league_id = await conn.fetchval("""
                    SELECT league_id FROM leagues WHERE name = $1
                """, league_name)
                
                if league_id:
                    # Add user to league globally
                    await conn.execute("""
                        INSERT INTO user_leagues (user_id, league_id, ready_status)
                        VALUES ($1, $2, '')
                        ON CONFLICT (user_id, league_id) DO UPDATE SET ready_status = ''
                    """, user_id, league_id)
                    valid_leagues.append(league_name)
                else:
                    invalid_leagues.append(league_name)
            
            return valid_leagues, invalid_leagues

    async def update_user_status(self, username, league_name, status):
        """Update user's ready status for a league"""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE user_leagues 
                SET ready_status = $3
                WHERE user_id = (SELECT user_id FROM users WHERE username = $1)
                AND league_id = (SELECT league_id FROM leagues WHERE name = $2)
            """, username, league_name, status)
            
            return result == "UPDATE 1"

    async def check_auto_advance(self, server_id):
        """Check if any leagues should auto-advance (all players ready)"""
        async with self.pool.acquire() as conn:
            # Get all leagues assigned to this server
            leagues = await conn.fetch("""
                SELECT l.league_id, l.name, l.display_name
                FROM leagues l
                JOIN server_leagues sl ON l.league_id = sl.league_id
                WHERE sl.server_id = $1
            """, server_id)
            
            advanced_leagues = []
            
            for league in leagues:
                # Count total users in this league that are active on this server
                total_users = await conn.fetchval("""
                    SELECT COUNT(DISTINCT u.user_id)
                    FROM users u
                    JOIN user_servers us ON u.user_id = us.user_id
                    JOIN user_leagues ul ON u.user_id = ul.user_id
                    WHERE us.server_id = $1 AND ul.league_id = $2
                """, server_id, league['league_id'])
                
                if total_users == 0:
                    continue  # Skip if no users in league on this server
                
                # Count ready users (X status) in this league that are active on this server
                ready_users = await conn.fetchval("""
                    SELECT COUNT(DISTINCT u.user_id)
                    FROM users u
                    JOIN user_servers us ON u.user_id = us.user_id
                    JOIN user_leagues ul ON u.user_id = ul.user_id
                    WHERE us.server_id = $1 AND ul.league_id = $2 AND ul.ready_status = 'X'
                """, server_id, league['league_id'])
                
                # If all users are ready, auto-advance
                if ready_users > 0 and ready_users == total_users:
                    # Clear all ready statuses for users in this league
                    await conn.execute("""
                        UPDATE user_leagues 
                        SET ready_status = ''
                        WHERE league_id = $1
                    """, league['league_id'])
                    
                    # Increment week for this league on this server
                    await conn.execute("""
                        UPDATE server_leagues 
                        SET current_week = current_week + 1
                        WHERE server_id = $1 AND league_id = $2
                    """, server_id, league['league_id'])
                    
                    advanced_leagues.append(league['display_name'])
            
            return advanced_leagues

    async def advance_league(self, server_id, league_name):
        """Manually advance a league"""
        async with self.pool.acquire() as conn:
            # Get league info
            league_info = await conn.fetchrow(
                "SELECT league_id, display_name FROM leagues WHERE name = $1",
                league_name.lower()
            )
            
            if not league_info:
                return None
            
            # Clear the league for all users globally
            await conn.execute("""
                UPDATE user_leagues 
                SET ready_status = ''
                WHERE league_id = $1
            """, league_info['league_id'])
            
            # Increment the week for this league on this server
            await conn.execute("""
                UPDATE server_leagues 
                SET current_week = current_week + 1
                WHERE server_id = $1 AND league_id = $2
            """, server_id, league_info['league_id'])
            
            # Get the new week number
            new_week = await conn.fetchval("""
                SELECT current_week FROM server_leagues 
                WHERE server_id = $1 AND league_id = $2
            """, server_id, league_info['league_id'])
            
    async def remove_user_from_server(self, username, server_id):
        """Remove a user from a specific server (but keep them in other servers and leagues)"""
        async with self.pool.acquire() as conn:
            # Check if user exists
            user_id = await conn.fetchval(
                "SELECT user_id FROM users WHERE username = $1", username
            )
            
            if not user_id:
                return False
            
            # Remove from this server
            result = await conn.execute(
                "DELETE FROM user_servers WHERE user_id = $1 AND server_id = $2",
                user_id, server_id
            )
            
            return result == "DELETE 1"

    async def remove_user_from_leagues(self, username, league_names):
        """Remove a user from specific leagues globally"""
        async with self.pool.acquire() as conn:
            # Get user ID
            user_id = await conn.fetchval(
                "SELECT user_id FROM users WHERE username = $1", username
            )
            
            if not user_id:
                return []
            
            removed_leagues = []
            for league_name in league_names:
                # Remove from league
                result = await conn.execute("""
                    DELETE FROM user_leagues 
                    WHERE user_id = $1 AND league_id = (
                        SELECT league_id FROM leagues WHERE name = $2
                    )
                """, user_id, league_name)
                
                if result == "DELETE 1":
                    removed_leagues.append(league_name)
            
            return removed_leagues

    async def delete_user_completely(self, username):
        """Completely delete a user from all servers and leagues"""
        async with self.pool.acquire() as conn:
            # Get user ID
            user_id = await conn.fetchval(
                "SELECT user_id FROM users WHERE username = $1", username
            )
            
            if not user_id:
                return False
            
            # Delete user (cascades to user_leagues and user_servers)
            result = await conn.execute(
                "DELETE FROM users WHERE user_id = $1", user_id
            )
            
            return result == "DELETE 1"

    async def get_user_leagues(self, username):
        """Get all leagues a user is in"""
        async with self.pool.acquire() as conn:
            leagues = await conn.fetch("""
                SELECT l.name, l.display_name, ul.ready_status
                FROM users u
                JOIN user_leagues ul ON u.user_id = ul.user_id
                JOIN leagues l ON ul.league_id = l.league_id
                WHERE u.username = $1
                ORDER BY l.display_name
            """, username)
            
            return leagues

    async def get_user_servers(self, username):
        """Get all servers a user is active on"""
        async with self.pool.acquire() as conn:
            servers = await conn.fetch("""
                SELECT s.server_id, s.name
                FROM users u
                JOIN user_servers us ON u.user_id = us.user_id
                JOIN servers s ON us.server_id = s.server_id
                WHERE u.username = $1
                ORDER BY s.name
            """, username)
            
            return servers

    async def advance_league(self, server_id, league_name):
        """Manually advance a league"""
        async with self.pool.acquire() as conn:
            # Get league info
            league_info = await conn.fetchrow(
                "SELECT league_id, display_name FROM leagues WHERE name = $1",
                league_name.lower()
            )
            
            if not league_info:
                return None
            
            # Clear the league for all users globally
            await conn.execute("""
                UPDATE user_leagues 
                SET ready_status = ''
                WHERE league_id = $1
            """, league_info['league_id'])
            
            # Increment the week for this league on this server
            await conn.execute("""
                UPDATE server_leagues 
                SET current_week = current_week + 1
                WHERE server_id = $1 AND league_id = $2
            """, server_id, league_info['league_id'])
            
            # Get the new week number
            new_week = await conn.fetchval("""
                SELECT current_week FROM server_leagues 
                WHERE server_id = $1 AND league_id = $2
            """, server_id, league_info['league_id'])
            
    async def set_league_week(self, server_id, league_name, week):
        """Set the current week for a league on a server"""
        async with self.pool.acquire() as conn:
            # Check if league exists and is assigned to this server
            league_info = await conn.fetchrow("""
                SELECT l.league_id, l.display_name, sl.current_week
                FROM leagues l
                JOIN server_leagues sl ON l.league_id = sl.league_id
                WHERE l.name = $1 AND sl.server_id = $2
            """, league_name, server_id)

            if not league_info:
                return None

            # Update the week
            await conn.execute("""
                UPDATE server_leagues 
                SET current_week = $3
                WHERE server_id = $1 AND league_id = $2
            """, server_id, league_info['league_id'], week)
            
            return league_info['display_name'], league_info['current_week']

    async def add_existing_user_to_leagues(self, username, league_names):
        """Add an existing user to specific leagues"""
        async with self.pool.acquire() as conn:
            # Check if user exists
            user_id = await conn.fetchval(
                "SELECT user_id FROM users WHERE username = $1", username
            )
            
            if not user_id:
                return None
            
            valid_leagues = []
            invalid_leagues = []
            
            for league_name in league_names:
                # Check if league exists
                league_id = await conn.fetchval(
                    "SELECT league_id FROM leagues WHERE name = $1", league_name
                )
                
                if league_id:
                    # Add user to league globally
                    await conn.execute("""
                        INSERT INTO user_leagues (user_id, league_id, ready_status)
                        VALUES ($1, $2, '')
                        ON CONFLICT (user_id, league_id) DO UPDATE SET ready_status = ''
                    """, user_id, league_id)
                    valid_leagues.append(league_name)
                else:
                    invalid_leagues.append(league_name)
            
            return valid_leagues, invalid_leagues

    async def link_discord_user(self, username, discord_id):
        """Link a Discord user ID to a username"""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE users 
                SET discord_id = $2
                WHERE username = $1
            """, username, discord_id)
            
            return result == "UPDATE 1"

    async def get_user_by_discord_id(self, discord_id):
        """Get username by Discord ID"""
        async with self.pool.acquire() as conn:
            username = await conn.fetchval(
                "SELECT username FROM users WHERE discord_id = $1", discord_id
            )
            return username

            return league_info['display_name'], new_week