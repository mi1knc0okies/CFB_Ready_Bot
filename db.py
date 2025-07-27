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

            # Users table - global users with discord_id and admin flag
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    discord_id BIGINT UNIQUE,
                    is_admin BOOLEAN DEFAULT FALSE,
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

    async def needs_migration(self):
        """Check if database needs migration"""
        async with self.pool.acquire() as conn:
            old_users = await conn.fetch("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'server_id'
            """)
            return len(old_users) > 0

    async def migrate_existing_data(self):
        """Migrate existing data to new schema - run this once"""
        async with self.pool.acquire() as conn:
            print("Starting migration...")
            
            try:
                # Check if is_admin column exists
                admin_col_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'is_admin'
                """)
                
                if not admin_col_exists:
                    print("Adding is_admin column...")
                    await conn.execute("ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT FALSE")
                
                # Check if discord_id has unique constraint
                discord_constraint_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = 'users' AND ccu.column_name = 'discord_id' AND tc.constraint_type = 'UNIQUE'
                """)
                
                if not discord_constraint_exists:
                    print("Adding discord_id unique constraint...")
                    try:
                        await conn.execute("ALTER TABLE users ADD CONSTRAINT users_discord_id_key UNIQUE (discord_id)")
                    except Exception as e:
                        print(f"Note: Could not add discord_id constraint (might already exist): {e}")
                
                # Check if old schema exists (server_id column in users table)
                old_users = await conn.fetch("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'server_id'
                """)
                
                if old_users:
                    print("Migrating from old schema...")
                    
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
                    
                    # Step 3: Clear existing data
                    await conn.execute("DELETE FROM user_leagues")
                    await conn.execute("DELETE FROM users")
                    
                    # Step 4: Recreate users table with new structure
                    await conn.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_username_server_id_key")
                    await conn.execute("ALTER TABLE users DROP COLUMN IF EXISTS server_id")
                    await conn.execute("ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS users_username_key UNIQUE (username)")
                    
                    # Step 5: Re-add foreign key constraints
                    await conn.execute("""
                        ALTER TABLE user_leagues 
                        ADD CONSTRAINT user_leagues_user_id_fkey 
                        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                    """)
                    
                    # Step 6: Re-insert users (deduplicated by username)
                    username_to_user_id = {}
                    for user in existing_users:
                        username = user['username']
                        if username not in username_to_user_id:
                            user_id = await conn.fetchval("""
                                INSERT INTO users (username, is_admin) 
                                VALUES ($1, FALSE) 
                                RETURNING user_id
                            """, username)
                            username_to_user_id[username] = user_id
                    
                    # Step 7: Restore user-league relationships
                    for ul in user_league_backup:
                        username = ul['username']
                        if username in username_to_user_id:
                            new_user_id = username_to_user_id[username]
                            
                            await conn.execute("""
                                INSERT INTO user_leagues (user_id, league_id, ready_status)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (user_id, league_id) DO UPDATE SET ready_status = $3
                            """, new_user_id, ul['league_id'], ul['ready_status'])
                    
                    # Step 8: Drop user_servers table if it exists (no longer needed)
                    await conn.execute("DROP TABLE IF EXISTS user_servers")
                    
                    print("Migration from old schema completed successfully!")
                else:
                    print("Schema is already up to date!")
                
                print("Migration completed successfully!")
                
            except Exception as e:
                print(f"Migration failed: {e}")
                raise

    async def get_server_leagues(self, server_id, show_all_servers=False):
        """Get leagues for a server or all leagues if main server"""
        async with self.pool.acquire() as conn:
            if show_all_servers:
                # Main server shows all leagues that exist across ALL servers
                query = """
                    SELECT DISTINCT l.league_id, l.name, l.display_name, 1 as current_week
                    FROM leagues l
                    WHERE l.league_id IN (
                        SELECT DISTINCT league_id FROM server_leagues
                    )
                    ORDER BY l.display_name
                """
                return await conn.fetch(query)
            else:
                # Individual server shows only its leagues with weeks
                query = """
                    SELECT l.league_id, l.name, l.display_name, sl.current_week
                    FROM leagues l
                    JOIN server_leagues sl ON l.league_id = sl.league_id
                    WHERE sl.server_id = $1
                    ORDER BY l.display_name
                """
                return await conn.fetch(query, server_id)

    async def get_server_users(self, server_id, show_all_servers=False):
        """Get users for a server based on assigned leagues"""
        async with self.pool.acquire() as conn:
            if show_all_servers:
                # Main server shows all users who are in any league
                query = """
                    SELECT DISTINCT u.username
                    FROM users u
                    WHERE u.user_id IN (
                        SELECT DISTINCT user_id FROM user_leagues
                    )
                    ORDER BY u.username
                """
                raw_users = await conn.fetch(query)
                return [{'username': user['username'], 'server_id': None} for user in raw_users]
            else:
                # Individual server shows users who are in leagues assigned to this server
                query = """
                    SELECT DISTINCT u.username, $1::bigint as server_id
                    FROM users u
                    JOIN user_leagues ul ON u.user_id = ul.user_id
                    JOIN server_leagues sl ON ul.league_id = sl.league_id
                    WHERE sl.server_id = $1
                    ORDER BY u.username
                """
                result = await conn.fetch(query, server_id)
                
                # Debug: let's also check what leagues are assigned to this server
                server_leagues = await conn.fetch("""
                    SELECT l.name, l.display_name 
                    FROM leagues l
                    JOIN server_leagues sl ON l.league_id = sl.league_id
                    WHERE sl.server_id = $1
                """, server_id)
                
                # Debug: let's check what users exist in those leagues
                if server_leagues:
                    league_ids = [sl['league_id'] for sl in await conn.fetch("""
                        SELECT league_id FROM server_leagues WHERE server_id = $1
                    """, server_id)]
                    
                    if league_ids:
                        users_in_leagues = await conn.fetch("""
                            SELECT DISTINCT u.username 
                            FROM users u
                            JOIN user_leagues ul ON u.user_id = ul.user_id
                            WHERE ul.league_id = ANY($1)
                        """, league_ids)
                        
                        print(f"Debug: Server {server_id} has leagues: {[sl['name'] for sl in server_leagues]}")
                        print(f"Debug: Users in those leagues: {[u['username'] for u in users_in_leagues]}")
                        print(f"Debug: Query returned: {[r['username'] for r in result]}")
                
                return result

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

    async def add_user_to_server(self, username, league_names): # Need to make adding discord_ID optional
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
                    JOIN user_leagues ul ON u.user_id = ul.user_id
                    WHERE ul.league_id = $1
                """, league['league_id'])
                
                if total_users == 0:
                    continue  # Skip if no users in league on this server
                
                # Count ready users (X status) in this league
                ready_users = await conn.fetchval("""
                    SELECT COUNT(DISTINCT u.user_id)
                    FROM users u
                    JOIN user_leagues ul ON u.user_id = ul.user_id
                    WHERE ul.league_id = $1 AND ul.ready_status = 'X'
                """, league['league_id'])
                
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
            
            return league_info['display_name'], new_week

    async def check_user_admin(self, discord_id):
        """Check if a user has admin privileges in the bot"""
        async with self.pool.acquire() as conn:
            is_admin = await conn.fetchval("""
                SELECT is_admin FROM users WHERE discord_id = $1
            """, discord_id)
        
            return bool(is_admin)

    async def set_user_admin(self, username, is_admin):
        """Set admin status for a user"""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE users 
                SET is_admin = $2
                WHERE username = $1
            """, username, is_admin)
            return result == "UPDATE 1"

    async def get_user_by_discord_id(self, discord_id):
        """Get username by Discord ID"""
        async with self.pool.acquire() as conn:
            username = await conn.fetchval(
                "SELECT username FROM users WHERE discord_id = $1", discord_id
            )
            return username

    async def link_discord_user(self, username, discord_id):
        """Link a Discord user ID to a username"""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE users 
                SET discord_id = $2
                WHERE username = $1
            """, username, discord_id)
            return result == "UPDATE 1"

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
            
            # Delete user (cascades to user_leagues)
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
            # Since we removed user_servers table, this returns empty
            # You can modify this based on your needs
            return []

    async def remove_user_from_server(self, username, server_id):
        """Remove a user from a specific server (placeholder - not needed with new schema)"""
        # With the new schema, users aren't tied to specific servers
        # This could remove them from all leagues if needed
        return False
        
    async def check_user_admin(self, discord_id):
        """Check if a user has admin privileges in the bot"""
        async with self.pool.acquire() as conn:
            is_admin = await conn.fetchval("""
                SELECT is_admin FROM users WHERE discord_id = $1
            """, discord_id)
            return bool(is_admin)

    async def set_user_admin(self, username, is_admin):
        """Set admin status for a user"""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE users 
                SET is_admin = $2
                WHERE username = $1
            """, username, is_admin)
            return result == "UPDATE 1"
