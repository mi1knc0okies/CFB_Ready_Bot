import discord
from discord import app_commands
import asyncpg
from datetime import datetime
from zoneinfo import ZoneInfo


class BotCommands:
    def __init__(self, bot):
        self.bot = bot

    def setup_commands(self):
        """Register all slash commands"""
        
        @self.bot.tree.command(name="setup", description="Setup the bot for this server")
        @app_commands.describe(channel="Channel to post tables in")
        async def setup(interaction: discord.Interaction, channel: discord.TextChannel):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            server_id = interaction.guild.id
            is_main = (server_id == self.bot.main_server_id)

            async with self.bot.db.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO servers (server_id, name, main_channel_id, is_main_server)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (server_id) DO UPDATE SET
                        name = $2, main_channel_id = $3, is_main_server = $4
                """, server_id, interaction.guild.name, channel.id, is_main)

            await self.bot.update_table_message(server_id)
            await interaction.response.send_message(f"Bot setup complete! Table will be posted in {channel.mention}", ephemeral=True)

        @self.bot.tree.command(name="create_league", description="Create a new league")
        @app_commands.describe(
            name="League identifier (e.g., rel, fcs)",
            display_name="Display name (e.g., REL, FCS)"
        )
        async def create_league(interaction: discord.Interaction, name: str, display_name: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            async with self.bot.db.pool.acquire() as conn:
                try:
                    await conn.execute(
                        "INSERT INTO leagues (name, display_name) VALUES ($1, $2)",
                        name.lower(), display_name.upper()
                    )
                    await interaction.response.send_message(f"League '{display_name}' created!", ephemeral=True)
                except asyncpg.UniqueViolationError:
                    await interaction.response.send_message(f"League '{name}' already exists!", ephemeral=True)

        @self.bot.tree.command(name="assign_league", description="Assign a league to this server")
        @app_commands.describe(league_name="League identifier to assign")
        async def assign_league(interaction: discord.Interaction, league_name: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            async with self.bot.db.pool.acquire() as conn:
                # Get league ID
                league = await conn.fetchrow(
                    "SELECT league_id, display_name FROM leagues WHERE name = $1",
                    league_name.lower()
                )

                if not league:
                    await interaction.response.send_message(f"League '{league_name}' not found!", ephemeral=True)
                    return

                # Assign to server
                try:
                    await conn.execute(
                        "INSERT INTO server_leagues (server_id, league_id, current_week) VALUES ($1, $2, 1)",
                        interaction.guild.id, league['league_id']
                    )
                    await self.bot.update_table_message(interaction.guild.id)
                    await interaction.response.send_message(f"League '{league['display_name']}' assigned to this server!", ephemeral=True)
                except asyncpg.UniqueViolationError:
                    await interaction.response.send_message(f"League '{league['display_name']}' already assigned to this server!", ephemeral=True)

        @self.bot.tree.command(name="add_user", description="Add a user and assign them to leagues")
        @app_commands.describe(
            username="Username to add",
            leagues="Comma-separated league names they participate in"
        )
        async def add_user(interaction: discord.Interaction, username: str, leagues: str):
            await interaction.response.defer(ephemeral=True)
            
            is_admin = (interaction.user.guild_permissions.administrator or 
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)
            
            if not is_admin:
                await interaction.followup.send("You need administrator permissions.", ephemeral=True)
                return
            
            league_names = [l.strip().lower() for l in leagues.split(',')]
            
            removed_leagues = await self.bot.db.remove_user_from_leagues(username, league_names)
            
            if removed_leagues:
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                await interaction.followup.send(f"Removed {username} from leagues: {', '.join(removed_leagues)}", ephemeral=True)
            else:
                await interaction.followup.send(f"User {username} was not found in any of those leagues.", ephemeral=True)

        @self.bot.tree.command(name="delete_user", description="Completely delete a user from all servers and leagues")
        @app_commands.describe(username="Username to completely delete (PERMANENT)")
        async def delete_user(interaction: discord.Interaction, username: str):
            if not await check_admin_permissions(interaction):
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            result = await self.bot.db.delete_user_completely(username)
            
            if result:
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                await interaction.followup.send(f"⚠️ PERMANENTLY deleted {username} from all servers and leagues.", ephemeral=True)
            else:
                await interaction.followup.send(f"User {username} was not found.", ephemeral=True)

        @self.bot.tree.command(name="sync_commands", description="Sync new commands with Discord")
        async def sync_commands(interaction: discord.Interaction):
            if not await check_admin_permissions(interaction):
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            try:
                synced = await self.bot.tree.sync()
                await interaction.followup.send(f"✅ Synced {len(synced)} commands with Discord!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to sync commands: {e}", ephemeral=True)
            
            league_names = [league.strip() for league in leagues.split(',')]
            
            # Add user to server and leagues
            valid_leagues, invalid_leagues = await self.bot.db.add_user_to_server(
                username, interaction.guild.id, league_names
            )
            
            await self.bot.update_table_message(interaction.guild.id)
            if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                await self.bot.update_table_message(self.bot.main_server_id)
            
            # Build response message
            response_msg = ""
            if valid_leagues:
                response_msg += f"Added {username} to leagues: {', '.join(valid_leagues)}"
            
            if invalid_leagues:
                if response_msg:
                    response_msg += f"\n\nWarning: These leagues were not found: {', '.join(invalid_leagues)}"
                else:
                    response_msg = f"Warning: These leagues were not found: {', '.join(invalid_leagues)}"
            
            await interaction.followup.send(response_msg, ephemeral=True)

        @self.bot.tree.command(name="ready", description="Mark yourself as ready for specified leagues")
        @app_commands.describe(leagues="Comma-separated league names")
        async def ready(interaction: discord.Interaction, leagues: str):
            await interaction.response.defer(ephemeral=True)
            
            username, allowed = await self.bot.get_user_mapping(interaction.user.id)
            if not username:
                await interaction.followup.send("You are not registered. Contact admin.", ephemeral=True)
                return
            
            league_names = [l.strip().lower() for l in leagues.split(',')]
            
            # Update status for each league
            updated_leagues = []
            for league_name in league_names:
                if await self.bot.db.update_user_status(username, league_name, 'X'):
                    updated_leagues.append(league_name)
            
            if not updated_leagues:
                await interaction.followup.send("You are not assigned to any of those leagues.", ephemeral=True)
                return
            
            # Update tables
            await self.bot.update_table_message(interaction.guild.id)
            if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                await self.bot.update_table_message(self.bot.main_server_id)
            
            # Check for auto-advance
            advanced_leagues = await self.bot.db.check_auto_advance(interaction.guild.id)
            if advanced_leagues:
                # Update tables again after auto-advance
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                
                # Post auto-advance announcements
                channel = await self.bot.get_main_channel(interaction.guild.id)
                if channel:
                    for league_name in advanced_leagues:
                        await channel.send(f"🚀 **{league_name} auto-advanced!** All players were ready.")
                
                await interaction.followup.send(f"Marked ready for: {', '.join(updated_leagues)} (Auto-advanced: {', '.join(advanced_leagues)})", ephemeral=True)
            else:
                # Post public status update
                status_msg = f"✅ **{username.capitalize()}** marked ready for: {', '.join(updated_leagues)}"
                await self.bot.post_status_update(interaction.guild.id, status_msg)
                
                await interaction.followup.send(f"Marked ready for: {', '.join(updated_leagues)}", ephemeral=True)

        @self.bot.tree.command(name="unready", description="Mark yourself as not ready for specified leagues")
        @app_commands.describe(leagues="Comma-separated league names")
        async def unready(interaction: discord.Interaction, leagues: str):
            await interaction.response.defer(ephemeral=True)
            
            username, allowed = await self.bot.get_user_mapping(interaction.user.id)
            if not username:
                await interaction.followup.send("You are not registered. Contact admin.", ephemeral=True)
                return
            
            league_names = [l.strip().lower() for l in leagues.split(',')]
            
            for league_name in league_names:
                await self.bot.db.update_user_status(username, league_name, '')
            
            await self.bot.update_table_message(interaction.guild.id)
            if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                await self.bot.update_table_message(self.bot.main_server_id)
            
            # Post public status update
            status_msg = f"❌ **{username.capitalize()}** marked not ready for: {', '.join(league_names)}"
            await self.bot.post_status_update(interaction.guild.id, status_msg)
            
            await interaction.followup.send(f"Marked not ready for: {', '.join(league_names)}", ephemeral=True)

        @self.bot.tree.command(name="set_status", description="Set custom status for a user in a league")
        @app_commands.describe(
            username="Username to set status for",
            league="League name", 
            status="Custom status (bri, don, bye, etc.) or leave empty to clear"
        )
        async def set_status(interaction: discord.Interaction, username: str, league: str, status: str = ""):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            league = league.lower().strip()
            status = status.strip()[:3]  # Limit to 3 characters to maintain spacing

            result = await self.bot.db.update_user_status(username, league, status)
            
            if result:
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                
                status_msg = f"cleared status" if not status else f"set status to '{status}'"
                await interaction.followup.send(f"{status_msg} for {username} in {league.upper()}", ephemeral=True)
            else:
                await interaction.followup.send(f"User '{username}' not found or not in league '{league}'.", ephemeral=True)

        @self.bot.tree.command(name="advance", description="Clear a league and advance to next week")
        @app_commands.describe(league="League name to clear and advance")
        async def advance(interaction: discord.Interaction, league: str):
            await interaction.response.defer(ephemeral=True)
            
            username, allowed = self.bot.get_user_mapping(interaction.user.id)
            if not username:
                await interaction.followup.send("You are not registered. Contact admin.", ephemeral=True)
                return
            
            result = await self.bot.db.advance_league(interaction.guild.id, league.lower())
            if not result:
                await interaction.followup.send(f"League '{league}' not found!", ephemeral=True)
                return
            
            league_display, new_week = result
            
            # Update tables
            await self.bot.update_table_message(interaction.guild.id)
            if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                await self.bot.update_table_message(self.bot.main_server_id)
            
            # Send confirmation privately
            await interaction.followup.send(f"Advanced {league_display} to Week {new_week}!", ephemeral=True)
            
            # Post public announcement
            eastern = ZoneInfo("America/New_York")
            now = datetime.now(eastern)
            day_str = now.strftime("%A")
            time_str = now.strftime("%I:%M %p")
            
            channel = await self.bot.get_main_channel(interaction.guild.id)
            if channel:
                await channel.send(f"🏈 **{league_display} Week {new_week}** advanced by {interaction.user.mention} on {day_str} at {time_str}")

        @self.bot.tree.command(name="set_week", description="Set the current week for a league")
        @app_commands.describe(
            league="League name",
            week="Week number to set"
        )
        async def set_week(interaction: discord.Interaction, league: str, week: int):
            # Allow any registered user to set weeks
            username, allowed = self.bot.get_user_mapping(interaction.user.id)
            if not username:
                await interaction.response.send_message("You are not registered. Contact admin.", ephemeral=True)
                return

            if week < 1:
                await interaction.response.send_message("Week must be 1 or greater.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            result = await self.bot.db.set_league_week(interaction.guild.id, league.lower(), week)
            
            if result:
                league_display, old_week = result
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                
                await interaction.followup.send(f"Set {league_display} to Week {week} (was Week {old_week})", ephemeral=True)
            else:
                await interaction.followup.send(f"League '{league}' not found or not assigned to this server.", ephemeral=True)

        @self.bot.tree.command(name="add_user_to_league", description="Add an existing user to specific leagues")
        @app_commands.describe(
            username="Username to add to leagues",
            leagues="Comma-separated league names"
        )
        async def add_user_to_league(interaction: discord.Interaction, username: str, leagues: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            league_names = [l.strip().lower() for l in leagues.split(',')]
            
            result = await self.bot.db.add_existing_user_to_leagues(username, league_names)
            
            if result is None:
                await interaction.followup.send(f"User '{username}' not found.", ephemeral=True)
                return
            
            valid_leagues, invalid_leagues = result
            
            await self.bot.update_table_message(interaction.guild.id)
            if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                await self.bot.update_table_message(self.bot.main_server_id)
            
            # Build response message
            response_msg = ""
            if valid_leagues:
                response_msg += f"Added {username} to leagues: {', '.join(valid_leagues)}"
            
            if invalid_leagues:
                if response_msg:
                    response_msg += f"\n\nWarning: These leagues were not found: {', '.join(invalid_leagues)}"
                else:
                    response_msg = f"Warning: These leagues were not found: {', '.join(invalid_leagues)}"
            
            await interaction.followup.send(response_msg, ephemeral=True)

        @self.bot.tree.command(name="link_discord", description="Link a Discord user to a username")
        @app_commands.describe(
            username="Username to link",
            user="Discord user to link to this username"
        )
        async def link_discord(interaction: discord.Interaction, username: str, user: discord.User):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            
            result = await self.bot.db.link_discord_user(username, user.id)
            
            if result:
                await interaction.followup.send(f"Linked Discord user {user.mention} to username '{username}'", ephemeral=True)
            else:
                await interaction.followup.send(f"Username '{username}' not found.", ephemeral=True)

        @self.bot.tree.command(name="status", description="View current table")
        async def status(interaction: discord.Interaction):
            async with self.bot.db.pool.acquire() as conn:
                is_main = await conn.fetchval(
                    "SELECT is_main_server FROM servers WHERE server_id = $1",
                    interaction.guild.id
                )

            table = await self.bot.table_generator.generate_table(interaction.guild.id, show_all_servers=bool(is_main))

            # Try to post in the main channel first
            channel = await self.bot.get_main_channel(interaction.guild.id)
            if channel:
                try:
                    await channel.send(table)
                    await interaction.response.send_message("Status posted in channel!", ephemeral=True)
                    return
                except:
                    pass

            # Fallback to ephemeral response
            await interaction.response.send_message(table, ephemeral=True)

        @self.bot.tree.command(name="remove_user", description="Remove a user from this server")
        @app_commands.describe(username="Username to remove from this server")
        async def remove_user(interaction: discord.Interaction, username: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            result = await self.bot.db.remove_user_from_server(username, interaction.guild.id)
            
            if result:
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                await interaction.followup.send(f"Removed {username} from this server.", ephemeral=True)
            else:
                await interaction.followup.send(f"User {username} was not found on this server.", ephemeral=True)

        @self.bot.tree.command(name="remove_user_from_league", description="Remove a user from specific leagues")
        @app_commands.describe(
            username="Username to remove from leagues",
            leagues="Comma-separated league names to remove them from"
        )
        async def remove_user_from_league(interaction: discord.Interaction, username: str, leagues: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            username = username.lower().strip()
            league_names = [l.strip().lower() for l in leagues.split(',')]
            
            removed_leagues = await self.bot.db.remove_user_from_leagues(username, league_names)
            
            if removed_leagues:
                await self.bot.update_table_message(interaction.guild.id)
                if interaction.guild.id != self.bot.main_server_id and self.bot.main_server_id:
                    await self.bot.update_table_message(self.bot.main_server_id)
                await interaction.followup.send(f"Removed {username} from leagues: {', '.join(removed_leagues)}", ephemeral=True)
            else:
                await interaction.followup.send(f"User {username} was not found in any of those leagues.", ephemeral=True)

        @self.bot.tree.command(name="user_info", description="Show detailed information about a user")
        @app_commands.describe(username="Username to get info for")
        async def user_info(interaction: discord.Interaction, username: str):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            username = username.lower().strip()
            
            # Get user's leagues and servers
            leagues = await self.bot.db.get_user_leagues(username)
            servers = await self.bot.db.get_user_servers(username)
            
            if not leagues and not servers:
                await interaction.response.send_message(f"User '{username}' not found.", ephemeral=True)
                return

            message = f"**User Info: {username.capitalize()}**\n\n"
            
            if servers:
                message += "**Active on servers:**\n"
                for server in servers:
                    message += f"• {server['name']} (ID: {server['server_id']})\n"
            else:
                message += "**Active on servers:** None\n"
            
            message += "\n"
            
            if leagues:
                message += "**League memberships:**\n"
                for league in leagues:
                    status = league['ready_status'] if league['ready_status'] else 'Not Ready'
                    message += f"• {league['display_name']} (`{league['name']}`): {status}\n"
            else:
                message += "**League memberships:** None\n"

            await interaction.response.send_message(message, ephemeral=True)

        @self.bot.tree.command(name="list_users", description="List all users on this server")
        async def list_users(interaction: discord.Interaction):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            users = await self.bot.db.get_server_users(interaction.guild.id, show_all_servers=False)
            
            if not users:
                await interaction.response.send_message("No users found on this server.", ephemeral=True)
                return

            message = f"**Users on {interaction.guild.name}:**\n"
            for user in users:
                message += f"• {user['username'].capitalize()}\n"

            await interaction.response.send_message(message, ephemeral=True)

        @self.bot.tree.command(name="sync_commands", description="Sync new commands with Discord")
        async def sync_commands(interaction: discord.Interaction):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            
            try:
                synced = await self.bot.tree.sync()
                await interaction.followup.send(f"✅ Synced {len(synced)} commands with Discord!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to sync commands: {e}", ephemeral=True)

        @self.bot.tree.command(name="migrate", description="Migrate existing data to new schema")
        async def migrate(interaction: discord.Interaction):
            is_admin = (interaction.user.guild_permissions.administrator or
                       interaction.user.id == interaction.guild.owner_id or
                       interaction.user.guild_permissions.manage_guild)

            if not is_admin:
                await interaction.response.send_message("You need administrator permissions.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)
            await self.bot.db.migrate_existing_data()
            await interaction.followup.send("Migration completed!", ephemeral=True)