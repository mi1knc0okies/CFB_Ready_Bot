import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import asyncio
import sys

from db import DatabaseManager
from table import TableGenerator
from commands import BotCommands

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()


class CFBBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            command_prefix='!',
            intents=intents,
            application_id=os.getenv('APP_ID')
        )

        self.db = DatabaseManager()
        self.table_generator = TableGenerator(self.db)
        self.main_server_id = int(os.getenv('MAIN_SERVER_ID')) if os.getenv('MAIN_SERVER_ID') else None
        
        # Setup commands
        self.commands_handler = BotCommands(self)
        self.commands_handler.setup_commands()

    def get_user_mapping(self, user_id):
        """Get username from environment variables"""
        for key, value in os.environ.items():
            if key.startswith(('PATH', 'HOME', 'USER', 'DISCORD_', 'APP_', 'MAIN_', 'DB_')):
                continue
            try:
                parts = value.split(',')
                env_user_id = int(parts[0].strip())
                if env_user_id == user_id:
                    username = key.lower()
                    allowed = int(parts[1].strip()) if len(parts) > 1 else 0
                    return username, allowed
            except:
                continue
        return None, 0

    async def get_main_channel(self, server_id):
        """Get main channel for a server"""
        async with self.db.pool.acquire() as conn:
            channel_id = await conn.fetchval(
                "SELECT main_channel_id FROM servers WHERE server_id = $1",
                server_id
            )
            return self.get_channel(channel_id) if channel_id else None

    async def update_table_message(self, server_id):
        """Update or post the table message for a server"""
        async with self.db.pool.acquire() as conn:
            server_info = await conn.fetchrow(
                "SELECT main_channel_id, table_message_id, is_main_server FROM servers WHERE server_id = $1",
                server_id
            )

            if not server_info or not server_info['main_channel_id']:
                return

            channel = self.get_channel(server_info['main_channel_id'])
            if not channel:
                return

            show_all = server_info['is_main_server']
            table_content = await self.table_generator.generate_table(server_id, show_all_servers=show_all)

            # Try to edit existing message
            if server_info['table_message_id']:
                try:
                    message = await channel.fetch_message(server_info['table_message_id'])
                    await message.edit(content=table_content)
                    return
                except discord.NotFound:
                    pass

            # Send new message
            message = await channel.send(table_content)
            await conn.execute(
                "UPDATE servers SET table_message_id = $1 WHERE server_id = $2",
                message.id, server_id
            )

    async def post_status_update(self, server_id, message_content):
        """Post a status update message, editing the previous one if possible"""
        async with self.db.pool.acquire() as conn:
            server_info = await conn.fetchrow(
                "SELECT main_channel_id, latest_status_message_id FROM servers WHERE server_id = $1",
                server_id
            )

            if not server_info or not server_info['main_channel_id']:
                return

            channel = self.get_channel(server_info['main_channel_id'])
            if not channel:
                return

            # Try to edit the existing status message
            if server_info['latest_status_message_id']:
                try:
                    old_message = await channel.fetch_message(server_info['latest_status_message_id'])
                    await old_message.edit(content=message_content)
                    return
                except discord.NotFound:
                    pass  # Message was deleted, we'll send a new one

            # Send new status message
            try:
                new_message = await channel.send(message_content)
                await conn.execute(
                    "UPDATE servers SET latest_status_message_id = $1 WHERE server_id = $2",
                    new_message.id, server_id
                )
            except Exception as e:
                print(f"Failed to send status update: {e}")

    async def setup_hook(self):
        print("Setting up bot...")
        await self.db.init_pool()
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")

    async def on_ready(self):
        print(f'{self.user} is ready!')
        print(f"Bot is in {len(self.guilds)} guilds")


# Create and run bot
if __name__ == '__main__':
    bot = CFBBot()
    bot.run(os.getenv('DISCORD_TOKEN'))