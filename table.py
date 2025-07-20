class TableGenerator:
    def __init__(self, db_manager):
        self.db = db_manager

    async def generate_table(self, server_id, show_all_servers=False):
        """Generate ASCII table with proper cross-server logic"""
        leagues = await self.db.get_server_leagues(server_id, show_all_servers)
        users = await self.db.get_server_users(server_id, show_all_servers)
        
        if not leagues:
            return "```\nNo leagues configured.\n```"

        # Calculate column widths - make all columns the same width
        name_width = 8  # Name column total width
        league_width = 4  # League column total width (3 chars + 1 space on each side)
        
        # Build table
        table = "```\n"
        
        # Top border
        table += "+" + "-" * name_width + "+"
        for _ in leagues:
            table += "-" * league_width + "+"
        table += "\n"
        
        # Header row - ensure consistent spacing
        table += "|" + "Name".center(name_width) + "|"
        for league in leagues:
            league_display = league['display_name'][:3].upper()
            table += league_display.center(league_width) + "|"
        table += "\n"
        
        # Header separator
        table += "+" + "-" * name_width + "+"
        for _ in leagues:
            table += "-" * league_width + "+"
        table += "\n"
        
        # User rows
        for user in users:
            username = user['username']
            
            # Format username
            display_name = username.capitalize()[:6]  # Limit to 6 chars to fit in column
            table += "|" + display_name.center(name_width) + "|"
            
            for league in leagues:
                # Get status globally (not per-server)
                status = await self.db.get_user_status(username, league['league_id'])
                
                # Determine display status
                if status is None:
                    # User is not assigned to this league
                    if show_all_servers:
                        display_status = 'X'  # Main server shows X for users not in league
                    else:
                        display_status = ' '  # Individual servers show blank
                elif status == '':
                    # User is in league but not ready
                    display_status = ' '
                else:
                    # User has a custom status or is ready (X)
                    display_status = status[:3]  # Limit to 3 characters to maintain spacing
                
                table += display_status.center(league_width) + "|"
            table += "\n"
            
            # Row separator
            table += "+" + "-" * name_width + "+"
            for _ in leagues:
                table += "-" * league_width + "+"
            table += "\n"
        
        table += "```"
        return table