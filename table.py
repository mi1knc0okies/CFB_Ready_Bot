class TableGenerator:
    def __init__(self, db_manager):
        self.db = db_manager

    async def generate_table(self, server_id, show_all_servers=False):
        """Generate ASCII table with proper cross-server logic and smart filtering"""
        leagues = await self.db.get_server_leagues(server_id, show_all_servers)
        users = await self.db.get_server_users(server_id, show_all_servers)
        
        if not leagues:
            return "```\nNo leagues configured.\n```"

        # Check readiness percentage for each league and filter users if needed
        filtered_users = []
        readiness_info = {}
        
        for league in leagues:
            # Count total users and ready users in this league
            total_users = 0
            ready_users = 0
            
            for user in users:
                status = await self.db.get_user_status(user['username'], league['league_id'])
                if status is not None:  # User is in this league
                    total_users += 1
                    if status == 'X':  # User is ready
                        ready_users += 1
            
            # Calculate readiness percentage
            readiness_percentage = (ready_users / total_users * 100) if total_users > 0 else 0
            readiness_info[league['league_id']] = {
                'total': total_users,
                'ready': ready_users,
                'percentage': readiness_percentage,
                'over_50': readiness_percentage > 50
            }
        
        # Filter users: if any league is over 50% ready, only show non-ready users
        any_league_over_50 = any(info['over_50'] for info in readiness_info.values())
        
        if any_league_over_50:
            # Only show users who are NOT ready (status != 'X') in leagues over 50%
            for user in users:
                should_show = False
                
                for league in leagues:
                    if readiness_info[league['league_id']]['over_50']:
                        status = await self.db.get_user_status(user['username'], league['league_id'])
                        # Show user if they're in this league but NOT ready
                        if status is not None and status != 'X':
                            should_show = True
                            break
                    else:
                        # For leagues under 50%, show everyone in the league
                        status = await self.db.get_user_status(user['username'], league['league_id'])
                        if status is not None:
                            should_show = True
                            break
                
                if should_show:
                    filtered_users.append(user)
        else:
            # No league is over 50% ready, show all users
            filtered_users = users

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
        
        # Header row with league names - ensure consistent spacing
        table += "|" + "Name".center(name_width) + "|"
        for league in leagues:
            league_display = league['display_name'][:3].upper()
            table += league_display.center(league_width) + "|"
        table += "\n"
        
        # Week row (show week numbers for each league)
        table += "|" + "Week".center(name_width) + "|"
        for league in leagues:
            week_display = f"W{league['current_week']}"
            table += week_display.center(league_width) + "|"
        table += "\n"
        
        # Readiness percentage row (show percentage ready for each league)
        table += "|" + "Ready".center(name_width) + "|"
        for league in leagues:
            info = readiness_info[league['league_id']]
            if info['total'] > 0:
                ready_display = f"{info['percentage']:.0f}%"
                if info['over_50']:
                    ready_display = f"{ready_display}"  # Mark leagues over 50%
            else:
                ready_display = "0%"
            table += ready_display.center(league_width) + "|"
        table += "\n"
        
        # Header separator
        table += "+" + "-" * name_width + "+"
        for _ in leagues:
            table += "-" * league_width + "+"
        table += "\n"
        
        # User rows (filtered based on readiness)
        if not filtered_users:
            table += "|" + "All Ready!".center(name_width) + "|"
            for _ in leagues:
                table += " ".center(league_width) + "|"
            table += "\n"
            
            # Row separator
            table += "+" + "-" * name_width + "+"
            for _ in leagues:
                table += "-" * league_width + "+"
            table += "\n"
        else:
            for user in filtered_users:
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
                        display_status = status[:3].upper()  # Limit to 3 characters to maintain spacing
                    
                    table += display_status.center(league_width) + "|"
                table += "\n"
                
                # Row separator
                table += "+" + "-" * name_width + "+"
                for _ in leagues:
                    table += "-" * league_width + "+"
                table += "\n"
        
        # Add footer note if any league is over 50%
        if any_league_over_50:
            table += "\n*Leagues over 50% ready - showing only non-ready players"
        
        table += "```"
        return table