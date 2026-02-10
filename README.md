# Eigen Bot - Comprehensive Discord Community Bot

> **A feature-rich, production-ready Discord bot for community engagement, support tickets, entertainment, and powerful moderation tools.**

Eigen Bot is an all-in-one Discord bot designed for thriving communities. Built with modern async Python and discord.py, it offers a complete suite of features from support ticket systems and starboard highlights to voting systems and custom tags—all with hybrid command support (both prefix `?` and slash `/` commands).

---

## Developers
- [@youngcoder45](https://github.com/youngcoder45)
- [@1Frodox](https://github.com/1Frodox)

---

## Command Types

Eigen Bot supports **hybrid commands** - both prefix and slash commands:

- **Prefix Commands** (Legacy): `?command` (e.g., `?dailyquest`, `?helpmenu`)
  - Most commands use prefix commands for backward compatibility
  - Fast, familiar, and don't require slash command permissions
  
- **Slash Commands** (Modern): `/command` (e.g., `/help`, `/timestamp`)
  - Selected commands available as slash commands for modern Discord experience
  - Auto-complete and built-in Discord UI
  - Limited to essential commands to stay within Discord's 100 command limit

** Tip**: Use `?helpmenu` or `/help` to explore all available commands!

---

## Core Features

### ** Ticket System**
Professional support ticket management using Discord threads:

**Features:**
- **Multiple Categories**: General Support, Bug Reports, Feature Requests, Partnership, Reports, Other Issues
- **Thread-Based Tickets**: Each ticket is a private thread with organized discussions
- **Interactive UI**: Button-based interface for creating and managing tickets
- **Ticket Controls**: Close tickets, claim tickets (for staff), automatic archiving
- **Customizable Roles**: Set different roles for support, reports, and partnerships
- **Ticket Logging**: Optional logging channel for all ticket actions
- **Persistent Panels**: Create ticket panels that survive bot restarts
- **Numbered Tickets**: Auto-incrementing ticket numbers for easy tracking

**Commands:**
- `/ticketpanel` - Create a persistent ticket panel
- `/ticketlog` - Configure ticket logging channel
- `/ticketsupport` - Set support team role
- `/ticketreport` - Set report team role
- `/ticketpartner` - Set partnership team role
- `/tickets` - List all tickets (open, closed, or all)
- `/ticketstats` - View ticket system statistics
- `/forceclose` - Force close a ticket (admin)

### ** Starboard System**
Highlight the best messages in your community:
- **Automatic Highlighting**: Messages that reach a star threshold appear in starboard
- **Customizable**: Set custom star emoji, adjustable threshold, self-starring toggle
- **Beautiful Embeds**: Dynamic colors based on star count, author thumbnails, timestamps
- **Real-time Updates**: Starboard messages update as stars are added/removed
- **Smart Handling**: Tracks who starred what, prevents duplicates, handles uncached messages
- **Admin Tools**: `?starboard_cleanup` to remove invalid entries

### ** Tag System**
Create and share custom text snippets:
- **Create Tags**: `?tags create <name> <content>` - Store reusable text
- **Retrieve Tags**: `?tag <name>` - Quickly fetch stored content
- **Edit Tags**: `?tags edit <name> <new_content>` - Update existing tags
- **Delete Tags**: `?tags delete <name>` - Remove unwanted tags
- **List Tags**: `?tags list` - View all server tags
- **Usage Tracking**: Tracks how many times each tag is used

### ** Election/Voting System**
Democratic decision-making for your community:
- **Create Elections**: `?election create <title> <candidates> [duration]`
- **Weighted Voting**: Vote strength based on user roles/tenure
- **Multiple Candidates**: Support for 2-10 candidates per election
- **Live Results**: Real-time vote counting and display
- **Interactive Voting**: Button-based voting interface
- **Timed Elections**: Auto-close after specified duration

### ** CodeBuddy System**
Engage your community with coding challenges and leaderboards:
- **Coding Quizzes**: Test your knowledge with automated coding questions
- **Leaderboards**: Weekly, all-time, and streak tracking
- **Stats & Flex**: Personal statistics and shareable stat cards
- **Engagement**: Earn points for correct answers and climb the ranks

### ** Daily Quest System**
Complete daily challenges to earn powerful rewards! Inspired by popular quest systems:

**Features:**
- **Daily Checklist**: Reset every 24 hours with fresh challenges
- **Quest Tasks**:
  - Solve 5 Basic CodeBuddy Quizzes
  - Vote for the Bot on top.gg (coming soon!)
- **Rewards**:
  - **Streak Freezes**: Automatically protect your quiz streak when you answer wrong
  - **Bonus Hints**: Use hints to eliminate wrong answers (ephemeral messages)
- **Progress Tracking**: Monitor your daily quest completion in real-time

**Commands:**
- `?dailyquest` / `?dq` / `?quests` - View daily quest progress
- `/dailyquest` - View quest progress (slash command)
- `?bonushint` / `?hint` - Use a bonus hint on active quiz
- `?inventory` / `?inv` - Check your streak freezes and bonus hints

**How It Works:**
1. Complete 5 quiz questions correctly
2. Vote for the bot (when available)
3. Earn 1 Streak Freeze + 1 Bonus Hint
4. Use rewards strategically to maintain your streak and climb leaderboards!

### ** Fun Commands**
Entertainment and engagement features:
- **Programming Jokes**: `?joke` - Get a clean programming-related joke
- **Compliments**: `?compliment [@user]` - Give professional programming compliments
- **Fortune**: `?fortune` - Receive a programming-themed fortune
- **Trivia**: `?trivia` - Programming trivia questions
- **8-Ball**: `?8ball <question>` - Magic 8-ball responses
- **Coin Flip**: `?coinflip` - Heads or tails
- **Dice Roll**: `?roll [size] [count]` - Roll dice

### ** Community Engagement**
Build an active, engaged community:
- **Random Quotes**: `?quote` - Inspirational programming quotes
- **Memes**: `?meme` - Programming humor from Reddit
- **Suggestions**: `?suggest <text>` - Submit feedback and ideas

### ** Utility Commands**
Helpful tools for server management:
- **Emote List**: `?emotes [search]` - Browse server emojis
- **Member Count**: `?membercount` - View current server member count
- **Random Color**: `?randomcolor` - Generate random hex colors
- **Reminders**: `?remindme <time> <message>` - Set personal reminders
- **Timestamps**: `/timestamp` - Generate Discord timestamps with timezone support

### ** Support & Feedback**
Connect with our support team:
- **Bug Reports**: `?bug` or `/bug` - Report bugs directly to our support server
- **Feature Requests**: `/newfeature` - Suggest new features or improvements
- **Feedback**: `/feedback` - Share your feedback with a 1-5 star rating
- **Support Server**: `?support` or `/support` - Get the support server invite link
- All reports are sent directly to our support team for review

### ** AFK System**
Let people know when you're away:
- **Set AFK**: `?afk [reason]` - Set your AFK status with an optional reason
- **Auto-respond**: Bot automatically notifies users when they mention you
- **Time Tracking**: Shows how long you've been AFK
- **Smart Removal**: Automatically removes AFK status when you send a message

### ** Birthday System**
Celebrate community birthdays:
- **Set Birthday**: `?setbirthday <DD/MM>` - Register your birthday
- **Birthday List**: `?birthdays` - View upcoming birthdays
- **Announcements**: Automatic birthday wishes on your special day
- **Privacy**: Only stores day and month, not year

### ** Admin & Moderation**
Powerful tools for server administrators:
- **Bot Management**: `?reload <cog>` - Reload cogs on the fly
- **Command Sync**: `?sync` - Sync slash commands
- **Permission-Based**: All admin commands restricted to administrators/bot owner

---

## Technical Excellence

### **Architecture & Design**
- **Async/Await**: Full async implementation for optimal performance
- **Hybrid Commands**: Every command works with both `?` prefix and `/` slash commands
- **Cog-Based Structure**: Modular design for easy maintenance and extensibility
- **Type Hints**: Comprehensive type annotations throughout codebase
- **Error Handling**: Graceful error handling with user-friendly messages

### **Database & Persistence**
- **SQLite**: File-based database for tags, starboard, invites, and tickets
- **aiosqlite**: Async SQLite operations for better performance
- **CodeBuddy Database**: Separate database for coding leaderboards
- **Data Integrity**: Proper constraints, indexes, and transaction handling

### **Performance & Reliability**
- **Connection Pooling**: Efficient database connection management
- **Caching**: Smart caching for starboard and invite systems
- **Rate Limiting**: Built-in cooldown management
- **Graceful Degradation**: Continues working even if some features fail
- **Comprehensive Logging**: Detailed logs for debugging and monitoring

### **Security & Safety**
- **Environment Variables**: Secure token and config storage
- **Permission Checks**: Role-based command access control
- **Input Validation**: Sanitization of user inputs
- **SQL Injection Prevention**: Parameterized queries throughout

---

## Installation & Setup

### **Prerequisites**
- Python 3.11 or higher
- FFMpeg (for TTS command)
- Discord Bot Token ([Get one here](https://discord.com/developers/applications))
- Git (for cloning)

### **Quick Start**

1. **Clone the Repository**
   ```bash
   git clone https://github.com/youngcoder45/Eigen-bot-In-Python.git
   cd Eigen-bot-In-Python
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv .venv
   
   # Linux/macOS
   source .venv/bin/activate
   
   # Windows
   .venv\\Scripts\\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your bot token and settings
   ```

5. **Run the Bot**
   ```bash
   python bot.py
   ```

### **Environment Variables**

Create a `.env` file with the following:

```env
# Required
DISCORD_TOKEN=your_bot_token_here

# Bot Configuration
OWNER_ID=your_discord_user_id
LOG_LEVEL=INFO

# Development (optional)
GUILD_ID=your_test_server_id  # For faster slash command sync

# CodeBuddy (optional)
QUESTION_CHANNEL_ID=channel_id_for_coding_questions
```

---

## Usage Guide

### **Command Prefixes**
- **Prefix Commands**: `?command` (e.g., `?help`)
- **Slash Commands**: `/command` (e.g., `/help`)
- **Hybrid**: Most commands support both formats!

### **Ticket Commands**
```
/ticketpanel [#channel] [support_role]  - Create ticket panel
/ticketlog [#channel]                    - Set ticket log channel
/ticketsupport [role]                    - Set support team role
/tickets [status] [user]                 - List tickets
/ticketstats                             - View statistics
/forceclose <ticket_id> [reason]         - Force close ticket
```

### **Starboard Commands**
```
?starboard setup #channel <threshold> <emoji>  - Setup starboard
?starboard stats                               - View statistics
?starboard toggle                              - Enable/disable
```

### **Tag Commands**
```
?tag <name>                      - Retrieve a tag
?tags create <name> <content>    - Create new tag
?tags edit <name> <content>      - Edit existing tag
?tags delete <name>              - Delete a tag
?tags list                       - List all tags
```

### **CodeBuddy Commands**
```
/codeweek           - Weekly coding leaderboard
/codestreak         - View streak leaderboard
/codeleaderboard    - All-time leaderboard
/codestats [@user]  - View coding stats
/codeflex           - Generate stats card image
```

### **Fun Commands**
```
?joke                - Programming joke
?compliment [@user]  - Give compliment
?fortune            - Programming fortune
?trivia             - Programming trivia
?8ball <question>   - Magic 8-ball
?coinflip           - Flip a coin
?roll [size] [count] - Roll dice
```

---

## Economy Bot (Separated)

Economy and casino features have been moved to a separate bot. Check the `another-bot/` folder for:
- Economy system (balance, work, daily, weekly, etc.)
- Casino games (blackjack, roulette, slots, etc.)
- Economy admin commands

See `another-bot/README.md` and `another-bot/MIGRATION_SUMMARY.md` for setup instructions.

---

## Database Files

```
├── botdata.db        # Main database (tickets, codebuddy)
├── tags.db           # Tag system
├── starboard.db      # Starboard system
└── invites.db        # Invite tracker (if enabled)
```

---

## Docker Deployment

### **Using Docker**
```bash
# Build image
docker build -t eigen-bot .

# Run container
docker run -d --env-file .env eigen-bot
```

### **Using Docker Compose**
```bash
docker-compose up -d
```

---

## Contributing

We welcome contributions! Here's how:

1. **Fork** the repository
2. **Create** a feature branch
3. **Commit** your changes
4. **Push** to the branch
5. **Open** a Pull Request

---

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## Legal

- **Terms of Service**: [View Terms](TERMS_OF_SERVICE.md)
- **Privacy Policy**: [View Privacy Policy](PRIVACY_POLICY.md)

By using Eigen Bot, you agree to our Terms of Service and Privacy Policy.

---

## Support & Documentation

### **Getting Help**
- **Bug Reports**: [Open an issue](https://github.com/youngcoder45/Eigen-bot-In-Python/issues)
- **Feature Requests**: [Open an issue](https://github.com/youngcoder45/Eigen-bot-In-Python/issues)

---

## Acknowledgments

Built with:
- [discord.py](https://github.com/Rapptz/discord.py) - Discord API wrapper
- [aiosqlite](https://github.com/omnilib/aiosqlite) - Async SQLite
- [python-dotenv](https://github.com/theskumar/python-dotenv) - Environment management

Special thanks to the discord.py community and all contributors!

---

<div align="center">

**Eigen Bot** - Where Community Meets Support

[GitHub](https://github.com/youngcoder45/Eigen-bot-In-Python) • [Issues](https://github.com/youngcoder45/Eigen-bot-In-Python/issues)

Made with for Discord communities

</div>
