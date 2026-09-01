# Discord Badge Completer

A Python-based Discord activity utility that controls Playing, Streaming, Watching, Listening, and Competing presence activities through a Discord Gateway session.

> **Project purpose**
> This project was designed around Discord beta/recent badge or activity-completion style requirements. Badge eligibility and award decisions are controlled by Discord, so this project does not guarantee that any badge will be awarded.

## Features

- Discord bot control interface with prefix commands
- User-token linking and session management
- Playing and Streaming activity support
- Watching, Listening, and Competing activities
- Random game selection
- Session reconnect/disconnect controls
- Owner-managed access list
- Activity and session status checking
- Configurable game list
- Basic latency command

## Important Discord Policy Notice

**This implementation is not an official Discord badge-claiming API.** It connects a normal Discord user account to the Discord Gateway with a user token and automates that account's presence. In policy terms, that is user-account automation/self-bot behavior, even though a separate Discord bot account is used as the control interface.

Discord currently states that automating normal user accounts outside the OAuth2/Bot API is prohibited and may result in account termination. Please review Discord's current policies before using or distributing this project.

## Security Warning

The current implementation accepts Discord **user tokens** and stores linked tokens in `tokens.json`. User tokens are sensitive credentials.

- Never publish real user tokens.
- Never commit credentials to a public GitHub repository.
- Do not paste tokens into issues, screenshots, logs, or support channels.
- If a token is exposed, secure the affected account and revoke/rotate credentials as appropriate.

## Requirements

- Python 3.9+
- `discord.py==2.3.2`
- `aiohttp==3.9.1`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Use the example configuration as a starting point:

```json
{
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "prefix": "$",
    "owner_id": 123456789012345678,
    "allowed_users": []
}
```

Replace the placeholder values with your own bot configuration. Keep real credentials out of Git history.

## Start

```bash
python bot.py
```

## Commands

| Command | Description |
| --- | --- |
| `$help` | Show available commands |
| `$link` | Open the token-link interface |
| `$token <token>` | Link a user token directly |
| `$unlink` | Remove the current user's saved token |
| `$tokeninfo` | Show linked account/session information |
| `$games` | Show the configured game list |
| `$play <game>` | Set a Playing activity |
| `$randomgame` | Set a random Playing activity |
| `$stop` | Clear the current activity |
| `$status` | Show token/session/activity status |
| `$startstream <game>` | Set a Streaming activity |
| `$stopstream` | Clear the Streaming activity |
| `$watching <text>` | Set a Watching activity |
| `$listening <text>` | Set a Listening activity |
| `$competing <text>` | Set a Competing activity |
| `$disconnect` | Disconnect the linked Gateway session |
| `$reconnect` | Reconnect the linked Gateway session |
| `$ping` | Show bot latency |

### Owner commands

| Command | Description |
| --- | --- |
| `$addaccess @user` | Grant access to a user |
| `$removeaccess @user` | Remove a user's access |
| `$listaccess` | Show the access list |

## File Structure

```text
tok4n-bot/
├── bot.py
├── config.json
├── config.example.json
├── games.json
├── requirements.txt
└── tokens.json
```

## Developer Credits

**Developer:** r3novadcl  
**Development Team:** FX DEVELOPMENT

## Disclaimer

This is an unofficial community project and is not affiliated with, endorsed by, or sponsored by Discord Inc. Discord may change its API, Gateway behavior, eligibility systems, or platform policies at any time. Use of this project is at your own risk and responsibility.
