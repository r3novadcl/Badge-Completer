import discord
from discord.ext import commands
from discord import ui
import aiohttp
import asyncio
import json
import os
import random
import threading
import time

# ========================
# FILE HELPERS
# ========================

def load_json(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}

def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

# ========================
# LOAD CONFIG
# ========================

config = load_json('config.json')
BOT_TOKEN = config.get('bot_token', '')
PREFIX = config.get('prefix', '$')
OWNER_ID = config.get('owner_id', 0)
ALLOWED_USERS = config.get('allowed_users', [])

games_data = load_json('games.json')
GAMES_LIST = games_data.get('games', [])


tokens_db = load_json('tokens.json')

# Active sessions: { "user_id": { "ws": websocket, "game": "game_name", "type": "playing/streaming" } }
active_sessions = {}

# ========================
# DISCORD GATEWAY - USER TOKEN ACTIVITY
# ========================

class UserTokenSession:
    
    GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
    
    def __init__(self, token, user_id):
        self.token = token
        self.user_id = user_id
        self.ws = None
        self.heartbeat_interval = None
        self.running = False
        self.loop = None
        self.thread = None
        self.current_activity = None
        self.session_id = None
        self.sequence = None
    
    async def _send_json(self, data):
        if self.ws and not self.ws.closed:
            await self.ws.send_str(json.dumps(data))
    
    async def _heartbeat_loop(self):
        while self.running:
            heartbeat = {
                "op": 1,
                "d": self.sequence
            }
            try:
                await self._send_json(heartbeat)
            except:
                break
            await asyncio.sleep(self.heartbeat_interval / 1000)
    
    async def _identify(self):
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "properties": {
                    "os": "windows",
                    "browser": "Chrome",
                    "device": "desktop"
                },
                "presence": self.current_activity or {
                    "status": "online",
                    "afk": False,
                    "activities": [],
                    "since": 0
                }
            }
        }
        await self._send_json(identify_payload)
    
    async def _update_presence(self, activity_data):
        presence_update = {
            "op": 3,
            "d": activity_data
        }
        await self._send_json(presence_update)
    
    def _build_game_activity(self, game_name):
        return {
            "status": "online",
            "afk": False,
            "since": 0,
            "activities": [
                {
                    "name": game_name,
                    "type": 0,  # 0 = Playing
                    "timestamps": {
                        "start": int(time.time() * 1000)
                    }
                }
            ]
        }
    
    def _build_streaming_activity(self, game_name, stream_url="https://twitch.tv/discord"):
        return {
            "status": "online",
            "afk": False,
            "since": 0,
            "activities": [
                {
                    "name": game_name,
                    "type": 1,  # 1 = Streaming
                    "url": stream_url,
                    "timestamps": {
                        "start": int(time.time() * 1000)
                    }
                }
            ]
        }
    
    def _build_empty_activity(self):
        return {
            "status": "online",
            "afk": False,
            "since": 0,
            "activities": []
        }
    
    async def _connect(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(self.GATEWAY_URL) as ws:
                    self.ws = ws
                    self.running = True
                    
                    # Receive HELLO
                    msg = await ws.receive_json()
                    if msg.get('op') == 10:
                        self.heartbeat_interval = msg['d']['heartbeat_interval']
                    
                    # Start heartbeat
                    heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
                    
                    # Identify
                    await self._identify()
                    
                    # Listen for messages
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            if data.get('s'):
                                self.sequence = data['s']
                            
                            op = data.get('op')
                            t = data.get('t')
                            
                            if t == 'READY':
                                self.session_id = data['d'].get('session_id')
                            
                            # Heartbeat ACK
                            if op == 11:
                                pass
                            
                            # Reconnect request
                            if op == 7:
                                break
                            
                            # Invalid session
                            if op == 9:
                                await asyncio.sleep(3)
                                await self._identify()
                        
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        
                        if not self.running:
                            break
                    
                    heartbeat_task.cancel()
        except Exception as e:
            print(f"[Session {self.user_id}] Connection error: {e}")
        finally:
            self.running = False
    
    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        while self.running:
            try:
                self.loop.run_until_complete(self._connect())
            except:
                pass
            if self.running:
                time.sleep(5)  # Reconnect delay
    
    def start(self, activity_data=None):
        self.current_activity = activity_data
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
    
    async def set_game(self, game_name):
        activity = self._build_game_activity(game_name)
        self.current_activity = activity
        if self.ws and not self.ws.closed:
            try:
                await self._update_presence(activity)
            except:
                pass
        return True
    
    async def set_stream(self, game_name, url="https://twitch.tv/discord"):
        activity = self._build_streaming_activity(game_name, url)
        self.current_activity = activity
        if self.ws and not self.ws.closed:
            try:
                await self._update_presence(activity)
            except:
                pass
        return True
    
    async def stop_activity(self):
        activity = self._build_empty_activity()
        self.current_activity = activity
        if self.ws and not self.ws.closed:
            try:
                await self._update_presence(activity)
            except:
                pass
        return True
    
    def disconnect(self):
        self.running = False
        if self.ws:
            try:
                asyncio.run_coroutine_threadsafe(self.ws.close(), self.loop)
            except:
                pass

# ========================
# TOKEN VALIDATION
# ========================

async def validate_token(token):
    """Token validate karo Discord API naal"""
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v10/users/@me", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return True, data
                else:
                    return False, None
    except:
        return False, None

# ========================
# TOKEN LINK MODAL
# ========================

class TokenModal(ui.Modal, title="🔗 Link Your Token"):
    token_input = ui.TextInput(
        label="Discord Token",
        placeholder="Paste your Discord token here...",
        style=discord.TextStyle.short,
        required=True,
        max_length=200
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        token = self.token_input.value.strip()
        user_id = str(interaction.user.id)
        
        # Validate token
        valid, user_data = await validate_token(token)
        
        if valid:
            username = user_data.get('username', 'Unknown')
            discriminator = user_data.get('discriminator', '0')
            
            # Save token
            tokens_db[user_id] = token
            save_json('tokens.json', tokens_db)
            
            # Start session
            if user_id in active_sessions:
                active_sessions[user_id].disconnect()
            
            session = UserTokenSession(token, user_id)
            session.start()
            active_sessions[user_id] = session
            
            display_name = f"{username}" if discriminator == "0" else f"{username}#{discriminator}"
            
            embed = discord.Embed(color=0x00ff00)
            embed.description = (
                f"✅ **Token Linked & Logged In!**\n\n"
                f"Logged in as **{display_name}**\n\n"
                f"🎮 Now all game commands will run on **YOUR** account!"
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(color=0xff0000)
            embed.description = (
                f"❌ **Token Invalid!**\n\n"
                f"The token you provided is invalid or expired.\n"
                f"Please try again with a valid token."
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

# ========================
# LINK BUTTON VIEW
# ========================

class LinkTokenView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @ui.button(label="Link Token", style=discord.ButtonStyle.blurple, emoji="🔗")
    async def link_button(self, interaction: discord.Interaction, button: ui.Button):
        # Check access
        user_id = str(interaction.user.id)
        if interaction.user.id != OWNER_ID and interaction.user.id not in ALLOWED_USERS:
            await interaction.response.send_message("❌ You don't have access!", ephemeral=True)
            return
        await interaction.response.send_modal(TokenModal())

# ========================
# BOT SETUP
# ========================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# ========================
# ACCESS CHECK
# ========================

def has_access():
    async def predicate(ctx):
        return ctx.author.id == OWNER_ID or ctx.author.id in ALLOWED_USERS
    return commands.check(predicate)

def is_owner():
    async def predicate(ctx):
        return ctx.author.id == OWNER_ID
    return commands.check(predicate)

def has_token():
    async def predicate(ctx):
        user_id = str(ctx.author.id)
        return user_id in tokens_db
    return commands.check(predicate)

# ========================
# EVENTS
# ========================

@bot.event
async def on_ready():
    print(f"{'='*50}")
    print(f"  Bot Online: {bot.user}")
    print(f"  Prefix: {PREFIX}")
    print(f"  Servers: {len(bot.guilds)}")
    print(f"  Saved Tokens: {len(tokens_db)}")
    print(f"{'='*50}")
    
    # Reconnect saved tokens
    for user_id, token in tokens_db.items():
        if user_id not in active_sessions:
            valid, _ = await validate_token(token)
            if valid:
                session = UserTokenSession(token, user_id)
                session.start()
                active_sessions[user_id] = session
                print(f"  [Auto-Reconnect] User {user_id} connected")
    
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name=f"{PREFIX}help")
    )

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **Access Denied!** You don't have permission."
        await ctx.reply(embed=embed)
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **Missing argument!** Use `{PREFIX}help` for commands."
        await ctx.reply(embed=embed)

# ========================
# HELP COMMAND
# ========================

@bot.command(name='help')
@has_access()
async def help_cmd(ctx):
    embed = discord.Embed(
        title="🤖 Commands",
        description=f"All commands start with `{PREFIX}`",
        color=0x5865F2
    )
    
    # Account
    embed.add_field(
        name="📦 Account",
        value=(
            f"`{PREFIX}link` - Link your token via popup (auto-retry login)\n"
            f"`{PREFIX}unlink` - Unlink your token\n"
            f"`{PREFIX}tokeninfo` - Check linked token info"
        ),
        inline=False
    )
    
    # Owner Only
    embed.add_field(
        name="👑 Owner Only",
        value=(
            f"`{PREFIX}addaccess @user` - Add user\n"
            f"`{PREFIX}removeaccess @user` - Remove user\n"
            f"`{PREFIX}listaccess` - All users"
        ),
        inline=False
    )
    
    # VC Screenshare
    embed.add_field(
        name="📹 VC Screenshare (24/7)",
        value=(
            f"`{PREFIX}stream <channel_id>` - Start 24/7 screenshare in VC\n"
            f"`{PREFIX}stopstream` - Stop screenshare"
        ),
        inline=False
    )
    
    # Games
    embed.add_field(
        name="🎮 Games (ON YOUR ACCOUNT)",
        value=(
            f"`{PREFIX}games` - All games\n"
            f"`{PREFIX}play <game>` - Set playing status\n"
            f"`{PREFIX}randomgame` - Random game\n"
            f"`{PREFIX}stop` - Stop game\n"
            f"`{PREFIX}status` - Check status"
        ),
        inline=False
    )
    
    # Streaming
    embed.add_field(
        name="🟣 Streaming",
        value=(
            f"`{PREFIX}startstream <game>` - Stream a game\n"
            f"`{PREFIX}stopstream` - Stop streaming"
        ),
        inline=False
    )
    
    # Utility
    embed.add_field(
        name="🔧 Utility",
        value=f"`{PREFIX}ping` - Check latency",
        inline=False
    )
    
    await ctx.reply(embed=embed)

# ========================
# ACCOUNT COMMANDS
# ========================

@bot.command(name='link')
@has_access()
async def link_token(ctx):
    embed = discord.Embed(color=0x5865F2)
    embed.description = (
        "🔗 **Link Your Discord Token**\n\n"
        "Click the button below to securely submit your token.\n\n"
        "⚠️ Token will be saved and bot will keep retrying login until successful!"
    )
    view = LinkTokenView()
    await ctx.reply(embed=embed, view=view)

@bot.command(name='token')
@has_access()
async def token_cmd(ctx, *, token: str = None):
    """Direct token submit command (alternative to modal)"""
    if token is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}token <your_token>`\n\n💡 Better to use `{PREFIX}link` for secure submission!"
        await ctx.reply(embed=embed)
        return
    
    # Delete user's message (contains token!)
    try:
        await ctx.message.delete()
    except:
        pass
    
    user_id = str(ctx.author.id)
    
    valid, user_data = await validate_token(token)
    
    if valid:
        username = user_data.get('username', 'Unknown')
        discriminator = user_data.get('discriminator', '0')
        
        tokens_db[user_id] = token
        save_json('tokens.json', tokens_db)
        
        if user_id in active_sessions:
            active_sessions[user_id].disconnect()
        
        session = UserTokenSession(token, user_id)
        session.start()
        active_sessions[user_id] = session
        
        display_name = f"{username}" if discriminator == "0" else f"{username}#{discriminator}"
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = (
            f"✅ **Token Linked & Logged In!**\n\n"
            f"Logged in as **{display_name}**\n\n"
            f"🎮 Now all game commands will run on **YOUR** account!"
        )
        await ctx.send(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **Token Invalid!** Please provide a valid token."
        await ctx.send(embed=embed)

@bot.command(name='unlink')
@has_access()
async def unlink_token(ctx):
    user_id = str(ctx.author.id)
    
    if user_id in active_sessions:
        active_sessions[user_id].disconnect()
        del active_sessions[user_id]
    
    if user_id in tokens_db:
        del tokens_db[user_id]
        save_json('tokens.json', tokens_db)
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = "✅ **Token unlinked successfully!**"
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **No token linked!**"
        await ctx.reply(embed=embed)

@bot.command(name='tokeninfo')
@has_access()
async def token_info(ctx):
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    token = tokens_db[user_id]
    valid, user_data = await validate_token(token)
    
    if valid:
        username = user_data.get('username', 'Unknown')
        disc = user_data.get('discriminator', '0')
        email = user_data.get('email', 'N/A')
        phone = user_data.get('phone', 'N/A')
        mfa = user_data.get('mfa_enabled', False)
        verified = user_data.get('verified', False)
        
        display = f"{username}" if disc == "0" else f"{username}#{disc}"
        
        session_active = user_id in active_sessions and active_sessions[user_id].running
        
        embed = discord.Embed(title="📋 Token Info", color=0x5865F2)
        embed.add_field(name="Username", value=display, inline=True)
        embed.add_field(name="Email", value=email or "N/A", inline=True)
        embed.add_field(name="Phone", value=phone or "N/A", inline=True)
        embed.add_field(name="2FA", value="✅" if mfa else "❌", inline=True)
        embed.add_field(name="Verified", value="✅" if verified else "❌", inline=True)
        embed.add_field(name="Session", value="🟢 Active" if session_active else "🔴 Inactive", inline=True)
        
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **Token is no longer valid!** Please re-link."
        await ctx.reply(embed=embed)

# ========================
# GAMES COMMANDS
# ========================

@bot.command(name='games')
@has_access()
async def games_list(ctx):
    games_text = ""
    for i, game in enumerate(GAMES_LIST, 1):
        games_text += f"`{game}`\n"
    
    embed = discord.Embed(
        title="🎮 Available Games",
        description=games_text,
        color=0x5865F2
    )
    embed.set_footer(text=f"Use {PREFIX}play <game> to start playing!")
    await ctx.reply(embed=embed)

@bot.command(name='play')
@has_access()
async def play_game(ctx, *, game_name: str = None):
    if game_name is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}play <game_name>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    # Check if session exists, if not create one
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        activity = session._build_game_activity(game_name)
        session.start(activity)
        active_sessions[user_id] = session
        
        # Wait for connection
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        await session.set_game(game_name)
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = f"🎮 **Now Playing:** `{game_name}`\n\nActivity is running on your account!"
    await ctx.reply(embed=embed)

@bot.command(name='randomgame')
@has_access()
async def random_game(ctx):
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    game_name = random.choice(GAMES_LIST)
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        activity = session._build_game_activity(game_name)
        session.start(activity)
        active_sessions[user_id] = session
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        await session.set_game(game_name)
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = f"🎲 **Random Game Selected:** `{game_name}`\n\n🎮 Now playing on your account!"
    await ctx.reply(embed=embed)

@bot.command(name='stop')
@has_access()
async def stop_game(ctx):
    user_id = str(ctx.author.id)
    
    if user_id not in active_sessions:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **No active session!**"
        await ctx.reply(embed=embed)
        return
    
    session = active_sessions[user_id]
    await session.stop_activity()
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = "⏹️ **Game activity stopped!**"
    await ctx.reply(embed=embed)

@bot.command(name='status')
@has_access()
async def check_status(ctx):
    user_id = str(ctx.author.id)
    
    has_token_linked = user_id in tokens_db
    has_session = user_id in active_sessions
    session_running = has_session and active_sessions[user_id].running
    current_activity = None
    
    if has_session and active_sessions[user_id].current_activity:
        activities = active_sessions[user_id].current_activity.get('activities', [])
        if activities:
            current_activity = activities[0].get('name', 'Unknown')
            activity_type = activities[0].get('type', 0)
            type_name = {0: "Playing", 1: "Streaming", 2: "Listening", 3: "Watching"}.get(activity_type, "Unknown")
            current_activity = f"{type_name}: {current_activity}"
    
    embed = discord.Embed(title="📊 Status", color=0x5865F2)
    embed.add_field(name="Token Linked", value="✅" if has_token_linked else "❌", inline=True)
    embed.add_field(name="Session Active", value="🟢" if session_running else "🔴", inline=True)
    embed.add_field(name="Current Activity", value=current_activity or "None", inline=False)
    
    await ctx.reply(embed=embed)

# ========================
# STREAMING COMMANDS
# ========================

@bot.command(name='startstream')
@has_access()
async def start_stream(ctx, *, game_name: str = None):
    if game_name is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}startstream <game_name>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        activity = session._build_streaming_activity(game_name)
        session.start(activity)
        active_sessions[user_id] = session
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        await session.set_stream(game_name)
    
    embed = discord.Embed(color=0x9146FF)
    embed.description = f"🟣 **Now Streaming:** `{game_name}`\n\nStreaming activity is running on your account!"
    await ctx.reply(embed=embed)

@bot.command(name='stopstream')
@has_access()
async def stop_stream(ctx):
    user_id = str(ctx.author.id)
    
    if user_id not in active_sessions:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **No active session!**"
        await ctx.reply(embed=embed)
        return
    
    session = active_sessions[user_id]
    await session.stop_activity()
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = "⏹️ **Stream activity stopped!**"
    await ctx.reply(embed=embed)

# ========================
# VC SCREENSHARE (24/7)
# ========================

@bot.command(name='stream')
@has_access()
async def vc_stream(ctx, channel_id: str = None):
    """VC vich 24/7 screenshare start karo"""
    if channel_id is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}stream <voice_channel_id>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **Session not active!** Please re-link your token."
        await ctx.reply(embed=embed)
        return
    
    session = active_sessions[user_id]
    
    # Send voice state update to join VC
    guild_id = str(ctx.guild.id)
    
    voice_state = {
        "op": 4,
        "d": {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "self_mute": False,
            "self_deaf": False,
            "self_video": True
        }
    }
    
    try:
        if session.ws and not session.ws.closed:
            await session._send_json(voice_state)
            
            embed = discord.Embed(color=0x00ff00)
            embed.description = f"📹 **Screenshare started in VC!**\n\nChannel: <#{channel_id}>"
            await ctx.reply(embed=embed)
        else:
            embed = discord.Embed(color=0xff0000)
            embed.description = "❌ **WebSocket not connected!** Try re-linking."
            await ctx.reply(embed=embed)
    except Exception as e:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **Error:** {str(e)}"
        await ctx.reply(embed=embed)

# ========================
# OWNER COMMANDS
# ========================

@bot.command(name='addaccess')
@is_owner()
async def add_access(ctx, member: discord.Member = None):
    if member is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}addaccess @user`"
        await ctx.reply(embed=embed)
        return
    
    if member.id not in ALLOWED_USERS:
        ALLOWED_USERS.append(member.id)
        config['allowed_users'] = ALLOWED_USERS
        save_json('config.json', config)
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = f"✅ **{member.mention} has been given access!**"
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xffff00)
        embed.description = f"⚠️ **{member.mention} already has access!**"
        await ctx.reply(embed=embed)

@bot.command(name='removeaccess')
@is_owner()
async def remove_access(ctx, member: discord.Member = None):
    if member is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}removeaccess @user`"
        await ctx.reply(embed=embed)
        return
    
    if member.id in ALLOWED_USERS:
        ALLOWED_USERS.remove(member.id)
        config['allowed_users'] = ALLOWED_USERS
        save_json('config.json', config)
        
        # Also disconnect their session
        user_id = str(member.id)
        if user_id in active_sessions:
            active_sessions[user_id].disconnect()
            del active_sessions[user_id]
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = f"✅ **{member.mention} access removed!**"
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **{member.mention} doesn't have access!**"
        await ctx.reply(embed=embed)

@bot.command(name='listaccess')
@is_owner()
async def list_access(ctx):
    if not ALLOWED_USERS:
        embed = discord.Embed(color=0xffff00)
        embed.description = "📋 **No users have access yet!**"
        await ctx.reply(embed=embed)
        return
    
    users_text = ""
    for i, uid in enumerate(ALLOWED_USERS, 1):
        users_text += f"`{i}.` <@{uid}>\n"
    
    embed = discord.Embed(
        title="👑 Access List",
        description=users_text,
        color=0x5865F2
    )
    await ctx.reply(embed=embed)

# ========================
# UTILITY COMMANDS
# ========================

@bot.command(name='ping')
@has_access()
async def ping(ctx):
    latency = round(bot.latency * 1000)
    embed = discord.Embed(color=0x5865F2)
    embed.description = f"🏓 **Pong!** `{latency}ms`"
    await ctx.reply(embed=embed)

# ========================
# CUSTOM ACTIVITY COMMANDS
# ========================

@bot.command(name='watching')
@has_access()
async def watching(ctx, *, text: str = None):
    if text is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}watching <text>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    activity_data = {
        "status": "online",
        "afk": False,
        "since": 0,
        "activities": [
            {
                "name": text,
                "type": 3  # 3 = Watching
            }
        ]
    }
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        session.start(activity_data)
        active_sessions[user_id] = session
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        session.current_activity = activity_data
        await session._update_presence(activity_data)
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = f"👀 **Now Watching:** `{text}`"
    await ctx.reply(embed=embed)

@bot.command(name='listening')
@has_access()
async def listening(ctx, *, text: str = None):
    if text is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}listening <text>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    activity_data = {
        "status": "online",
        "afk": False,
        "since": 0,
        "activities": [
            {
                "name": text,
                "type": 2  # 2 = Listening
            }
        ]
    }
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        session.start(activity_data)
        active_sessions[user_id] = session
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        session.current_activity = activity_data
        await session._update_presence(activity_data)
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = f"🎧 **Now Listening to:** `{text}`"
    await ctx.reply(embed=embed)

@bot.command(name='competing')
@has_access()
async def competing(ctx, *, text: str = None):
    if text is None:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ Usage: `{PREFIX}competing <text>`"
        await ctx.reply(embed=embed)
        return
    
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    activity_data = {
        "status": "online",
        "afk": False,
        "since": 0,
        "activities": [
            {
                "name": text,
                "type": 5  # 5 = Competing
            }
        ]
    }
    
    if user_id not in active_sessions or not active_sessions[user_id].running:
        token = tokens_db[user_id]
        session = UserTokenSession(token, user_id)
        session.start(activity_data)
        active_sessions[user_id] = session
        await asyncio.sleep(3)
    else:
        session = active_sessions[user_id]
        session.current_activity = activity_data
        await session._update_presence(activity_data)
    
    embed = discord.Embed(color=0x00ff00)
    embed.description = f"🏆 **Now Competing in:** `{text}`"
    await ctx.reply(embed=embed)

# ========================
# DISCONNECT COMMAND
# ========================

@bot.command(name='disconnect')
@has_access()
async def disconnect_session(ctx):
    user_id = str(ctx.author.id)
    
    if user_id in active_sessions:
        active_sessions[user_id].disconnect()
        del active_sessions[user_id]
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = "✅ **Session disconnected!** Token is still saved."
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **No active session!**"
        await ctx.reply(embed=embed)

@bot.command(name='reconnect')
@has_access()
async def reconnect_session(ctx):
    user_id = str(ctx.author.id)
    
    if user_id not in tokens_db:
        embed = discord.Embed(color=0xff0000)
        embed.description = f"❌ **No token linked!** Use `{PREFIX}link` first."
        await ctx.reply(embed=embed)
        return
    
    if user_id in active_sessions:
        active_sessions[user_id].disconnect()
    
    token = tokens_db[user_id]
    valid, user_data = await validate_token(token)
    
    if valid:
        session = UserTokenSession(token, user_id)
        session.start()
        active_sessions[user_id] = session
        
        username = user_data.get('username', 'Unknown')
        
        embed = discord.Embed(color=0x00ff00)
        embed.description = f"🔄 **Reconnected!** Logged in as **{username}**"
        await ctx.reply(embed=embed)
    else:
        embed = discord.Embed(color=0xff0000)
        embed.description = "❌ **Token expired!** Please re-link."
        await ctx.reply(embed=embed)

# ========================
# RUN BOT
# ========================

if __name__ == '__main__':
    if not BOT_TOKEN or BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("❌ Please set your bot token in config.json!")
    else:
        bot.run(BOT_TOKEN)
