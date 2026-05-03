"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         CHILL PILL · Discord Activity Bot                     ║
║                           Premium Rank & Moderation System                    ║
║              Enhanced Embeds · Better UI · Improved Performance               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging, os, json, time, io, re, math, random, asyncio
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import aiohttp
import pytz
from datetime import datetime, timedelta, time as dt_time
import signal
import sys
import math

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()
TOKEN     = os.getenv("DISCORD_TOKEN")
DATA_FILE = "data.json"
BOT_NAME  = "Chill Pill"
BOT_COLOR = 0x7C3AED

# ── In-memory trackers ─────────────────────────────────────────────────────────
spam_tracker: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
vc_sessions:  dict[int, dict[int, float]]        = defaultdict(dict)
active_tickets: dict[int, dict] = {}  # Store active ticket info by channel ID

# ══════════════════════════════════════════════════════════════════════════════
#  DATA HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                return data
        except json.JSONDecodeError:
            log.error(f"Corrupted data file! Trying backup...")
            if os.path.exists(f"{DATA_FILE}.backup"):
                try:
                    with open(f"{DATA_FILE}.backup", "r") as f:
                        data = json.load(f)
                        log.info("✅ Loaded from backup successfully")
                        return data
                except:
                    pass
            log.warning("⚠️ Starting with fresh data")
            return {}
        except Exception as e:
            log.error(f"Error loading data: {e}")
            return {}
    return {}

def save_data(data: dict):
    try:
        # 1. Create backup of existing data first
        if os.path.exists(DATA_FILE):
            import shutil
            shutil.copy(DATA_FILE, f"{DATA_FILE}.backup")
        
        # 2. Save new data
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
        
        log.info(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        log.error(f"Failed to save data: {e}")

def get_guild_data(data: dict, guild_id: int) -> dict:
    gid = str(guild_id)
    if gid not in data:
        data[gid] = {"settings": _default_settings(), "users": {}}
    s = data[gid]["settings"]
    for k, v in _default_settings().items():
        s.setdefault(k, v)
    return data[gid]

def _default_settings() -> dict:
    return {
        "role_thresholds":          {},
        "vc_role_thresholds":       {},
        "conditional_paths":        [],
        "whitelisted_channels":     [],
        "blacklisted_channels":     [],
        "blacklisted_members":      [],
        "owner_ids":                [],
        "spam_interval":            3,
        "count_emojis":             True,
        "include_vc_in_roles":      False,
        "mute_role":                None,
        "log_channel":              None,
        "automod_enabled":          False,
        "automod_banned_words":     [],
        "automod_max_mentions":     5,
        "automod_max_caps_pct":     80,
        "ticket_category":          None,
        "ticket_support_role":      None,
        "xp_enabled":               True,
        "xp_min":                   1,
        "xp_max":                   5,
        "xp_cooldown":              10,
        "streak_enabled":           True,
        "msg_goal":                 {},
        "milestone_announce":       None,
        "milestone_thresholds":     [100, 500, 1000, 5000, 10000],
        "vc_milestone_thresholds":  [60, 300, 600, 1200],
        # Welcome system (multi-channel + embed toggle + auto-delete)
        "welcome_channels":         [],
        "welcome_message":          "Welcome to the server, {mention}! 🎉",
        "welcome_use_embed":        True,
        "welcome_delete_seconds":   0,
        "goodbye_channels":         [],
        "goodbye_message":          "**{name}** has left the server.",
        "goodbye_use_embed":        True,
        "goodbye_delete_seconds":   0,
        # Legacy single-channel keys (kept for backwards compat)
        "welcome_channel":          None,
        "goodbye_channel":          None,
        # ── DM Permissions (NEW)
        "dm_allowed_roles": [],
        # ── TICKET SYSTEM CONFIGURATION (NEW)
        "ticket_setup_channel":     None,           # Channel where ticket panel is posted
        "ticket_panel_message_id":  None,           # Message ID of the ticket panel
        "ticket_categories": {                       # Ticket categories configuration
            # Example: "Support": {"category_id": 123456789, "enabled": True}
        },
        "ticket_manager_roles":     [],             # Roles that can manage tickets
        "ticket_transcripts":       None,           # Channel for ticket transcripts
        "ticket_default_title":     "Support Ticket",
        "ticket_default_description": "Please describe your issue in detail. Our support team will assist you shortly.",
        "ticket_auto_close_minutes": 0,             # Auto-close after X minutes (0 = disabled)
        "role_salaries": {},
                # ===== NEW: Role Progression Announcement Settings =====
        "role_progress_channel": None,      # Channel ID for role progression announcements
        "role_progress_enabled": True,       # Whether announcements are enabled
        "role_announce_roles": [],           # Specific roles to ping (role IDs)
        # ===== END NEW SETTINGS =====
    }

def get_user_data(guild_data: dict, user_id: int) -> dict:
    uid = str(user_id)
    if uid not in guild_data["users"]:
        guild_data["users"][uid] = {
            "total": 0, "daily": {}, "weekly": {}, "monthly": {},
            "vc_total": 0, "vc_daily": {}, "vc_weekly": {}, "vc_monthly": {},
            "warnings": 0,
            "xp": 0, "xp_daily": {}, "xp_weekly": {}, "xp_monthly": {},
            "last_xp_time": 0.0,
            "streak": 0,
            "last_msg_date": "",
            "first_msg_date": "",
            "longest_streak": 0,
            "achieved_milestones": [],
            "vc_achieved_milestones": [],
        }
    u = guild_data["users"][uid]
    for k, v in [
        ("vc_total", 0), ("vc_daily", {}), ("vc_weekly", {}), ("vc_monthly", {}),
        ("warnings", 0),
        ("xp", 0), ("xp_daily", {}), ("xp_weekly", {}), ("xp_monthly", {}),
        ("last_xp_time", 0.0),
        ("streak", 0), ("last_msg_date", ""), ("first_msg_date", ""),
        ("longest_streak", 0), ("achieved_milestones", []),
        ("vc_achieved_milestones", []),
    ]:
        u.setdefault(k, v)
    return u

# Get IST timezone
IST = pytz.timezone('Asia/Kolkata')

def get_ist_now():
    """Get current time in IST"""
    return datetime.now(IST)

def today_key():
    """Get today's date in IST"""
    return get_ist_now().strftime("%Y-%m-%d")

def week_key():
    """Get week number in IST"""
    d = get_ist_now()
    return f"{d.year}-W{d.strftime('%W')}"

def month_key():
    """Get month in IST"""
    return get_ist_now().strftime("%Y-%m")

def get_start_of_day_ist():
    """Get today's 12:00 AM IST timestamp"""
    now_ist = get_ist_now()
    start = datetime(now_ist.year, now_ist.month, now_ist.day, 0, 0, 0, tzinfo=IST)
    return start.timestamp()

def increment_counts(udata: dict, amount: int = 1):
    udata["total"] = udata.get("total", 0) + amount
    # Use IST for daily keys
    daily_key = today_key()
    weekly_key = week_key()
    monthly_key = month_key()
    udata["daily"][daily_key] = udata["daily"].get(daily_key, 0) + amount
    udata["weekly"][weekly_key] = udata["weekly"].get(weekly_key, 0) + amount
    udata["monthly"][monthly_key] = udata["monthly"].get(monthly_key, 0) + amount

def increment_vc(udata: dict, minutes: float):
    m = round(minutes, 2)
    daily_key = today_key()
    weekly_key = week_key()
    monthly_key = month_key()
    udata["vc_total"] = round(udata.get("vc_total", 0) + m, 2)
    udata["vc_daily"][daily_key] = round(udata["vc_daily"].get(daily_key, 0) + m, 2)
    udata["vc_weekly"][weekly_key] = round(udata["vc_weekly"].get(weekly_key, 0) + m, 2)
    udata["vc_monthly"][monthly_key] = round(udata["vc_monthly"].get(monthly_key, 0) + m, 2)

def increment_xp(udata: dict, amount: int):
    daily_key = today_key()
    weekly_key = week_key()
    monthly_key = month_key()
    udata["xp"] = udata.get("xp", 0) + amount
    udata["xp_daily"][daily_key] = udata["xp_daily"].get(daily_key, 0) + amount
    udata["xp_weekly"][weekly_key] = udata["xp_weekly"].get(weekly_key, 0) + amount
    udata["xp_monthly"][monthly_key] = udata["xp_monthly"].get(monthly_key, 0) + amount

def update_streak(udata: dict) -> int:
    """Update daily streak using IST dates"""
    today = today_key()
    last = udata.get("last_msg_date", "")
    
    # Get yesterday in IST
    yesterday_ist = get_ist_now() - timedelta(days=1)
    yesterday = yesterday_ist.strftime("%Y-%m-%d")
    
    if last == today:
        return udata.get("streak", 0)
    
    if last == yesterday:
        udata["streak"] = udata.get("streak", 0) + 1
    else:
        udata["streak"] = 1
    
    udata["last_msg_date"] = today
    if not udata.get("first_msg_date"):
        udata["first_msg_date"] = today
    if udata["streak"] > udata.get("longest_streak", 0):
        udata["longest_streak"] = udata["streak"]
    return udata["streak"]

def is_owner(user: discord.Member, guild: discord.Guild, guild_data: dict) -> bool:
    return (
        user.id == guild.owner_id
        or user.guild_permissions.administrator
        or user.id in guild_data["settings"].get("owner_ids", [])
    )

def can_use_dm_commands(user: discord.Member, guild: discord.Guild, guild_data: dict) -> bool:
    """
    Check if user can use DM commands.
    !dm (single member) → server owner OR user with an allowed role
    !dmall / !dmrole    → server owner ONLY (enforced in each command)
    """
    # Actual Discord server owner always has access
    if user.id == guild.owner_id:
        return True
    # Bot-owner list also gets access to single-member DM
    if user.id in guild_data["settings"].get("owner_ids", []):
        return True
    # Check if user has any role that has been granted DM permission
    allowed_roles = guild_data["settings"].get("dm_allowed_roles", [])
    user_role_ids = [r.id for r in user.roles]
    for allowed_role_id in allowed_roles:
        if allowed_role_id in user_role_ids:
            return True
    return False

def is_actual_server_owner(user: discord.Member, guild: discord.Guild) -> bool:
    """
    Returns True ONLY for the actual Discord server owner.
    Used to gate the most dangerous commands (!dmall, !dmrole, !dmaddrole, !dmremoverole).
    """
    return user.id == guild.owner_id

# ── Spam / emoji helpers ───────────────────────────────────────────────────────
def is_emoji_only(content: str) -> bool:
    stripped = re.sub(r'<a?:\w+:\d+>', '', content.strip())
    stripped = re.sub(
        r'[\U0001F300-\U0001FAFF\U00002702-\U000027B0\U0001F000-\U0001F9FF'
        r'\U00002600-\U000026FF\U000024C2-\U0001F251]+', '', stripped, flags=re.UNICODE)
    return stripped.strip() == ''

def should_count_message(message: discord.Message, guild_data: dict) -> bool:
    s   = guild_data["settings"]
    cid = message.channel.id
    wl  = s.get("whitelisted_channels", [])
    bl  = s.get("blacklisted_channels", [])
    if wl and cid not in wl: return False
    if cid in bl:             return False
    if not s.get("count_emojis", True) and is_emoji_only(message.content): return False
    return True

def check_spam(guild_id: int, user_id: int, interval: float) -> bool:
    now = time.time()
    ts  = spam_tracker[guild_id][user_id]
    ts[:] = [t for t in ts if now - t < interval]
    first = len(ts) == 0
    ts.append(now)
    return first

# ── VC session persistence helpers ────────────────────────────────────────────
def save_vc_sessions(data: dict):
    """Persist active VC join times to disk so they survive restarts."""
    serializable = {}
    for gid, sessions in vc_sessions.items():
        serializable[str(gid)] = {str(uid): join_time for uid, join_time in sessions.items()}
    data["_vc_sessions"] = serializable

def restore_vc_sessions(data: dict):
    """Reload VC join times from disk after a restart."""
    raw = data.get("_vc_sessions", {})
    for gid_str, sessions in raw.items():
        for uid_str, join_time in sessions.items():
            vc_sessions[int(gid_str)][int(uid_str)] = join_time
    log.info(f"🔄 Restored {sum(len(s) for s in vc_sessions.values())} active VC session(s) from disk")

# ── Role assignment ────────────────────────────────────────────────────────────
def get_applicable_thresholds(member: discord.Member, guild_data: dict) -> dict:
    s    = guild_data["settings"]
    mids = {r.id for r in member.roles}
    
    # First check conditional paths
    for path in s.get("conditional_paths", []):
        tid = path.get("trigger_role_id")
        if tid and int(tid) in mids:
            return path.get("thresholds", {})
    
    # If no conditional path matches, return the default role thresholds
    # IMPORTANT: Always return role_thresholds, not empty dict!
    return s.get("role_thresholds", {})

async def assign_roles(guild: discord.Guild, member: discord.Member, udata: dict, gdata: dict):
    s          = gdata["settings"]
    total      = udata.get("total", 0)
    vc_total   = udata.get("vc_total", 0)
    include_vc = s.get("include_vc_in_roles", False)
    if member.id in s.get("blacklisted_members", []): return
    effective  = total + (int(vc_total) if include_vc else 0)
    thresholds = get_applicable_thresholds(member, gdata)
    
    # Track newly assigned roles for announcement
    newly_assigned_roles = []
    
    for rid_str, req in thresholds.items():
        role = guild.get_role(int(rid_str))
        if role and effective >= req and role not in member.roles:
            try:
                await member.add_roles(role, reason=f"Reached {req} messages")
                log.info(f"[Role] {role.name} → {member}")
                newly_assigned_roles.append(role)
            except discord.Forbidden: 
                pass
    
    for rid_str, req_min in s.get("vc_role_thresholds", {}).items():
        role = guild.get_role(int(rid_str))
        if role and vc_total >= req_min and role not in member.roles:
            try:
                await member.add_roles(role, reason=f"Reached {req_min} VC min")
                log.info(f"[VC Role] {role.name} → {member}")
                newly_assigned_roles.append(role)
            except: 
                pass
    
    # Send announcements for newly assigned roles
    if newly_assigned_roles:
        await announce_role_achievement(guild, member, newly_assigned_roles[0], udata, gdata)

def get_next_milestone(member: discord.Member, total: int, gdata: dict):
    t = get_applicable_thresholds(member, gdata)
    upcoming = [(req, member.guild.get_role(int(rid))) for rid, req in t.items() if req > total]
    if not upcoming: return None, None
    upcoming.sort(key=lambda x: x[0])
    needed, role = upcoming[0]
    return needed - total, role

def get_next_vc_milestone(member: discord.Member, vc_total: float, gdata: dict):
    t = gdata["settings"].get("vc_role_thresholds", {})
    upcoming = [(req, member.guild.get_role(int(rid))) for rid, req in t.items() if req > vc_total]
    if not upcoming: return None, None
    upcoming.sort(key=lambda x: x[0])
    needed, role = upcoming[0]
    return round(needed - vc_total, 1), role

def get_rank_position(guild: discord.Guild, gdata: dict, user_id: int) -> int:
    scores = sorted(gdata["users"].items(), key=lambda x: x[1].get("total", 0), reverse=True)
    for i, (uid, _) in enumerate(scores, 1):
        if int(uid) == user_id: return i
    return len(scores) + 1

def get_xp_rank_position(guild: discord.Guild, gdata: dict, user_id: int) -> int:
    scores = sorted(gdata["users"].items(), key=lambda x: x[1].get("xp", 0), reverse=True)
    for i, (uid, _) in enumerate(scores, 1):
        if int(uid) == user_id: return i
    return len(scores) + 1

def get_vc_rank_position(guild: discord.Guild, gdata: dict, user_id: int) -> int:
    scores = sorted(gdata["users"].items(), key=lambda x: x[1].get("vc_total", 0), reverse=True)
    for i, (uid, _) in enumerate(scores, 1):
        if int(uid) == user_id: return i
    return len(scores) + 1

async def announce_role_achievement(guild: discord.Guild, member: discord.Member, achieved_role: discord.Role, udata: dict, gdata: dict):
    """Send a beautiful role achievement announcement to the configured channel"""
    s = gdata["settings"]
    
    # Check if announcements are enabled
    if not s.get("role_progress_enabled", True):
        return
    
    # Get the announcement channel
    channel_id = s.get("role_progress_channel")
    if not channel_id:
        return
    
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    
    try:
        # Generate the achievement card
        card = await generate_role_achievement_card(member, achieved_role, udata, gdata)
        
        # Get roles to ping
        announce_roles = s.get("role_announce_roles", [])
        ping_text = ""
        if announce_roles:
            ping_mentions = []
            for role_id in announce_roles:
                role = guild.get_role(role_id)
                if role:
                    ping_mentions.append(role.mention)
            if ping_mentions:
                ping_text = " ".join(ping_mentions)
        
        # Send the announcement
        await channel.send(content=ping_text, file=card)
        log.info(f"Role achievement announced: {member} earned {achieved_role.name} in {guild.name}")
        
    except Exception as e:
        log.error(f"Failed to send role achievement announcement: {e}")

# ── Milestone announcer ────────────────────────────────────────────────────────
async def check_and_announce_milestones(
    guild: discord.Guild,
    member: discord.Member,
    udata: dict,
    gdata: dict,
):
    s     = gdata["settings"]
    ch_id = s.get("milestone_announce")
    if not ch_id: return
    ch = guild.get_channel(int(ch_id))
    if not ch: return
    total    = udata.get("total", 0)
    vc_total = udata.get("vc_total", 0)
    for ms in s.get("milestone_thresholds", [100, 500, 1000, 5000, 10000]):
        achieved = udata.setdefault("achieved_milestones", [])
        if total >= ms and ms not in achieved:
            achieved.append(ms)
            embed = discord.Embed(
                title="🎉 Milestone Reached!",
                description=f"🏆 {member.mention} has sent **{ms:,} messages**!\nThat's an amazing achievement. Keep it up!",
                color=0xF1C40F,
                timestamp=datetime.utcnow(),
            )
            embed.set_thumbnail(url=member.display_avatar.url)
            embed.set_footer(text=f"Total messages: {total:,}")
            try: await ch.send(embed=embed)
            except: pass
    for ms in s.get("vc_milestone_thresholds", [60, 300, 600, 1200]):
        vc_achieved = udata.setdefault("vc_achieved_milestones", [])
        if vc_total >= ms and ms not in vc_achieved:
            vc_achieved.append(ms)
            h = ms // 60; m_ = ms % 60
            time_str = f"{h}h {m_}m" if h else f"{m_}m"
            embed = discord.Embed(
                title="🎙 Voice Milestone!",
                description=f"🔊 {member.mention} has spent **{time_str}** in voice channels!\nWhat a dedicated community member!",
                color=0x5865F2,
                timestamp=datetime.utcnow(),
            )
            embed.set_thumbnail(url=member.display_avatar.url)
            try: await ch.send(embed=embed)
            except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  RANK CARD GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    suffix = "-Bold" if bold else ""
    # Try different font paths for better compatibility
    font_paths = [
        f"/usr/share/fonts/truetype/liberation/LiberationSans{suffix}.ttf",
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        "/System/Library/Fonts/Helvetica.ttc",  # macOS
        "C:/Windows/Fonts/Arial.ttf",           # Windows
    ]
    for path in font_paths:
        try:
            return ImageFont.truetype(path, size)
        except:
            pass
    return ImageFont.load_default()

def _lerp(c1, c2, t):
    return tuple(int(a + (b - a) * t) for a, b in zip(c1, c2))

def _progress_color(pct: float):
    if pct < 0.5: return _lerp((110, 20, 220), (230, 30, 160), pct * 2)
    return _lerp((230, 30, 160), (255, 200, 30), (pct - 0.5) * 2)

def _draw_bar(draw, x1, y, x2, h, r, pct, c1, c2):
    draw.rounded_rectangle([x1, y, x2, y + h], radius=r, fill=(30, 20, 55, 200))
    fw = max(int((x2 - x1) * pct), 0)
    if fw > r * 2:
        for i in range(fw):
            t   = i / max(fw - 1, 1)
            col = _lerp(c1, c2, t)
            draw.line([(x1 + i, y + 1), (x1 + i, y + h - 1)], fill=(*col, 220))
        draw.rounded_rectangle([x1, y, x1 + fw, y + h], radius=r, fill=None, outline=(*c2, 155), width=1)
    return fw

async def fetch_avatar(url: str) -> Image.Image:
    async with aiohttp.ClientSession() as s:
        async with s.get(str(url)) as r:
            data = await r.read()
    return Image.open(io.BytesIO(data)).convert("RGBA")

def _make_circle(img: Image.Image, size: int) -> Image.Image:
    img  = img.resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    img.putalpha(mask)
    return img

# ──────────────────────────────────────────────────────────────────────────────
# 1. RANK CARD  (replaces generate_rank_card)
# ──────────────────────────────────────────────────────────────────────────────
import io, math, time, random
import discord
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime

# Paste your existing helpers above this point in the real file:
# _get_font, _lerp, fetch_avatar, _make_circle,
# get_applicable_thresholds, get_next_milestone, get_next_vc_milestone,
# get_rank_position, get_user_data, today_key, week_key, month_key

async def generate_rank_card(member: discord.Member, udata: dict, gdata: dict, rank_pos: int) -> discord.File:
    """
    Redesigned rank card:
      • Dark purple gradient background with glow blobs
      • Gold-ringed avatar with green online dot
      • Earned roles + salaries below name
      • 3-column stat strip (messages, voice, XP)
      • Gradient progress bar with milestone labels
      • Next-role chip at the bottom
      • Streak row footer
    """
    W, H = 680, 440

    # ── Palette ────────────────────────────────────────────────────────────
    BG1        = (15, 13, 26)
    BG2        = (26, 23, 48)
    CARD       = (30, 27, 51)
    CARD_B     = (60, 50, 110)
    GOLD       = (245, 158, 11)
    GOLD_L     = (252, 211, 77)
    WHITE      = (232, 227, 255)
    MUTED      = (155, 147, 204)
    DIM        = (90, 82, 130)
    CYAN       = (6, 182, 212)
    CYAN_D     = (8, 145, 178)
    PURPLE     = (124, 58, 237)
    PURPLE_L   = (157, 111, 245)
    GREEN      = (16, 185, 129)
    ORANGE     = (249, 115, 22)

    # ── Canvas ─────────────────────────────────────────────────────────────
    img  = Image.new("RGBA", (W, H), BG1)
    draw = ImageDraw.Draw(img)

    # Gradient background
    for y in range(H):
        t = y / H
        r = int(BG1[0] + (BG2[0] - BG1[0]) * t)
        g = int(BG1[1] + (BG2[1] - BG1[1]) * t)
        b = int(BG1[2] + (BG2[2] - BG1[2]) * t)
        draw.line([(0, y), (W, y)], fill=(r, g, b, 255))

    # Glow blobs
    from PIL import Image as PILImage
    glow = PILImage.new("RGBA", (W, H), (0, 0, 0, 0))
    gd   = ImageDraw.Draw(glow)
    gd.ellipse((-80, -60, 320, 260), fill=(*PURPLE, 28))
    gd.ellipse((460, -40, 820, 280), fill=(*CYAN, 18))
    img = PILImage.alpha_composite(img, glow)
    draw = ImageDraw.Draw(img)

    # Card border
    draw.rounded_rectangle([6, 6, W-6, H-6], radius=18,
                            fill=None, outline=(*CARD_B, 100), width=1)

    # ── Fonts ──────────────────────────────────────────────────────────────
    # (using your existing _get_font helper)
    f_tiny   = _get_font(10, bold=True)
    f_small  = _get_font(13, bold=False)
    f_label  = _get_font(13, bold=True)
    f_body   = _get_font(15, bold=False)
    f_name   = _get_font(24, bold=True)
    f_role   = _get_font(16, bold=False)
    f_stat_n = _get_font(22, bold=True)
    f_stat_l = _get_font(11, bold=True)
    f_bar    = _get_font(13, bold=True)
    f_head   = _get_font(11, bold=True)

    PAD = 26

    # ── Header strip ───────────────────────────────────────────────────────
    for y in range(0, 8):
        alpha = int(180 * (1 - y/8))
        draw.line([(6, y+6), (W-6, y+6)], fill=(*PURPLE, alpha))

    # "ROLE PROGRESSION" label
    header_txt = "ROLE PROGRESSION"
    hbbox = draw.textbbox((0, 0), header_txt, font=f_head)
    hw = hbbox[2] - hbbox[0]
    draw.text(((W - hw) // 2, 18), header_txt, font=f_head, fill=(*PURPLE_L, 200))

    # Rank badge (top-right)
    badge_txt = f"Rank #{rank_pos}"
    bbbox = draw.textbbox((0, 0), badge_txt, font=f_label)
    bw = bbbox[2] - bbbox[0]; bh = bbbox[3] - bbbox[1]
    bx = W - PAD - bw - 16; by = 14
    draw.rounded_rectangle([bx-8, by-4, bx+bw+8, by+bh+4],
                            radius=12, fill=(*GOLD, 30), outline=(*GOLD, 120), width=1)
    draw.text((bx, by), badge_txt, font=f_label, fill=GOLD)

    # ── Avatar ─────────────────────────────────────────────────────────────
    AV   = 84
    AX   = PAD
    AY   = 46
    RING = 4

    # Outer glow ring
    for i in range(4, 0, -1):
        alpha = 60 - i * 12
        draw.ellipse([AX-RING-i, AY-RING-i, AX+AV+RING+i, AY+AV+RING+i],
                     fill=None, outline=(*GOLD, alpha), width=1)

    # Gold ring
    draw.ellipse([AX-RING, AY-RING, AX+AV+RING, AY+AV+RING],
                 fill=None, outline=GOLD, width=2)

    try:
        av_img    = await fetch_avatar(member.display_avatar.with_format("png").with_size(256).url)
        av_circle = _make_circle(av_img, AV)
        img.paste(av_circle, (AX, AY), av_circle)
        draw = ImageDraw.Draw(img)
    except Exception:
        draw.ellipse([AX, AY, AX+AV, AY+AV], fill=(*PURPLE, 200))

    # Online dot
    DOT_R = 8
    draw.ellipse([AX+AV-DOT_R, AY+AV-DOT_R, AX+AV+DOT_R, AY+AV+DOT_R],
                 fill=(*GREEN, 255))
    draw.ellipse([AX+AV-DOT_R+2, AY+AV-DOT_R+2, AX+AV+DOT_R-2, AY+AV+DOT_R-2],
                 fill=(*GREEN, 255))

    # ── Name & roles ───────────────────────────────────────────────────────
    TX = AX + AV + 18
    TY = AY + 4
    draw.text((TX, TY), member.display_name, font=f_name, fill=WHITE)
    name_h = draw.textbbox((0,0), member.display_name, font=f_name)[3] + 6

    s_settings    = gdata["settings"]
    role_salaries = s_settings.get("role_salaries", {})
    mids          = {r.id for r in member.roles}
    thresholds    = get_applicable_thresholds(member, gdata)
    earned_roles  = []
    for rid_str, req in sorted(thresholds.items(), key=lambda x: x[1]):
        rid = int(rid_str)
        if rid in mids:
            role = member.guild.get_role(rid)
            if role:
                earned_roles.append(role)

    role_y = TY + name_h
    if earned_roles:
        for role in earned_roles[-2:]:
            # Coloured role chip background
            rbbox = draw.textbbox((0,0), role.name, font=f_role)
            rw = rbbox[2]-rbbox[0]; rh = rbbox[3]-rbbox[1]
            draw.rounded_rectangle([TX-1, role_y-3, TX+rw+12, role_y+rh+3],
                                   radius=8, fill=(*GOLD, 22), outline=(*GOLD, 80), width=1)
            draw.text((TX+6, role_y), role.name, font=f_role, fill=GOLD_L)
            role_y += rh + 10

            salary = role_salaries.get(str(role.id))
            if salary:
                draw.text((TX+6, role_y), f"Salary: {salary}", font=f_small, fill=(*GOLD, 180))
                salary_h = draw.textbbox((0,0), f"Salary: {salary}", font=f_small)[3]
                role_y += salary_h + 6
    else:
        draw.text((TX, role_y), "No roles earned yet", font=f_role, fill=(*MUTED, 160))

    # ── Stat strip ─────────────────────────────────────────────────────────
    total   = udata.get("total", 0)
    vc_t    = udata.get("vc_total", 0)
    xp      = udata.get("xp", 0)
    streak  = udata.get("streak", 0)
    longest = udata.get("longest_streak", 0)

    vc_h = int(vc_t // 60); vc_m = int(vc_t % 60)
    vc_str = f"{vc_h}h {vc_m}m" if vc_h else f"{vc_m}m"

    STAT_Y   = AY + AV + 20
    STAT_W   = (W - PAD * 2 - 20) // 3
    STAT_H   = 62
    STAT_GAP = 10

    stats = [
        (f"{total:,}",   "MESSAGES"),
        (vc_str,          "VOICE TIME"),
        (f"{xp:,}",       "TOTAL XP"),
    ]

    for i, (val, lbl) in enumerate(stats):
        sx = PAD + i * (STAT_W + STAT_GAP)
        # Card background
        draw.rounded_rectangle([sx, STAT_Y, sx+STAT_W, STAT_Y+STAT_H],
                                radius=10, fill=(*CARD, 200), outline=(*CARD_B, 80), width=1)
        # Value
        vbbox = draw.textbbox((0,0), val, font=f_stat_n)
        vw = vbbox[2]-vbbox[0]
        draw.text((sx + (STAT_W-vw)//2, STAT_Y+10), val, font=f_stat_n, fill=WHITE)
        # Label
        lbbox = draw.textbbox((0,0), lbl, font=f_stat_l)
        lw = lbbox[2]-lbbox[0]
        draw.text((sx + (STAT_W-lw)//2, STAT_Y+38), lbl, font=f_stat_l, fill=(*MUTED, 180))

    # ── Progress bar ───────────────────────────────────────────────────────
    PRG_Y = STAT_Y + STAT_H + 18
    BAR_X1 = PAD
    BAR_X2 = W - PAD
    BAR_H  = 14
    BAR_R  = 7

    # Next role calculation
    thresholds_sorted = sorted(thresholds.items(), key=lambda x: x[1])
    prev_t  = 0
    next_t  = None
    next_role = None
    for rid_str, req in thresholds_sorted:
        if req > total:
            next_t    = req
            next_role = member.guild.get_role(int(rid_str))
            break
        prev_t = req

    pct = min((total - prev_t) / max((next_t or total+1) - prev_t, 1), 1.0) if next_t else 1.0

    # Label row above bar
    left_lbl  = f"{total:,} msgs"
    right_lbl = f"{next_t:,}" if next_t else "MAX"
    draw.text((BAR_X1, PRG_Y), left_lbl, font=f_small, fill=(*DIM, 200))
    rbbox = draw.textbbox((0,0), right_lbl, font=f_small)
    rw = rbbox[2]-rbbox[0]
    draw.text((BAR_X2-rw, PRG_Y), right_lbl, font=f_small, fill=(*DIM, 200))

    BAR_Y = PRG_Y + 20
    draw.rounded_rectangle([BAR_X1, BAR_Y, BAR_X2, BAR_Y+BAR_H], radius=BAR_R, fill=(*CARD_B, 120))

    fill_w = max(int((BAR_X2 - BAR_X1) * pct), BAR_R * 2 if pct > 0.03 else 0)
    if fill_w > 0:
        for i in range(fill_w):
            t  = i / max(fill_w - 1, 1)
            col = (
                int(PURPLE[0] + (CYAN[0] - PURPLE[0]) * t),
                int(PURPLE[1] + (CYAN[1] - PURPLE[1]) * t),
                int(PURPLE[2] + (CYAN[2] - PURPLE[2]) * t),
            )
            draw.line([(BAR_X1+i, BAR_Y+1), (BAR_X1+i, BAR_Y+BAR_H-1)], fill=(*col, 220))

        # Shimmer line
        sx = BAR_X1 + fill_w - 2
        draw.line([(sx, BAR_Y+1), (sx, BAR_Y+BAR_H-1)], fill=(255, 255, 255, 120), width=2)

    # Percent in centre of bar
    pct_str = f"{pct*100:.1f}%"
    pbbox   = draw.textbbox((0,0), pct_str, font=f_bar)
    pw = pbbox[2]-pbbox[0]; ph = pbbox[3]-pbbox[1]
    draw.text(((BAR_X1+BAR_X2-pw)//2, BAR_Y+(BAR_H-ph)//2 + 1), pct_str, font=f_bar, fill=WHITE)

    # ── Next-role chip ─────────────────────────────────────────────────────
    CHIP_Y = BAR_Y + BAR_H + 14
    chip_h = 46

    draw.rounded_rectangle([BAR_X1, CHIP_Y, BAR_X2, CHIP_Y+chip_h],
                            radius=10, fill=(*PURPLE, 30), outline=(*PURPLE, 80), width=1)

    if next_role:
        msgs_needed = next_t - total
        left_part  = f"Next: {next_role.name}"
        right_part = f"{msgs_needed:,} msgs to go"
        draw.text((BAR_X1 + 14, CHIP_Y + 8),  left_part,  font=f_label, fill=PURPLE_L)
        draw.text((BAR_X1 + 14, CHIP_Y + 26), right_part, font=f_small, fill=(*MUTED, 180))
        # Salary
        salary = role_salaries.get(str(next_role.id))
        if salary:
            sal_str = f"Salary: {salary}"
            sbbox   = draw.textbbox((0,0), sal_str, font=f_label)
            sw      = sbbox[2]-sbbox[0]
            draw.text((BAR_X2 - sw - 14, CHIP_Y + 14), sal_str, font=f_label, fill=(*GOLD, 220))
    else:
        max_txt = "✨  Maximum rank achieved!"
        mbbox   = draw.textbbox((0,0), max_txt, font=f_label)
        mw      = mbbox[2]-mbbox[0]
        draw.text(((W-mw)//2, CHIP_Y + 14), max_txt, font=f_label, fill=GOLD_L)

    # ── Streak footer ──────────────────────────────────────────────────────
    FOOTER_Y = CHIP_Y + chip_h + 14
    sep_col  = (*CARD_B, 60)
    draw.line([(PAD, FOOTER_Y), (W-PAD, FOOTER_Y)], fill=sep_col, width=1)

    FOOT_TY = FOOTER_Y + 10

    streak_color = ORANGE if streak >= 7 else MUTED
    parts = [
        (f"🔥  {streak} day streak", streak_color),
        ("   •   ", DIM),
        (f"🏆  Longest: {longest}d", MUTED),
    ]
    cx = PAD
    for txt, col in parts:
        draw.text((cx, FOOT_TY), txt, font=f_small, fill=col)
        w_ = draw.textbbox((0,0), txt, font=f_small)[2]
        cx += w_

    # ── Corner accents ─────────────────────────────────────────────────────
    CS = 16
    for pts in [
        [(PAD-6, FOOT_TY+CS+12), (PAD-6, FOOT_TY+12), (PAD-6+CS, FOOT_TY+12)],
        [(W-PAD+6, FOOT_TY+CS+12), (W-PAD+6, FOOT_TY+12), (W-PAD+6-CS, FOOT_TY+12)],
    ]:
        draw.line(pts, fill=(*GOLD, 80), width=1)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="rank_card.png")


def generate_banner() -> discord.File:
    W, H = 950, 200
    img  = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    for y in range(H):
        t = y / H
        draw.line([(0, y), (W, y)], fill=(int(8+6*t), int(5+4*t), int(20+15*t), 255))
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd   = ImageDraw.Draw(glow)
    gd.ellipse((-80, -60, 350, 220), fill=(80, 10, 200, 30))
    gd.ellipse((600, -40, 1050, 240), fill=(180, 20, 100, 22))
    img  = Image.alpha_composite(img, glow)
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([3, 3, W-3, H-3], radius=18, fill=None, outline=(100, 50, 200, 130), width=2)

    f_title = _get_font(90, True)
    f_sub   = _get_font(18)
    text    = "CHILL PILL"
    bbox    = draw.textbbox((0, 0), text, font=f_title)
    tw      = bbox[2] - bbox[0]; th = bbox[3] - bbox[1]
    tx, ty  = (W - tw) // 2, (H - th) // 2 - 10

    txt_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    td        = ImageDraw.Draw(txt_layer)
    td.text((tx+3, ty+5), text, font=f_title, fill=(0, 0, 0, 120))
    cx = tx
    for i, ch in enumerate(text):
        t   = i / max(len(text)-1, 1)
        col = _lerp((140,40,255),(230,40,160),t*2) if t < 0.5 else _lerp((230,40,160),(255,210,50),(t-0.5)*2)
        td.text((cx, ty), ch, font=f_title, fill=(*col, 255))
        cb = td.textbbox((cx, ty), ch, font=f_title)
        cx += cb[2] - cb[0]
    img  = Image.alpha_composite(img, txt_layer)
    draw = ImageDraw.Draw(img)

    sub = "Your server's rank & activity tracker"
    sb  = draw.textbbox((0, 0), sub, font=f_sub)
    draw.text(((W-(sb[2]-sb[0]))//2, ty+th+6), sub, font=f_sub, fill=(180, 160, 240, 180))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="chillpill_banner.png")

# ──────────────────────────────────────────────────────────────────────────────
# 2. ACHIEVEMENT CARD  (replaces generate_role_achievement_card)
# ──────────────────────────────────────────────────────────────────────────────
async def generate_role_achievement_card(
    member: discord.Member,
    achieved_role: discord.Role,
    udata: dict,
    gdata: dict,
) -> discord.File:
    """
    Redesigned achievement card:
      • Deep purple banner header with gold gradient title
      • Avatar with gold ring in header corner
      • Stats row: messages / voice / XP bonus
      • Gold divider
      • Large gold role name + progress badge
      • Purple footer bar
    """
    W, H  = 800, 380
    PAD   = 28

    PURPLE  = (124, 58, 237)
    PURPLE_D= (79, 30, 179)
    GOLD    = (245, 158, 11)
    GOLD_L  = (252, 211, 77)
    WHITE   = (232, 227, 255)
    MUTED   = (155, 147, 204)
    DIM     = (90,  82, 130)
    CYAN    = (6, 182, 212)

    img  = Image.new("RGBA", (W, H), (15, 13, 26, 255))
    draw = ImageDraw.Draw(img)

    # Gradient background
    for y in range(H):
        t = y / H
        r = int(15 + 12*t); g = int(13 + 10*t); b = int(26 + 18*t)
        draw.line([(0, y), (W, y)], fill=(r, g, b, 255))


    # ── Banner header (top 90px) ───────────────────────────────────────────
    BANNER_H = 90
    for y in range(BANNER_H):
        t   = y / BANNER_H
        r   = int(PURPLE_D[0] + (PURPLE[0]-PURPLE_D[0]) * t)
        g   = int(PURPLE_D[1] + (PURPLE[1]-PURPLE_D[1]) * t)
        b   = int(PURPLE_D[2] + (PURPLE[2]-PURPLE_D[2]) * t)
        draw.line([(0, y), (W, y)], fill=(r, g, b, 200))

    # Gold bottom edge of banner
    for i in range(2):
        draw.line([(0, BANNER_H-i), (W, BANNER_H-i)],
                  fill=(*GOLD, 120 - i*60), width=1)

    # Banner text
    f_banner_sm  = _get_font(10, bold=True)
    f_banner_big = _get_font(32, bold=True)
    f_stat_v     = _get_font(28, bold=True)
    f_stat_l     = _get_font(10, bold=True)
    f_role_lbl   = _get_font(10, bold=True)
    f_role_name  = _get_font(34, bold=True)
    f_progress   = _get_font(40, bold=True)
    f_mention    = _get_font(12, bold=False)
    f_footer     = _get_font(11, bold=False)

    draw.text((PAD, 14), "ROLE PROGRESSION", font=f_banner_sm,
              fill=(200, 200, 220, 200))
    draw.text((PAD, 34), "ROLE UNLOCKED", font=f_banner_big, fill=GOLD_L)

    # Avatar in header (right side)
    AV   = 70
    AX   = W - PAD - AV
    AY   = 10
    RING = 3
    draw.ellipse([AX-RING, AY-RING, AX+AV+RING, AY+AV+RING],
                 fill=None, outline=GOLD, width=2)
    try:
        av_img    = await fetch_avatar(member.display_avatar.with_format("png").with_size(128).url)
        av_circle = _make_circle(av_img, AV)
        img.paste(av_circle, (AX, AY), av_circle)
        draw = ImageDraw.Draw(img)
    except Exception:
        draw.ellipse([AX, AY, AX+AV, AY+AV], fill=(*PURPLE, 200))

    # Username below avatar
    u_name = f"@{member.name}"
    ubbox  = draw.textbbox((0,0), u_name, font=f_mention)
    uw     = ubbox[2]-ubbox[0]
    draw.text((AX + (AV-uw)//2, AY+AV+5), u_name, font=f_mention, fill=(*MUTED, 200))

    # ── Stats row ──────────────────────────────────────────────────────────
    if udata is None:
        udata = get_user_data(gdata, member.id)

    total  = udata.get("total", 0)
    vc_t   = udata.get("vc_total", 0.0)
    xp     = udata.get("xp", 0)
    vc_h   = int(vc_t // 60); vc_m = int(vc_t % 60)
    vc_str = f"{vc_h}h" if vc_h else (f"{vc_m}m" if vc_m else "0m")
    bonus_str = f"+{xp//100}K" if xp >= 1000 else f"+{xp}"

    STAT_Y = BANNER_H + 18
    for i, (val, lbl) in enumerate([
        (f"{total:,}", "MESSAGES"),
        (vc_str,        "VOICE"),
        (bonus_str,     "XP BONUS"),
    ]):
        sx = PAD + i * 170
        col = GOLD_L if i == 2 else WHITE
        draw.text((sx, STAT_Y),    val, font=f_stat_v, fill=col)
        draw.text((sx, STAT_Y+36), lbl, font=f_stat_l, fill=(*MUTED, 180))

    # ── Divider ────────────────────────────────────────────────────────────
    DIV_Y = STAT_Y + 62
    for x in range(W):
        alpha = int(120 * math.sin(math.pi * x / W))
        draw.point((x, DIV_Y), fill=(*GOLD, alpha))

    # ── Role section ───────────────────────────────────────────────────────
    ROLE_Y = DIV_Y + 16

    # Left: role achieved
    draw.text((PAD, ROLE_Y), "ROLE ACHIEVED", font=f_role_lbl, fill=(*MUTED, 200))
    draw.text((PAD, ROLE_Y+22), achieved_role.name, font=f_role_name, fill=GOLD_L)
    draw.text((PAD, ROLE_Y+66), f"@{member.name}", font=f_mention, fill=(*MUTED, 180))

    # Right: progress
    thresholds    = get_applicable_thresholds(member, gdata)
    role_list     = sorted([(int(rid), req) for rid, req in thresholds.items()], key=lambda x: x[1])
    total_roles   = len(role_list)
    current_index = 0
    for i, (role_id, req) in enumerate(role_list, 1):
        if role_id == achieved_role.id:
            current_index = i
            break

    prog_str  = f"{current_index}/{total_roles}"
    PROG_X    = W - PAD - 140
    draw.text((PROG_X, ROLE_Y), "PROGRESS", font=f_role_lbl,
              fill=(*MUTED, 200), anchor="ma")
    pbbox = draw.textbbox((0,0), prog_str, font=f_progress)
    pw = pbbox[2]-pbbox[0]
    draw.text((PROG_X - pw//2, ROLE_Y+20), prog_str, font=f_progress, fill=GOLD_L)

    # ── Footer ─────────────────────────────────────────────────────────────
    FOOT_Y = H - 32
    draw.rectangle([0, FOOT_Y, W, H], fill=(*PURPLE, 50))
    draw.line([(0, FOOT_Y), (W, FOOT_Y)], fill=(*GOLD, 40), width=1)

    foot_txt = f"@{member.name} achieved the {achieved_role.name} role! ({current_index}/{total_roles} roles completed)"
    ftbbox   = draw.textbbox((0,0), foot_txt, font=f_footer)
    fw       = ftbbox[2]-ftbbox[0]
    draw.text(((W-fw)//2, FOOT_Y + 9), foot_txt, font=f_footer, fill=(*MUTED, 180))

    # Outer border
    draw.rounded_rectangle([2, 2, W-2, H-2], radius=14,
                            fill=None, outline=(*PURPLE, 80), width=1)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="role_achievement.png")

# ══════════════════════════════════════════════════════════════════════════════
#  LEADERBOARD / STATS HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def build_leaderboard(guild: discord.Guild, gdata: dict, period: str, top: int = 10, mode: str = "msg"):
    # Make sure these use IST dates
    key_fn = {"daily": today_key, "weekly": week_key, "monthly": month_key}
    scores = []
    for uid_str, udata in gdata["users"].items():
        if mode == "msg":
            count = udata.get("total", 0) if period == "total" else udata.get(period, {}).get(key_fn[period](), 0)
        elif mode == "vc":
            count = udata.get("vc_total", 0) if period == "total" else udata.get(f"vc_{period}", {}).get(key_fn[period](), 0)
        elif mode == "xp":
            count = udata.get("xp", 0) if period == "total" else udata.get(f"xp_{period}", {}).get(key_fn.get(period, lambda: "")(), 0)
        elif mode == "streak":
            count = udata.get("streak", 0)
        else:
            count = 0
        if count == 0: continue
        m = guild.get_member(int(uid_str))
        if m: scores.append((m, count))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top]

# ──────────────────────────────────────────────────────────────────────────────
# 3. LEADERBOARD EMBED  (replaces leaderboard_embed)
# ──────────────────────────────────────────────────────────────────────────────
BOT_COLOR = 0x7C3AED   # same as main file

def leaderboard_embed(scores, period: str, guild: discord.Guild, mode: str = "msg") -> discord.Embed:
    period_lbl = {"total": "All Time", "daily": "Today", "weekly": "This Week", "monthly": "This Month"}
    mode_icons = {"msg": "📊", "vc": "🎙️", "xp": "✨", "streak": "🔥"}
    mode_names = {"msg": "Messages", "vc": "Voice Time", "xp": "XP", "streak": "Streak"}
    unit_map   = {"msg": "msgs", "vc": "min", "xp": "XP", "streak": "days"}

    title = f"{mode_icons.get(mode,'📊')} {mode_names.get(mode,'Stats')} Leaderboard — {period_lbl.get(period,'All Time')}"
    embed = discord.Embed(title=title, color=BOT_COLOR, timestamp=datetime.utcnow())

    if not scores:
        embed.description = "*No data yet. Start chatting to appear here!*"
    else:
        unit   = unit_map.get(mode, "pts")
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines  = []
        for i, (m, c) in enumerate(scores, 1):
            medal = medals.get(i, f"`{i:>2}.`")
            # Highlight top 3 with bold score
            if i <= 3:
                lines.append(f"{medal} **{m.display_name}** — **{c:,}** {unit}")
            else:
                lines.append(f"{medal} {m.display_name} — {c:,} {unit}")
        embed.description = "\n".join(lines)

    embed.set_footer(
        text=f"{guild.name}  •  Use !lb msg/vc/xp/streak for different modes",
        icon_url=guild.icon.url if guild.icon else None,
    )
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    return embed

# ──────────────────────────────────────────────────────────────────────────────
# 4. STATS EMBED  (replaces stats_embed)
# ──────────────────────────────────────────────────────────────────────────────
def stats_embed(member: discord.Member, udata: dict, gdata: dict, guild: discord.Guild) -> discord.Embed:
    # Helpers assumed imported from main file
    from datetime import datetime

    def today_key():
        import pytz
        from datetime import datetime
        IST = pytz.timezone('Asia/Kolkata')
        return datetime.now(IST).strftime("%Y-%m-%d")
    def week_key():
        import pytz
        from datetime import datetime
        IST = pytz.timezone('Asia/Kolkata')
        d = datetime.now(IST)
        return f"{d.year}-W{d.strftime('%W')}"
    def month_key():
        import pytz
        from datetime import datetime
        IST = pytz.timezone('Asia/Kolkata')
        return datetime.now(IST).strftime("%Y-%m")

    daily   = udata["daily"].get(today_key(), 0)
    weekly  = udata["weekly"].get(week_key(), 0)
    monthly = udata["monthly"].get(month_key(), 0)
    total   = udata.get("total", 0)
    vc_d    = udata.get("vc_daily", {}).get(today_key(), 0)
    vc_w    = udata.get("vc_weekly", {}).get(week_key(), 0)
    vc_m    = udata.get("vc_monthly", {}).get(month_key(), 0)
    vc_t    = udata.get("vc_total", 0)
    warns   = udata.get("warnings", 0)
    xp      = udata.get("xp", 0)
    streak  = udata.get("streak", 0)
    longest = udata.get("longest_streak", 0)
    first   = udata.get("first_msg_date", "Unknown")
    xp_d    = udata.get("xp_daily", {}).get(today_key(), 0)
    xp_w    = udata.get("xp_weekly", {}).get(week_key(), 0)
    xp_m    = udata.get("xp_monthly", {}).get(month_key(), 0)

    rank_pos    = get_rank_position(guild, gdata, member.id)
    vc_rank_pos = get_vc_rank_position(guild, gdata, member.id)
    xp_rank_pos = get_xp_rank_position(guild, gdata, member.id)
    vc_h = int(vc_t // 60); vc_mn = int(vc_t % 60)

    color = member.color if member.color.value else BOT_COLOR
    embed = discord.Embed(
        title=f"📈  Stats — {member.display_name}",
        color=color,
        timestamp=datetime.utcnow(),
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    if guild.icon:
        embed.set_author(name=guild.name, icon_url=guild.icon.url)

    # Messages block
    embed.add_field(
        name="💬  Messages",
        value=(
            f"Today   **{daily:,}**\n"
            f"Week    **{weekly:,}**\n"
            f"Month   **{monthly:,}**\n"
            f"Total   **{total:,}**\n"
            f"Rank    **#{rank_pos}**"
        ),
        inline=True,
    )

    # Voice block
    embed.add_field(
        name="🎙️  Voice",
        value=(
            f"Today   **{vc_d} min**\n"
            f"Week    **{vc_w} min**\n"
            f"Month   **{vc_m} min**\n"
            f"Total   **{vc_h}h {vc_mn}m**\n"
            f"Rank    **#{vc_rank_pos}**"
        ),
        inline=True,
    )

    # XP block
    embed.add_field(
        name="✨  XP",
        value=(
            f"Today   **{xp_d:,}**\n"
            f"Week    **{xp_w:,}**\n"
            f"Month   **{xp_m:,}**\n"
            f"Total   **{xp:,}**\n"
            f"Rank    **#{xp_rank_pos}**"
        ),
        inline=True,
    )

    # Streak + moderation
    streak_icon = "🔥" if streak >= 7 else "📅"
    embed.add_field(
        name=f"{streak_icon}  Activity Streak",
        value=(
            f"Current  **{streak}** days\n"
            f"Longest  **{longest}** days\n"
            f"First msg  **{first}**"
        ),
        inline=True,
    )

    warn_text = "✅ Clean record" if warns == 0 else f"⚠️ **{warns}** warning{'s' if warns>1 else ''}"
    embed.add_field(name="🛡️  Moderation", value=warn_text, inline=True)

    # Next milestones
    mn, nr = get_next_milestone(member, total, gdata)
    vn, vr = get_next_vc_milestone(member, vc_t, gdata)
    ms = []
    if nr: ms.append(f"🎯 **{mn:,}** msgs → **{nr.name}**")
    if vr: ms.append(f"🎙️ **{vn}** min → **{vr.name}**")
    if ms:
        embed.add_field(name="🚀  Next Milestones", value="\n".join(ms), inline=True)

    return embed

# ══════════════════════════════════════════════════════════════════════════════
#  AUTOMOD HELPER
# ══════════════════════════════════════════════════════════════════════════════
async def run_automod(message: discord.Message, gdata: dict) -> bool:
    s = gdata["settings"]
    if not s.get("automod_enabled", False): return False
    if message.author.guild_permissions.manage_messages: return False
    content = message.content
    reasons = []
    for word in s.get("automod_banned_words", []):
        if word.lower() in content.lower():
            reasons.append(f"banned word: `{word}`"); break
    max_men = s.get("automod_max_mentions", 5)
    if len(message.mentions) > max_men:
        reasons.append(f"too many mentions ({len(message.mentions)})")
    if len(content) > 10:
        caps_pct = sum(1 for c in content if c.isupper()) / len(content) * 100
        if caps_pct > s.get("automod_max_caps_pct", 80):
            reasons.append(f"excessive caps ({int(caps_pct)}%)")
    if not reasons: return False
    try:    await message.delete()
    except: pass
    data  = load_data()
    gdata = get_guild_data(data, message.guild.id)
    udata = get_user_data(gdata, message.author.id)
    udata["warnings"] = udata.get("warnings", 0) + 1
    save_data(data)
    reason_str = ", ".join(reasons)
    try:
        await message.channel.send(
            f"⚠️ {message.author.mention} message removed — {reason_str}. (Warning {udata['warnings']})",
            delete_after=8)
    except: pass
    log_cid = s.get("log_channel")
    if log_cid:
        log_ch = message.guild.get_channel(int(log_cid))
        if log_ch:
            embed = discord.Embed(title="🛡️ AutoMod Action", color=0xFF4444, timestamp=datetime.utcnow())
            embed.add_field(name="User",    value=str(message.author))
            embed.add_field(name="Reason",  value=reason_str)
            embed.add_field(name="Channel", value=message.channel.mention)
            embed.add_field(name="Content", value=content[:500] or "(empty)", inline=False)
            embed.set_footer(text="AutoMod • Chill Pill")
            try: await log_ch.send(embed=embed)
            except: pass
    return True

# ══════════════════════════════════════════════════════════════════════════════
#  HELP MENU WITH DROPDOWN + BUTTONS
# ══════════════════════════════════════════════════════════════════════════════
HELP_CATEGORIES = {
    "counter": {
        "emoji": "⚡", "name": "Activity Counter", "desc": "Track messages, voice, XP & streaks", "color": 0x00FF88,
        "commands": [
            ("!rank [@user]", "Rank card with progress bars, XP & streak"),
            ("!stats [@user]", "Full breakdown: msgs, VC, XP, ranks"),
            ("!msgcount [@user]", "Quick message count"),
            ("!msgtop", "Top 10 message senders"),
            ("!lb msg [period]", "Message leaderboard"),
            ("!vctime [@user]", "Voice time for a user"),
            ("!vctop", "Top 10 voice users"),
            ("!lb vc [period]", "Voice leaderboard"),
            ("!xp [@user]", "Check XP"),
            ("!xptop", "Top 10 by XP"),
            ("!streak [@user]", "Daily message streak"),
            ("!streaktop", "Streak leaderboard"),
            ("!activity [@user]", "Full activity report"),
            ("!compare @user1 @user2", "Side-by-side comparison"),
            ("!setgoal <count>", "Set personal message goal"),
            ("!goal [@user]", "Check goal progress"),
        ]
    },
    "ranks": {
        "emoji": "🏅", "name": "Rank Roles", "desc": "Message & voice role systems", "color": 0xFFA500,
        "commands": [
            ("!setrole @role <count>", "Set message role threshold"),
            ("!removerole @role", "Remove message threshold"),
            ("!listroles", "List all message thresholds"),
            ("!setvcrole @role <min>", "Set VC role threshold"),
            ("!removevcrole @role", "Remove VC threshold"),
            ("!listvcroles", "List VC role thresholds"),
            ("!addpath @trigger @role <count>", "Add conditional role path"),
            ("!removepath @trigger", "Remove a path"),
            ("!listpaths", "List all conditional paths"),
        ]
    },
    "moderation": {
        "emoji": "🔨", "name": "Moderation", "desc": "Warn, mute, kick, ban & purge", "color": 0xFF4444,
        "commands": [
            ("!warn @user [reason]", "Warn a member"),
            ("!warnings [@user]", "View warnings"),
            ("!clearwarnings @user", "Clear warnings"),
            ("!mute @user [min] [reason]", "Timed mute"),
            ("!unmute @user", "Unmute"),
            ("!kick @user [reason]", "Kick member"),
            ("!ban @user [reason]", "Ban member"),
            ("!unban <user_id>", "Unban user"),
            ("!purge <n>", "Delete N messages"),
            ("!purge bot [n]", "Delete bot messages only"),
            ("!purge user @user [n]", "Delete user's messages"),
            ("!purge contains <text>", "Delete by keyword"),
            ("!setmuterole @role", "Set mute role"),
        ]
    },
    "automod": {
        "emoji": "🤖", "name": "AutoMod", "desc": "Automated moderation", "color": 0x9B59B6,
        "commands": [
            ("!automod on/off", "Enable/disable AutoMod"),
            ("!addword <word>", "Add banned word"),
            ("!removeword <word>", "Remove banned word"),
            ("!listwords", "List banned words"),
            ("!setmaxmentions <n>", "Max mentions per message"),
            ("!setmaxcaps <pct>", "Max caps percentage"),
            ("!setlogchannel #ch", "Set log channel"),
        ]
    },
    "welcome": {
        "emoji": "👋", "name": "Welcome System", "desc": "Welcome/goodbye with auto-delete", "color": 0x57F287,
        "commands": [
            ("!welcome", "Open welcome settings menu"),
            ("!welcome add #channel", "Add welcome channel"),
            ("!welcome remove #channel", "Remove welcome channel"),
            ("!welcome addgoodbye #channel", "Add goodbye channel"),
            ("!welcome removegoodbye #channel", "Remove goodbye channel"),
            ("!welcome setdelete <sec>", "Set auto-delete (0=off)"),
            ("!welcome setmessage <msg>", "Set custom welcome message"),
            ("!welcome setgoodbye <msg>", "Set custom goodbye message"),
            ("!welcome embed", "Toggle embed mode"),
            ("!welcome test", "Test welcome message"),
            ("!welcome testgoodbye", "Test goodbye message"),
        ]
    },
    "settings": {
        "emoji": "⚙️", "name": "Server Settings", "desc": "Configure bot behavior", "color": 0x3498DB,
        "commands": [
            ("!settings", "View full server config"),
            ("!setspam <sec>", "Anti-spam interval"),
            ("!toggleemoji", "Toggle emoji counting"),
            ("!togglevc", "Toggle VC in role thresholds"),
            ("!whitelist #ch", "Whitelist channel"),
            ("!blacklist #ch", "Blacklist channel"),
            ("!clearwhitelist", "Clear whitelist"),
            ("!clearblacklist", "Clear blacklist"),
            ("!listchannels", "Show channel settings"),
            ("!addowner @user", "Add bot owner"),
            ("!removeowner @user", "Remove bot owner"),
        ]
    },
    "milestones": {
        "emoji": "🎯", "name": "Milestones", "desc": "Message & VC milestone announcements", "color": 0xF1C40F,
        "commands": [
            ("!setmilestone #channel", "Set announce channel"),
            ("!addmilestone <count>", "Add message milestone"),
            ("!removemilestone <count>", "Remove milestone"),
            ("!listmilestones", "List all milestones"),
            ("!setxp min <n>", "Set min XP gain"),
            ("!setxp max <n>", "Set max XP gain"),
            ("!setxpcooldown <sec>", "Set XP cooldown"),
        ]
    },
    "voice": {
        "emoji": "🔊", "name": "Voice Management", "desc": "Voice channel utilities", "color": 0x1ABC9C,
        "commands": [
            ("!vclb [period]", "Voice time leaderboard"),
            ("!vctime [@user]", "Check VC time"),
            ("!vctop", "Top 10 voice users"),
            ("!setvcrole @role <min>", "Set VC role threshold"),
            ("!removevcrole @role", "Remove VC threshold"),
            ("!listvcroles", "List VC role thresholds"),
        ]
    },
    "giveaway": {
        "emoji": "🎁", "name": "Giveaways", "desc": "Host and manage giveaways", "color": 0xE91E63,
        "commands": [
            ("!gcreate", "Start a giveaway (interactive)"),
            ("!gend <msg_id>", "End giveaway early"),
            ("!greroll <msg_id>", "Reroll a winner"),
            ("!glist", "List active giveaways"),
        ]
    },
     "ticket": {
        "emoji": "🎫", "name": "Ticket System", "desc": "Advanced ticket management with dropdown menus", "color": 0x2ECC71,
        "commands": [
            ("!setup #channel", "Setup ticket panel in a channel (Server Owner)"),
            ("!addticketcat <category> <name> [desc]", "Add a ticket category with its own Discord category"),
            ("!removeticketcat <name>", "Remove a ticket category"),
            ("!listticketcats", "List all ticket categories"),
            ("!toggleticketcat <name>", "Enable/disable a ticket category"),
            ("!setcatemoji <name> <emoji>", "Set emoji for a ticket category"),
            ("!addticketmanager @role", "Add a ticket manager role"),
            ("!removeticketmanager @role", "Remove a ticket manager role"),
            ("!setticketembed <title> <desc>", "Set ticket panel embed title & description"),
            ("!settickettranscript #channel", "Set channel for ticket transcripts"),
            ("!close", "Close current ticket (creates transcript)"),
            ("!rename <new-name>", "Rename current ticket channel"),
            ("!delete", "Delete current ticket channel (admin only)"),
        ]
    }, 
    "info": {
        "emoji": "ℹ️", "name": "Information", "desc": "Server & user info", "color": 0x95A5A6,
        "commands": [
            ("!serverinfo", "Server information"),
            ("!userinfo [@user]", "User information"),
            ("!roleinfo @role", "Role information"),
            ("!botinfo", "About this bot"),
            ("!ping", "Bot latency"),
            ("!avatar [@user]", "View user avatar"),
        ]
    },
    "fun": {
        "emoji": "🎮", "name": "Fun Commands", "desc": "Entertainment", "color": 0xFF6B6B,
        "commands": [
            ("!coinflip", "Heads or Tails"),
            ("!roll [sides]", "Roll a dice"),
            ("!8ball <question>", "Ask the magic 8-ball"),
            ("!choose a | b | c", "Random choice from list"),
            ("!reverse <text>", "Reverse your text"),
        ]
    },
    "owner_perms": {
        "emoji": "👑", "name": "Owner Only", "desc": "Bot owner commands (admins only)", "color": 0xFFD700,
        "commands": [
            ("!addowner @user", "Grant bot-owner perms"),
            ("!removeowner @user", "Revoke bot-owner perms"),
            ("!addmsgs @user <n>", "Add messages manually"),
            ("!removemsgs @user <n>", "Remove messages"),
            ("!resetuser @user", "Reset all stats for one member"),
            ("!resetallstats", "Reset ALL stats for entire server"),
            ("!resetallmessages", "Reset all message stats server-wide"),
            ("!resetallvc", "Reset all VC stats server-wide"),
            ("!blacklistmember @user", "Block role upgrades"),
            ("!unblacklistmember @user", "Unblock member"),
            ("!setrole @role <count>", "Set message threshold"),
            ("!setvcrole @role <min>", "Set VC threshold"),
            ("!addpath @trigger @role <count>", "Add conditional path"),
            ("!setmilestone #channel", "Set milestone channel"),
            ("!addmilestone <count>", "Add milestone"),
            ("!setmaxmentions <n>", "Max mentions"),
            ("!setmaxcaps <pct>", "Max caps percentage"),
            ("!automod on/off", "Toggle AutoMod"),
            ("!setlogchannel #ch", "Set log channel"),
            ("!setmuterole @role", "Set mute role"),
            ("!setticketcategory <cat>", "Set ticket category"),
            ("!setticketsupport @role", "Set ticket support role"),
            ("!dm @member <msg>", "DM a specific member"),
            ("!dmall <msg>", "DM all members in server"),
            ("!dmrole @role <msg>", "DM all members with specific role"),
            ("!dmaddrole @role", "Allow a role to use DM commands"),
            ("!dmremoverole @role", "Remove role's DM permission"),
            ("!dmlistroles", "List roles with DM permissions"),
        ]
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# 5. HELP HOME EMBED  (replaces help_home_embed)
# ──────────────────────────────────────────────────────────────────────────────
def help_home_embed(guild):
    embed = discord.Embed(
        title="🎉  CHILL PILL",
        description=(
            "*Your ultimate Discord companion — activity tracking, moderation, and fun!*\n\n"
            "**🗂️ Categories**\n"
            "⚡ Activity  ·  🏅 Rank Roles  ·  🔨 Moderation\n"
            "🤖 AutoMod  ·  👋 Welcome  ·  🎫 Tickets\n"
            "🎯 Milestones  ·  🔊 Voice  ·  🎁 Giveaways\n"
            "ℹ️ Info  ·  🎮 Fun  ·  ⚙️ Settings  ·  👑 Owner\n\n"
            "**📖 Usage**\n"
            "Select a category from the dropdown below to view commands.\n"
            "All commands use the `!prefix` style — e.g. `!rank @user`\n\n"
            "**✨ Quick start**\n"
            "`!rank` · `!stats` · `!lb msg` · `!help`"
        ),
        color=BOT_COLOR,
        timestamp=datetime.utcnow(),
    )
    total_cmds = sum(len(cat["commands"]) for cat in HELP_CATEGORIES.values())
    embed.set_footer(text=f"📚 {total_cmds} commands  •  Select a category below")
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
        embed.set_author(name=guild.name, icon_url=guild.icon.url)
    return embed

class HelpDropdown(discord.ui.Select):
    def __init__(self, guild, user_id, bot_ref):
        member = guild.get_member(user_id)
        # Safely handle if member is None
        is_admin = False
        if member:
            is_admin = member.guild_permissions.administrator or member.id == guild.owner_id
        
        options = []
        for key, cat in HELP_CATEGORIES.items():
            if key == "owner_perms" and not is_admin:
                continue
            if key == "ticket" and not is_admin:
                continue
            options.append(
                discord.SelectOption(
                    label=cat["name"],
                    value=key,
                    description=cat["desc"],
                    emoji=cat["emoji"]
                )
            )
        
        super().__init__(
            placeholder="📚 Select a command category...",
            options=options,
            min_values=1,
            max_values=1
        )
        self.guild   = guild
        self.user_id = user_id
        self.bot_ref = bot_ref

    async def callback(self, interaction: discord.Interaction):
        """Handle when a user selects an option from the dropdown"""
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message(
                "❌ This help menu is not for you! Use `!help` to open your own.", ephemeral=True)

        cat_key = self.values[0]
        cat = HELP_CATEGORIES[cat_key]

        embed = discord.Embed(
            title=f"{cat['emoji']}  {cat['name']}",
            description=f"*{cat['desc']}*\n\u200b",
            color=cat["color"],
            timestamp=datetime.utcnow(),
        )

        for cmd, desc in cat["commands"]:
            embed.add_field(name=f"`{cmd}`", value=desc, inline=True)

        if len(cat["commands"]) % 2 == 1:
            embed.add_field(name="\u200b", value="\u200b", inline=True)

        embed.set_footer(text=f"{len(cat['commands'])} commands  •  !help to return")
        if self.guild and self.guild.icon:
            embed.set_thumbnail(url=self.guild.icon.url)

        view = HelpView(self.guild, self.user_id, bot_ref=self.bot_ref)
        await interaction.response.edit_message(embed=embed, view=view)
        
# ──────────────────────────────────────────────────────────────────────────────
# 6. HELP DROPDOWN  (replaces HelpDropdown)
# ──────────────────────────────────────────────────────────────────────────────
# Replace HelpDropdown.callback with this version for coloured category embeds:=

class HelpView(discord.ui.View):
    def __init__(self, guild, user_id=None, bot_ref=None):
        super().__init__(timeout=300)  # 5 minutes is more reasonable
        self.guild   = guild
        self.user_id = user_id
        self.bot_ref = bot_ref
        self.add_item(HelpDropdown(guild, user_id, bot_ref))

        home_btn = discord.ui.Button(label="🏠 Home", style=discord.ButtonStyle.primary, row=1)
        async def home_cb(interaction: discord.Interaction):
            if user_id and interaction.user.id != user_id:
                return await interaction.response.send_message("❌ Not your menu.", ephemeral=True)
            await interaction.response.edit_message(embed=help_home_embed(guild), view=HelpView(guild, user_id, bot_ref))
        home_btn.callback = home_cb
        self.add_item(home_btn)

        stats_btn = discord.ui.Button(label="📊 Bot Stats", style=discord.ButtonStyle.secondary, row=1)
        async def stats_cb(interaction: discord.Interaction):
            if user_id and interaction.user.id != user_id:
                return await interaction.response.send_message("❌ Not your menu.", ephemeral=True)
            embed = discord.Embed(title="🤖 Bot Statistics", color=BOT_COLOR, timestamp=datetime.utcnow())
            total_cmds = sum(len(cat["commands"]) for cat in HELP_CATEGORIES.values())
            embed.add_field(name="📚 Commands", value=f"**{total_cmds}**", inline=True)
            embed.add_field(name="📁 Categories", value=f"**{len(HELP_CATEGORIES)}**", inline=True)
            embed.add_field(name="🖥️ Servers", value=f"**{len(bot_ref.guilds) if bot_ref else 0}**", inline=True)
            embed.add_field(name="⏱️ Latency", value=f"**{round(bot_ref.latency*1000) if bot_ref else 0}ms**", inline=True)
            await interaction.response.edit_message(embed=embed, view=HelpView(guild, user_id, bot_ref))
        stats_btn.callback = stats_cb
        self.add_item(stats_btn)

        close_btn = discord.ui.Button(label="❌ Close", style=discord.ButtonStyle.danger, row=1)
        async def close_cb(interaction: discord.Interaction):
            await interaction.message.delete()
        close_btn.callback = close_cb
        self.add_item(close_btn)

# ══════════════════════════════════════════════════════════════════════════════
#  TICKET SYSTEM - DROPDOWN MENU & BUTTONS
# ══════════════════════════════════════════════════════════════════════════════

class TicketButton(discord.ui.Button):
    """Button that opens the ticket creation dropdown - PERSISTENT"""
    def __init__(self, label: str = "Create Ticket", style: discord.ButtonStyle = discord.ButtonStyle.primary, emoji: str = "🎫"):
        super().__init__(
            label=label, 
            style=style, 
            emoji=emoji, 
            custom_id="persistent_ticket_create_button",
            row=0
        )
    
    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        gdata = get_guild_data(data, interaction.guild.id)
        view = TicketCategoryView(interaction.guild, interaction.user.id)
        await interaction.response.send_message(
            "📋 **Select a ticket category:**\nChoose the category that best fits your issue.",
            view=view,
            ephemeral=True
        )

class TicketCategorySelect(discord.ui.Select):
    """Dropdown for selecting ticket category - PERSISTENT"""
    def __init__(self, guild, user_id):
        self.guild = guild
        self.user_id = user_id
        
        data = load_data()
        gdata = get_guild_data(data, guild.id)
        ticket_categories = gdata["settings"].get("ticket_categories", {})
        
        options = []
        for cat_name, cat_config in ticket_categories.items():
            if cat_config.get("enabled", True):
                emoji = cat_config.get("emoji", "🎫")
                description = cat_config.get("description", "Create a ticket for this category")
                options.append(
                    discord.SelectOption(
                        label=cat_name[:100],
                        value=cat_name,
                        description=description[:100],
                        emoji=emoji
                    )
                )
        
        options.append(discord.SelectOption(
            label="❌ Cancel",
            value="cancel",
            description="Cancel ticket creation",
            emoji="❌"
        ))
        
        super().__init__(
            placeholder="📋 Select a ticket category...",
            options=options,
            min_values=1,
            max_values=1,
            custom_id="persistent_ticket_category_select"
        )
    
    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu is not for you!", ephemeral=True)
        
        selected = self.values[0]
        
        if selected == "cancel":
            return await interaction.response.send_message("❌ Ticket creation cancelled.", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        data = load_data()
        gdata = get_guild_data(data, interaction.guild.id)
        ticket_categories = gdata["settings"].get("ticket_categories", {})
        
        if selected not in ticket_categories:
            return await interaction.followup.send("❌ This category is no longer available.", ephemeral=True)
        
        cat_config = ticket_categories[selected]
        category_id = cat_config.get("category_id")
        
        if not category_id:
            return await interaction.followup.send("❌ This category is not properly configured.", ephemeral=True)
        
        category = interaction.guild.get_channel(int(category_id))
        if not category:
            return await interaction.followup.send("❌ Category not found. Please contact an admin.", ephemeral=True)
        
        for channel in interaction.guild.channels:
            if channel.name.startswith(f"ticket-{interaction.user.name.lower()}"):
                if isinstance(channel, discord.TextChannel):
                    return await interaction.followup.send(
                        f"❌ You already have an open ticket! Please close it first: {channel.mention}",
                        ephemeral=True
                    )
        
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_messages=True,
                attach_files=True,
                embed_links=True
            ),
        }
        
        support_role_id = gdata["settings"].get("ticket_support_role")
        if support_role_id:
            support_role = interaction.guild.get_role(int(support_role_id))
            if support_role:
                overwrites[support_role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_messages=True
                )
        
        manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
        for role_id in manager_role_ids:
            manager_role = interaction.guild.get_role(role_id)
            if manager_role:
                overwrites[manager_role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_messages=True
                )
        
        safe_name = interaction.user.name.lower().replace(" ", "-")[:20]
        cat_prefix = cat_config.get("prefix", "ticket")
        channel_name = f"{cat_prefix}-{safe_name}"
        
        try:
            ticket_channel = await interaction.guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                topic=f"Ticket for {interaction.user.name} | Category: {selected} | Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
        except Exception as e:
            return await interaction.followup.send(f"❌ Failed to create ticket: {e}", ephemeral=True)
        
        ticket_info = {
            "user_id": interaction.user.id,
            "category": selected,
            "created_at": datetime.now().isoformat(),
            "creator_name": interaction.user.name
        }
        active_tickets[ticket_channel.id] = ticket_info
        
        default_title = gdata["settings"].get("ticket_default_title", "Support Ticket")
        default_desc = gdata["settings"].get("ticket_default_description", "Please describe your issue in detail.")
        
        embed = discord.Embed(
            title=f"🎫 {default_title} - {selected}",
            description=f"{default_desc}\n\n"
                       f"**Ticket Information:**\n"
                       f"• **Created by:** {interaction.user.mention}\n"
                       f"• **Category:** {selected}\n"
                       f"• **Created at:** <t:{int(datetime.now().timestamp())}:F>\n\n"
                       f"**Instructions:**\n"
                       f"• Please explain your issue clearly\n"
                       f"• Provide screenshots if applicable\n"
                       f"• Use `!close` to close this ticket\n"
                       f"• Use `!rename <new-name>` to rename this ticket",
            color=BOT_COLOR,
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Ticket ID: {ticket_channel.id}  •  Use !close to close this ticket")
        
        view = TicketActionView(interaction.guild, ticket_channel.id, interaction.user.id)
        
        await ticket_channel.send(
            content=f"{interaction.user.mention} {' '.join([f'<@&{rid}>' for rid in manager_role_ids]) if manager_role_ids else ''}{f' <@&{support_role_id}>' if support_role_id else ''}",
            embed=embed,
            view=view
        )
        
        await ticket_channel.send(
            f"✅ **Ticket created!** Support will be with you shortly.\n"
            f"Please describe your issue in this channel."
        )
        
        await interaction.followup.send(
            f"✅ Ticket created! Please go to {ticket_channel.mention}",
            ephemeral=True
        )
        
        log_channel_id = gdata["settings"].get("log_channel")
        if log_channel_id:
            log_ch = interaction.guild.get_channel(int(log_channel_id))
            if log_ch:
                log_embed = discord.Embed(
                    title="🎫 Ticket Created",
                    description=f"**User:** {interaction.user.mention}\n"
                               f"**Channel:** {ticket_channel.mention}\n"
                               f"**Category:** {selected}",
                    color=0x00FF00,
                    timestamp=datetime.utcnow()
                )
                await log_ch.send(embed=log_embed)

class TicketCategoryView(discord.ui.View):
    """View containing the ticket category dropdown - PERSISTENT"""
    def __init__(self, guild, user_id):
        super().__init__(timeout=None)
        self.add_item(TicketCategorySelect(guild, user_id))

class TicketActionView(discord.ui.View):
    """View containing ticket action buttons - Persistent across restarts"""
    def __init__(self, guild, channel_id, creator_id):
        super().__init__(timeout=None)
        self.guild = guild
        self.channel_id = channel_id
        self.creator_id = creator_id
        
        close_btn = discord.ui.Button(label="🔒 Close Ticket", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="close_ticket")
        close_btn.callback = self.close_ticket_callback
        self.add_item(close_btn)
        
        rename_btn = discord.ui.Button(label="✏️ Rename", style=discord.ButtonStyle.secondary, emoji="✏️", custom_id="rename_ticket")
        rename_btn.callback = self.rename_ticket_callback
        self.add_item(rename_btn)
        
        claim_btn = discord.ui.Button(label="🎫 Claim", style=discord.ButtonStyle.success, emoji="🎫", custom_id="claim_ticket")
        claim_btn.callback = self.claim_ticket_callback
        self.add_item(claim_btn)
    
    async def close_ticket_callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        data = load_data()
        gdata = get_guild_data(data, guild.id)
        
        support_role_id = gdata["settings"].get("ticket_support_role")
        manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
        
        has_permission = (
            (support_role_id and support_role_id in [r.id for r in interaction.user.roles]) or
            any(rid in [r.id for r in interaction.user.roles] for rid in manager_role_ids) or
            interaction.user.guild_permissions.administrator or
            interaction.user.id == guild.owner_id
        )
        
        if not has_permission:
            return await interaction.response.send_message(
                "❌ Only the server owner and ticket managers can close tickets.", 
                ephemeral=True
            )
        
        confirm_embed = discord.Embed(
            title="🔒 Close Ticket?",
            description=f"Are you sure you want to close this ticket?\n\nThis will **delete** the channel.\n\nReact ✅ to confirm or ❌ to cancel.",
            color=0xFFA500,
            timestamp=datetime.utcnow()
        )
        await interaction.response.send_message(embed=confirm_embed)
        confirm_msg = await interaction.original_response()
        await confirm_msg.add_reaction("✅")
        await confirm_msg.add_reaction("❌")
        
        def check(reaction, user):
            return user == interaction.user and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
        
        try:
            reaction, _ = await bot.wait_for("reaction_add", timeout=30.0, check=check)
            if str(reaction.emoji) == "❌":
                await confirm_msg.delete()
                return await interaction.followup.send("❌ Ticket closing cancelled.", ephemeral=True)
        except asyncio.TimeoutError:
            await confirm_msg.delete()
            return await interaction.followup.send("❌ Confirmation timed out.", ephemeral=True)
        
        await confirm_msg.delete()
        channel = interaction.channel
        await interaction.followup.send("🔒 Closing ticket in 5 seconds...", ephemeral=True)
        await asyncio.sleep(5)
        
        if channel:
            await self.send_transcript(interaction, channel)
            await channel.delete(reason=f"Ticket closed by {interaction.user}")
            
            log_channel_id = gdata["settings"].get("log_channel")
            if log_channel_id:
                log_ch = guild.get_channel(int(log_channel_id))
                if log_ch:
                    log_embed = discord.Embed(
                        title="🎫 Ticket Closed",
                        description=f"**Closed by:** {interaction.user.mention}\n"
                                   f"**Channel:** #{channel.name}",
                        color=0xFF0000,
                        timestamp=datetime.utcnow()
                    )
                    await log_ch.send(embed=log_embed)
    
    async def rename_ticket_callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        data = load_data()
        gdata = get_guild_data(data, guild.id)
        
        support_role_id = gdata["settings"].get("ticket_support_role")
        manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
        
        has_permission = (
            (support_role_id and support_role_id in [r.id for r in interaction.user.roles]) or
            any(rid in [r.id for r in interaction.user.roles] for rid in manager_role_ids) or
            interaction.user.guild_permissions.administrator or
            interaction.user.id == guild.owner_id
        )
        
        if not has_permission:
            return await interaction.response.send_message(
                "❌ Only the server owner and ticket managers can rename tickets.", 
                ephemeral=True
            )
        
        modal = TicketRenameModal(interaction.channel.id)
        await interaction.response.send_modal(modal)
    
    async def claim_ticket_callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        data = load_data()
        gdata = get_guild_data(data, guild.id)
        
        support_role_id = gdata["settings"].get("ticket_support_role")
        manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
        
        has_permission = (
            (support_role_id and support_role_id in [r.id for r in interaction.user.roles]) or
            any(rid in [r.id for r in interaction.user.roles] for rid in manager_role_ids) or
            interaction.user.guild_permissions.administrator or
            interaction.user.id == guild.owner_id
        )
        
        if not has_permission:
            return await interaction.response.send_message(
                "❌ Only the server owner and ticket managers can claim tickets.", 
                ephemeral=True
            )
        
        channel = interaction.channel
        if channel:
            embed = discord.Embed(
                title="🎫 Ticket Claimed",
                description=f"**{interaction.user.mention}** has claimed this ticket and will be assisting you.",
                color=0x00FF00,
                timestamp=datetime.utcnow()
            )
            await channel.send(embed=embed)
            await interaction.response.send_message("✅ You have claimed this ticket!", ephemeral=True)
    
    async def send_transcript(self, interaction, channel):
        """Send a transcript of the ticket to the transcript channel"""
        guild = interaction.guild
        data = load_data()
        gdata = get_guild_data(data, guild.id)
        transcript_channel_id = gdata["settings"].get("ticket_transcripts")
        
        if transcript_channel_id:
            transcript_channel = guild.get_channel(int(transcript_channel_id))
            if transcript_channel:
                messages = []
                async for msg in channel.history(limit=200, oldest_first=True):
                    timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
                    messages.append(f"[{timestamp}] {msg.author.name}: {msg.content}")
                
                transcript_text = "\n".join(messages)
                
                transcript_file = discord.File(
                    io.BytesIO(transcript_text.encode()),
                    filename=f"transcript_{channel.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                )
                
                embed = discord.Embed(
                    title="📄 Ticket Transcript",
                    description=f"**Channel:** {channel.name}\n"
                               f"**Closed by:** {interaction.user.mention}\n"
                               f"**Messages:** {len(messages)}",
                    color=BOT_COLOR,
                    timestamp=datetime.utcnow()
                )
                await transcript_channel.send(embed=embed, file=transcript_file)

class TicketRenameModal(discord.ui.Modal, title="Rename Ticket"):
    """Modal for renaming tickets"""
    def __init__(self, channel_id):
        super().__init__()
        self.channel_id = channel_id
        
        self.new_name = discord.ui.TextInput(
            label="New Channel Name",
            placeholder="Enter new name (no spaces, use - or _)",
            required=True,
            max_length=32
        )
        self.add_item(self.new_name)
    
    async def on_submit(self, interaction: discord.Interaction):
        channel = interaction.guild.get_channel(self.channel_id)
        if channel:
            new_name = self.new_name.value.lower().replace(" ", "-")
            try:
                await channel.edit(name=f"ticket-{new_name}")
                await interaction.response.send_message(f"✅ Ticket renamed to `{new_name}`", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(f"❌ Failed to rename: {e}", ephemeral=True)

# ══════════════════════════════════════════════════════════════════════════════
#  BOT SETUP
# ══════════════════════════════════════════════════════════════════════════════
intents                 = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.voice_states    = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None, case_insensitive=True)

# ── Shared data store used across the bot ─────────────────────────────────────
# We keep ONE copy of data in memory and mutate it. Background tasks write
# this copy to disk. This avoids the "load → discard in-memory changes → save"
# pattern that was silently resetting stats.
_data: dict = {}

def get_data() -> dict:
    """Return the shared in-memory data store."""
    return _data

def save_and_shutdown():
    """Save data and shutdown gracefully"""
    log.info("Saving data before shutdown...")
    # Flush VC sessions into data before saving
    _flush_vc_to_data(_data)
    save_vc_sessions(_data)
    save_data(_data)
    log.info("Data saved!")
    sys.exit(0)

# Register signal handlers for clean shutdown
signal.signal(signal.SIGINT, lambda sig, frame: save_and_shutdown())
signal.signal(signal.SIGTERM, lambda sig, frame: save_and_shutdown())

# ══════════════════════════════════════════════════════════════════════════════
#  EVENTS
# ══════════════════════════════════════════════════════════════════════════════
@bot.event
async def on_disconnect():
    """Save data (including live VC sessions) when bot disconnects"""
    log.info("Bot disconnecting - saving data...")
    _flush_vc_to_data(_data)
    save_vc_sessions(_data)
    save_data(_data)
    log.info("Data saved on disconnect")

@bot.event
async def on_shutdown():
    """Save data when bot shuts down"""
    log.info("Bot shutting down - saving data...")
    _flush_vc_to_data(_data)
    save_vc_sessions(_data)
    save_data(_data)
    log.info("Data saved on shutdown")

def _flush_vc_to_data(data: dict):
    """
    Commit current in-memory VC session times into user data WITHOUT
    resetting the session start time (so time keeps accumulating).
    Called by the periodic flush task and on shutdown.
    """
    now = time.time()
    for gid, sessions in vc_sessions.items():
        guild = bot.get_guild(int(gid))
        if not guild:
            continue
        gdata = get_guild_data(data, int(gid))
        for uid, join_time in list(sessions.items()):
            minutes = (now - join_time) / 60
            member  = guild.get_member(int(uid))
            if member:
                udata = get_user_data(gdata, int(uid))
                increment_vc(udata, minutes)
                # Reset the session start to now so we don't double-count
                sessions[uid] = now

async def restore_persistent_views():
    """Restore all persistent views after bot restart"""
    log.info("🔄 Restoring persistent views...")
    
    restored_count = 0
    
    for guild_id_str, gdata in _data.items():
        if guild_id_str.startswith("_"):
            continue  # skip internal keys like _vc_sessions
        guild_id = int(guild_id_str)
        guild = bot.get_guild(guild_id)
        
        if not guild:
            continue
        
        settings = gdata.get("settings", {})
        
        panel_channel_id = settings.get("ticket_setup_channel")
        panel_message_id = settings.get("ticket_panel_message_id")
        
        if panel_channel_id and panel_message_id:
            channel = guild.get_channel(int(panel_channel_id))
            if channel:
                try:
                    message = await channel.fetch_message(int(panel_message_id))
                    view = discord.ui.View(timeout=None)
                    view.add_item(TicketButton())
                    await message.edit(view=view)
                    restored_count += 1
                    log.info(f"✅ Restored ticket panel in {guild.name} - #{channel.name}")
                except discord.NotFound:
                    log.warning(f"⚠️ Ticket panel message not found in {guild.name}")
                except Exception as e:
                    log.error(f"❌ Failed to restore ticket panel in {guild.name}: {e}")
    
    log.info(f"✅ Restored {restored_count} ticket panels")

@bot.event
async def on_ready():
    await bot.tree.sync()
    log.info(f"Logged in as {bot.user} ({bot.user.id})")
    
    # Start background tasks
    cleanup_old_data.start()
    flush_vc_sessions_task.start()
    auto_save_data.start()
    
    # ⭐ CRITICAL: Restore persistent views after bot restart
    await restore_persistent_views()
    
    # Bot status
    await bot.change_presence(
        activity=discord.Streaming(name="!help | CHILL PILL", url="https://twitch.tv/yourchannel"),
        status=discord.Status.online
    )

    # Bot status - Rotating status
    statuses = [
        discord.Game(name="!help | Chill Pill"),
        discord.Streaming(name=f"📊 {len(bot.guilds)} servers"),
        discord.Activity(type=discord.ActivityType.listening, name="!help"),
        discord.Activity(type=discord.ActivityType.watching, name="CHILL PILL ON TOP"),
        discord.CustomActivity(name="✨ Type !help ✨"),
    ]
    
    async def rotate_status():
        while True:
            for status in statuses:
                await bot.change_presence(activity=status, status=discord.Status.online)
                await asyncio.sleep(10)  # Change every 10 seconds
    
    bot.loop.create_task(rotate_status())

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    gid = member.guild.id
    if after.channel and not before.channel:
        # User joined a VC — record the join time
        vc_sessions[gid][member.id] = time.time()
        # Persist the new session immediately so a restart doesn't lose it
        save_vc_sessions(get_data())
        save_data(get_data())
    elif before.channel and not after.channel:
        # User left a VC — commit their time
        join_time = vc_sessions[gid].pop(member.id, None)
        if join_time:
            minutes = (time.time() - join_time) / 60
            data    = get_data()
            gdata   = get_guild_data(data, gid)
            udata   = get_user_data(gdata, member.id)
            increment_vc(udata, minutes)
            # Remove from persisted sessions too
            save_vc_sessions(data)
            save_data(data)
            await assign_roles(member.guild, member, udata, gdata)
            await check_and_announce_milestones(member.guild, member, udata, gdata)

@bot.event
async def on_member_join(member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, member.guild.id)
    s     = gdata["settings"]

    channels = list(s.get("welcome_channels", []))
    if not channels and s.get("welcome_channel"):
        channels = [s["welcome_channel"]]

    if not channels:
        return

    embed_mode = s.get("welcome_use_embed", True)
    delete_sec = s.get("welcome_delete_seconds", 0)
    msg_template = s.get("welcome_message", "Welcome to the server, {mention}! 🎉")
    msg = (msg_template
           .replace("{mention}", member.mention)
           .replace("{name}", member.display_name)
           .replace("{server}", member.guild.name)
           .replace("{count}", str(member.guild.member_count)))

    for ch_id in channels:
        ch = member.guild.get_channel(int(ch_id))
        if not ch:
            continue
        embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url if member.guild.icon else None)
        embed.set_footer(text=f"Member #{member.guild.member_count}")
        try:
            if embed_mode:
                embed = discord.Embed(description=msg, color=0x57F287)
                embed.set_thumbnail(url=member.display_avatar.url)
                if member.guild.icon:
                    embed.set_author(name=member.guild.name, icon_url=member.guild.icon.url)
                sent = await ch.send(embed=embed)
            else:
                sent = await ch.send(msg)
            if delete_sec > 0:
                await asyncio.sleep(delete_sec)
                try: await sent.delete()
                except: pass
        except Exception:
            pass

@bot.event
async def on_member_remove(member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, member.guild.id)
    s     = gdata["settings"]

    channels = list(s.get("goodbye_channels", []))
    if not channels and s.get("goodbye_channel"):
        channels = [s["goodbye_channel"]]

    if not channels:
        return

    embed_mode = s.get("goodbye_use_embed", True)
    delete_sec = s.get("goodbye_delete_seconds", 0)
    msg_template = s.get("goodbye_message", "**{name}** has left the server.")
    msg = (msg_template
           .replace("{mention}", member.mention)
           .replace("{name}", member.display_name)
           .replace("{server}", member.guild.name)
           .replace("{count}", str(member.guild.member_count)))

    for ch_id in channels:
        ch = member.guild.get_channel(int(ch_id))
        if not ch:
            continue
        try:
            if embed_mode:
                embed = discord.Embed(description=msg, color=0xED4245)
                sent  = await ch.send(embed=embed)
            else:
                sent  = await ch.send(msg)
            if delete_sec > 0:
                await asyncio.sleep(delete_sec)
                try: await sent.delete()
                except: pass
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════════════
#  PREFIX COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@bot.command(name="help")
async def prefix_help(ctx):
    banner = generate_banner()
    view   = HelpView(ctx.guild, ctx.author.id, bot_ref=bot)
    embed  = help_home_embed(ctx.guild)
    await ctx.send(file=banner)
    await ctx.send(embed=embed, view=view)

# ── Rank / Stats / Leaderboards ───────────────────────────────────────────────
@bot.command(name="rank")
async def prefix_rank(ctx, member: discord.Member = None):
    member   = member or ctx.author
    data     = get_data()
    gdata    = get_guild_data(data, ctx.guild.id)
    udata    = get_user_data(gdata, member.id)
    rank_pos = get_rank_position(ctx.guild, gdata, member.id)
    async with ctx.typing():
        try:
            card = await generate_rank_card(member, udata, gdata, rank_pos)
            embed = discord.Embed(
                title=f"{member.display_name}'s Role Progression",
                description="Track your progress towards earning new roles!",
                color=BOT_COLOR
            )
            await ctx.send(embed=embed)
            await ctx.send(file=card)
        except Exception as e:
            log.error(f"Rank card: {e}", exc_info=True)
            await ctx.send("❌ Could not generate rank card. Check `Pillow` and `aiohttp` are installed.")

@bot.command(name="stats")
async def prefix_stats(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    await ctx.send(embed=stats_embed(member, udata, gdata, ctx.guild))

@bot.command(name="lb")
async def prefix_lb(ctx, mode: str = "msg", period: str = "total"):
    mode   = mode.lower()
    period = period.lower()
    if mode in ("daily", "weekly", "monthly", "total"):
        period, mode = mode, "msg"
    if mode not in ("msg", "vc", "xp", "streak"):
        return await ctx.send("❌ Mode: `msg` `vc` `xp` `streak`. Period: `total` `daily` `weekly` `monthly`.")
    if period not in ("total", "daily", "weekly", "monthly"):
        return await ctx.send("❌ Period: `total` `daily` `weekly` `monthly`.")
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, period, mode=mode)
    await ctx.send(embed=leaderboard_embed(scores, period, ctx.guild, mode=mode))

@bot.command(name="vclb")
async def prefix_vclb(ctx, period: str = "total"):
    period = period.lower()
    if period not in ("total", "daily", "weekly", "monthly"):
        return await ctx.send("❌ Choose: `total` `daily` `weekly` `monthly`.")
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, period, mode="vc")
    await ctx.send(embed=leaderboard_embed(scores, period, ctx.guild, mode="vc"))

@bot.command(name="msgcount")
async def prefix_msgcount(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    total  = udata.get("total", 0)
    rank   = get_rank_position(ctx.guild, gdata, member.id)
    embed  = discord.Embed(description=f"💬 **{member.display_name}** — **{total:,} messages** (Rank #{rank})", color=BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.send(embed=embed)

@bot.command(name="msgtop")
async def prefix_msgtop(ctx):
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, "total", mode="msg")
    await ctx.send(embed=leaderboard_embed(scores, "total", ctx.guild, mode="msg"))

@bot.command(name="vctime")
async def prefix_vctime(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    vc_t   = udata.get("vc_total", 0)
    h      = int(vc_t // 60); m = int(vc_t % 60)
    rank   = get_vc_rank_position(ctx.guild, gdata, member.id)
    embed  = discord.Embed(description=f"🎙 **{member.display_name}** — **{h}h {m}m** in voice (VC Rank #{rank})", color=BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.send(embed=embed)

@bot.command(name="vctop")
async def prefix_vctop(ctx):
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, "total", mode="vc")
    await ctx.send(embed=leaderboard_embed(scores, "total", ctx.guild, mode="vc"))

@bot.command(name="xp")
async def prefix_xp(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    xp     = udata.get("xp", 0)
    rank   = get_xp_rank_position(ctx.guild, gdata, member.id)
    embed  = discord.Embed(description=f"✨ **{member.display_name}** — **{xp:,} XP** (XP Rank #{rank})", color=BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.send(embed=embed)

@bot.command(name="xptop")
async def prefix_xptop(ctx):
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, "total", mode="xp")
    await ctx.send(embed=leaderboard_embed(scores, "total", ctx.guild, mode="xp"))

@bot.command(name="streak")
async def prefix_streak(ctx, member: discord.Member = None):
    member  = member or ctx.author
    data    = get_data()
    gdata   = get_guild_data(data, ctx.guild.id)
    udata   = get_user_data(gdata, member.id)
    streak  = udata.get("streak", 0)
    longest = udata.get("longest_streak", 0)
    first   = udata.get("first_msg_date", "Unknown")
    embed   = discord.Embed(title=f"🔥 Streak — {member.display_name}", color=0xFF8C00 if streak >= 7 else BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="🔥 Current Streak", value=f"**{streak}** days", inline=True)
    embed.add_field(name="🏆 Longest Streak", value=f"**{longest}** days", inline=True)
    embed.add_field(name="📅 First Message",  value=first, inline=True)
    if streak >= 30:   embed.set_footer(text="🌟 Incredible dedication!")
    elif streak >= 7:  embed.set_footer(text="🔥 On fire! Keep it up!")
    await ctx.send(embed=embed)

@bot.command(name="streaktop")
async def prefix_streaktop(ctx):
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, "total", mode="streak")
    await ctx.send(embed=leaderboard_embed(scores, "total", ctx.guild, mode="streak"))

@bot.command(name="activity")
async def prefix_activity(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    await ctx.send(embed=stats_embed(member, udata, gdata, ctx.guild))

@bot.command(name="compare")
async def prefix_compare(ctx, member1: discord.Member, member2: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    u1    = get_user_data(gdata, member1.id)
    u2    = get_user_data(gdata, member2.id)
    def w(a, b): return "✅" if a > b else ("❌" if a < b else "🤝")
    t1=u1.get("total",0); t2=u2.get("total",0)
    v1=u1.get("vc_total",0); v2=u2.get("vc_total",0)
    x1=u1.get("xp",0); x2=u2.get("xp",0)
    s1=u1.get("streak",0); s2=u2.get("streak",0)
    embed = discord.Embed(title=f"⚔️ {member1.display_name} vs {member2.display_name}", color=BOT_COLOR, timestamp=datetime.utcnow())
    embed.add_field(name=f"{w(t1,t2)} {member1.display_name}",
        value=f"💬 **{t1:,}** msgs\n🎙 **{int(v1//60)}h {int(v1%60)}m** VC\n✨ **{x1:,}** XP\n🔥 **{s1}** day streak", inline=True)
    embed.add_field(name=f"{w(t2,t1)} {member2.display_name}",
        value=f"💬 **{t2:,}** msgs\n🎙 **{int(v2//60)}h {int(v2%60)}m** VC\n✨ **{x2:,}** XP\n🔥 **{s2}** day streak", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="setgoal")
async def prefix_setgoal(ctx, count: int):
    if count < 1: return await ctx.send("❌ Goal must be ≥ 1.")
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    gdata["settings"].setdefault("msg_goal", {})[str(ctx.author.id)] = count
    save_data(data)
    await ctx.send(f"🎯 Goal set: **{count:,}** messages. Keep going!")

@bot.command(name="goal")
async def prefix_goal(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    goals  = gdata["settings"].get("msg_goal", {})
    goal   = goals.get(str(member.id))
    total  = udata.get("total", 0)
    if not goal:
        return await ctx.send(f"ℹ️ **{member.display_name}** has no goal set. Use `!setgoal <count>`.")
    pct  = min(total / goal, 1.0)
    bar  = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
    done = total >= goal
    embed = discord.Embed(title=f"🎯 Goal — {member.display_name}", color=0x57F287 if done else BOT_COLOR)
    embed.add_field(name="Progress",  value=f"`{bar}` {int(pct*100)}%")
    embed.add_field(name="Messages",  value=f"**{total:,}** / **{goal:,}**")
    if done: embed.add_field(name="Status", value="✅ **GOAL ACHIEVED!**", inline=False)
    else:    embed.add_field(name="Remaining", value=f"**{goal-total:,}** messages to go", inline=False)
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.send(embed=embed)

# ── Milestone admin ────────────────────────────────────────────────────────────
@bot.command(name="setmilestone")
async def prefix_setmilestone(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["milestone_announce"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Milestones will be announced in {channel.mention}.")

@bot.command(name="addmilestone")
async def prefix_addmilestone(ctx, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    ms = gdata["settings"].setdefault("milestone_thresholds", [100, 500, 1000, 5000, 10000])
    if count in ms: return await ctx.send(f"ℹ️ **{count:,}** is already a milestone.")
    ms.append(count); ms.sort()
    save_data(data)
    await ctx.send(f"✅ Added **{count:,}** message milestone.")

@bot.command(name="removemilestone")
async def prefix_removemilestone(ctx, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    ms = gdata["settings"].get("milestone_thresholds", [])
    if count not in ms: return await ctx.send(f"❌ **{count:,}** is not a milestone.")
    ms.remove(count)
    save_data(data)
    await ctx.send(f"✅ Removed **{count:,}** milestone.")

@bot.command(name="listmilestones")
async def prefix_listmilestones(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    ms    = gdata["settings"].get("milestone_thresholds", [])
    vc_ms = gdata["settings"].get("vc_milestone_thresholds", [])
    ch_id = gdata["settings"].get("milestone_announce")
    embed = discord.Embed(title="🎯 Milestones", color=BOT_COLOR)
    embed.add_field(name="Announce Channel",      value=f"<#{ch_id}>" if ch_id else "Not set", inline=False)
    embed.add_field(name="💬 Message Milestones", value=", ".join(f"**{m:,}**" for m in ms) or "None", inline=False)
    embed.add_field(name="🎙 VC Milestones (min)", value=", ".join(f"**{m:,}**" for m in vc_ms) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="setxp")
async def prefix_setxp(ctx, subcommand: str, value: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if subcommand.lower() == "min":
        gdata["settings"]["xp_min"] = max(1, value)
        save_data(data)
        await ctx.send(f"✅ Min XP per message: **{gdata['settings']['xp_min']}**.")
    elif subcommand.lower() == "max":
        gdata["settings"]["xp_max"] = max(1, value)
        save_data(data)
        await ctx.send(f"✅ Max XP per message: **{gdata['settings']['xp_max']}**.")
    else:
        await ctx.send("❌ Usage: `!setxp min <n>` or `!setxp max <n>`")

@bot.command(name="setxpcooldown")
async def prefix_setxpcooldown(ctx, seconds: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["xp_cooldown"] = max(1, seconds)
    save_data(data)
    await ctx.send(f"✅ XP cooldown: **{seconds}s** between XP gains.")

# ── Info commands ──────────────────────────────────────────────────────────────
@bot.command(name="ping")
async def prefix_ping(ctx):
    await ctx.send(f"🏓 Pong! Latency: **{round(bot.latency*1000)}ms**")

@bot.command(name="botinfo")
async def prefix_botinfo(ctx):
    embed = discord.Embed(title=f"🤖 About {BOT_NAME}", color=BOT_COLOR)
    embed.add_field(name="Bot",      value=str(bot.user))
    embed.add_field(name="Servers",  value=str(len(bot.guilds)))
    embed.add_field(name="Latency",  value=f"{round(bot.latency*1000)}ms")
    embed.add_field(name="Commands", value=str(len(bot.commands)))
    if bot.user: embed.set_thumbnail(url=bot.user.display_avatar.url)
    await ctx.send(embed=embed)

@bot.command(name="userinfo")
async def prefix_userinfo(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed  = discord.Embed(title=str(member), color=member.color if member.color.value else BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="ID",       value=str(member.id))
    embed.add_field(name="Nickname", value=member.nick or "None")
    embed.add_field(name="Joined",   value=member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?")
    embed.add_field(name="Created",  value=member.created_at.strftime("%Y-%m-%d"))
    embed.add_field(name="Roles",    value=str(len(member.roles)-1))
    await ctx.send(embed=embed)

@bot.command(name="avatar")
async def prefix_avatar(ctx, member: discord.Member = None):
    member = member or ctx.author
    embed  = discord.Embed(title=f"{member.display_name}'s Avatar", color=BOT_COLOR)
    embed.set_image(url=member.display_avatar.url)
    await ctx.send(embed=embed)

@bot.command(name="roleinfo")
async def prefix_roleinfo(ctx, role: discord.Role):
    embed = discord.Embed(title=role.name, color=role.color)
    embed.add_field(name="ID",          value=str(role.id))
    embed.add_field(name="Members",     value=str(len(role.members)))
    embed.add_field(name="Hoisted",     value=str(role.hoist))
    embed.add_field(name="Mentionable", value=str(role.mentionable))
    embed.add_field(name="Position",    value=str(role.position))
    await ctx.send(embed=embed)

# ── Moderation ─────────────────────────────────────────────────────────────────
@bot.command(name="warn")
@commands.has_permissions(manage_messages=True)
async def prefix_warn(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    udata = get_user_data(gdata, member.id)
    udata["warnings"] = udata.get("warnings", 0) + 1
    save_data(data)
    await ctx.send(f"⚠️ **{member.display_name}** warned. Reason: {reason} (Total: {udata['warnings']})")
    try: await member.send(f"You were warned in **{ctx.guild.name}**: {reason}")
    except: pass

@bot.command(name="warnings")
async def prefix_warnings(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = get_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    await ctx.send(f"⚠️ **{member.display_name}** has **{udata.get('warnings',0)}** warning(s).")

@bot.command(name="clearwarnings")
@commands.has_permissions(manage_messages=True)
async def prefix_clearwarnings(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    udata = get_user_data(gdata, member.id)
    udata["warnings"] = 0
    save_data(data)
    await ctx.send(f"✅ Cleared all warnings for **{member.display_name}**.")

@bot.command(name="mute")
@commands.has_permissions(manage_roles=True)
async def prefix_mute(ctx, member: discord.Member, minutes: int = 0, *, reason: str = "No reason"):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    mr_id = gdata["settings"].get("mute_role")
    if not mr_id: return await ctx.send("❌ No mute role set. Use `!setmuterole @role`.")
    role = ctx.guild.get_role(int(mr_id))
    if not role: return await ctx.send("❌ Mute role not found.")
    await member.add_roles(role, reason=reason)
    await ctx.send(f"🔇 **{member.display_name}** muted. Reason: {reason}" + (f" ({minutes}min)" if minutes else ""))
    if minutes > 0:
        await asyncio.sleep(minutes * 60)
        await member.remove_roles(role, reason="Auto-unmute")

@bot.command(name="unmute")
@commands.has_permissions(manage_roles=True)
async def prefix_unmute(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    mr_id = gdata["settings"].get("mute_role")
    if not mr_id: return await ctx.send("❌ No mute role set.")
    role  = ctx.guild.get_role(int(mr_id))
    if not role: return await ctx.send("❌ Mute role not found.")
    await member.remove_roles(role)
    await ctx.send(f"🔊 **{member.display_name}** unmuted.")

@bot.command(name="kick")
@commands.has_permissions(kick_members=True)
async def prefix_kick(ctx, member: discord.Member, *, reason: str = "No reason"):
    await member.kick(reason=reason)
    await ctx.send(f"👢 **{member.display_name}** kicked. Reason: {reason}")

@bot.command(name="ban")
@commands.has_permissions(ban_members=True)
async def prefix_ban(ctx, member: discord.Member, *, reason: str = "No reason"):
    await member.ban(reason=reason, delete_message_days=1)
    await ctx.send(f"🔨 **{member.display_name}** banned. Reason: {reason}")

@bot.command(name="unban")
@commands.has_permissions(ban_members=True)
async def prefix_unban(ctx, user_id: int):
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.unban(user)
        await ctx.send(f"✅ Unbanned **{user}**.")
    except discord.NotFound:
        await ctx.send("❌ User not found or not banned.")

@bot.command(name="purge")
@commands.has_permissions(manage_messages=True)
async def prefix_purge(ctx, *args):
    if not args:
        return await ctx.send(
            "❌ Usage:\n"
            "`!purge <n>` — Delete N messages\n"
            "`!purge bot [n]` — Delete bot messages\n"
            "`!purge user @user [n]` — Delete user's messages\n"
            "`!purge contains <text>` — Delete by keyword"
        )
    subcommand = args[0].lower()

    if subcommand == "bot":
        limit = min(int(args[1]) if len(args) > 1 else 200, 500)
        deleted = await ctx.channel.purge(limit=limit, check=lambda m: m.author.bot)
        await ctx.send(f"🤖 Deleted **{len(deleted)}** bot message(s).", delete_after=5)

    elif subcommand == "user":
        target = ctx.message.mentions[0] if ctx.message.mentions else None
        if not target: return await ctx.send("❌ Mention a user: `!purge user @user [n]`")
        limit   = min(int(args[-1]) if len(args) > 2 else 200, 500)
        deleted = await ctx.channel.purge(limit=limit, check=lambda m: m.author.id == target.id)
        await ctx.send(f"🗑️ Deleted **{len(deleted)}** message(s) from **{target.display_name}**.", delete_after=5)

    elif subcommand == "contains":
        if len(args) < 2: return await ctx.send("❌ Usage: `!purge contains <keyword>`")
        keyword = " ".join(args[1:]).lower()
        deleted = await ctx.channel.purge(limit=500, check=lambda m: keyword in m.content.lower())
        await ctx.send(f"🗑️ Deleted **{len(deleted)}** message(s) containing `{keyword}`.", delete_after=5)

    else:
        try:    count = min(max(int(args[0]), 1), 100)
        except: return await ctx.send("❌ Unknown subcommand. Options: `bot` `user` `contains` or a number.")
        deleted = await ctx.channel.purge(limit=count + 1)
        await ctx.send(f"🗑️ Deleted **{len(deleted)-1}** messages.", delete_after=5)

@bot.command(name="setmuterole")
async def prefix_setmuterole(ctx, role: discord.Role):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["mute_role"] = str(role.id)
    save_data(data)
    await ctx.send(f"✅ Mute role set to **{role.name}**.")

# ── AutoMod settings ───────────────────────────────────────────────────────────
@bot.command(name="automod")
async def prefix_automod(ctx, state: str):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    enabled = state.lower() in ("on", "true", "enable", "1")
    gdata["settings"]["automod_enabled"] = enabled
    save_data(data)
    await ctx.send(f"✅ AutoMod **{'enabled' if enabled else 'disabled'}**.")

@bot.command(name="addword")
async def prefix_addword(ctx, *, word: str):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    words = gdata["settings"].setdefault("automod_banned_words", [])
    if word.lower() in words: return await ctx.send("ℹ️ Word already banned.")
    words.append(word.lower())
    save_data(data)
    await ctx.send(f"✅ Added `{word}` to banned words.")

@bot.command(name="removeword")
async def prefix_removeword(ctx, *, word: str):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    words = gdata["settings"].get("automod_banned_words", [])
    if word.lower() not in words: return await ctx.send("ℹ️ Word not in list.")
    words.remove(word.lower())
    save_data(data)
    await ctx.send(f"✅ Removed `{word}` from banned words.")

@bot.command(name="listwords")
async def prefix_listwords(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    words = gdata["settings"].get("automod_banned_words", [])
    if not words: return await ctx.send("ℹ️ No banned words.")
    await ctx.send(embed=discord.Embed(
        title="🚫 Banned Words", description=", ".join(f"`{w}`" for w in words), color=BOT_COLOR))

@bot.command(name="setmaxmentions")
async def prefix_setmaxmentions(ctx, n: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["automod_max_mentions"] = n
    save_data(data)
    await ctx.send(f"✅ Max mentions per message: **{n}**.")

@bot.command(name="setmaxcaps")
async def prefix_setmaxcaps(ctx, pct: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["automod_max_caps_pct"] = pct
    save_data(data)
    await ctx.send(f"✅ Max caps percentage: **{pct}%**.")

@bot.command(name="setlogchannel")
async def prefix_setlogchannel(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["log_channel"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Log channel set to {channel.mention}.")

# ── Direct Message (DM) Commands ──────────────────────────────────────────────
@bot.command(name="dm")
async def prefix_dm(ctx, member: discord.Member = None, *, message: str = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not can_use_dm_commands(ctx.author, ctx.guild, gdata):
        return await ctx.send("🚫 Permission Denied — You don't have permission to use DM commands.")
    
    if member is None:
        return await ctx.send("❌ Please mention a member to DM!\nUsage: `!dm @member Your message here`")
    
    if message is None:
        return await ctx.send("❌ Please provide a message to send!\nUsage: `!dm @member Your message here`")
    
    try:
        full_message = f"**Message from {ctx.author.display_name}** (Server: {ctx.guild.name})\n\n{message}"
        await member.send(full_message)
        await ctx.send(f"✅ Message sent to **{member.display_name}**!")
        log.info(f"DM sent by {ctx.author} to {member}: {message[:50]}...")
    except discord.Forbidden:
        await ctx.send(f"❌ Cannot DM **{member.display_name}**! They have DMs disabled or bot is blocked.")
    except Exception as e:
        await ctx.send(f"❌ Failed to send message: {e}")

@bot.command(name="dmrole")
async def prefix_dmrole(ctx, role: discord.Role, *, message: str = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("🚫 Permission Denied — !dmrole can only be used by the server owner.")
    
    if message is None:
        return await ctx.send("❌ Please provide a message to send!\nUsage: `!dmrole @role Your message here`")
    
    members_with_role = [m for m in ctx.guild.members if role in m.roles and not m.bot]
    
    if not members_with_role:
        return await ctx.send(f"❌ No members found with role **{role.name}**.")
    
    confirm_msg = await ctx.send(f"⚠️ **WARNING:** You are about to DM **{len(members_with_role)}** members with role **{role.name}**. Continue?\n\nReact with ✅ to confirm or ❌ to cancel.")
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
    
    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ DM cancelled.")
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Command cancelled.")
    
    await ctx.send(f"📨 Started sending DMs to **{len(members_with_role)}** members with role **{role.name}**...")
    
    full_message = f"**Message from {ctx.author.display_name}** (Server: {ctx.guild.name})\nTarget Role: {role.name}\n\n{message}"
    
    success_count = 0
    fail_count = 0
    status_msg = await ctx.send(f"📨 Progress: 0/{len(members_with_role)}")
    
    for i, member in enumerate(members_with_role):
        try:
            await member.send(full_message)
            success_count += 1
        except:
            fail_count += 1
        
        if (i + 1) % 10 == 0:
            await status_msg.edit(content=f"📨 Progress: {i+1}/{len(members_with_role)} (✅ {success_count} | ❌ {fail_count})")
        
        await asyncio.sleep(0.5)
    
    await status_msg.edit(content=f"✅ DM Role completed! Success: **{success_count}** | Failed: **{fail_count}** to **{role.name}** members")
    log.info(f"DM Role completed by {ctx.author} for role {role.name}: {success_count} success, {fail_count} failed")

@bot.command(name="dmall")
async def prefix_dmall(ctx, *, message: str = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("🚫 Permission Denied — !dmall can only be used by the server owner.")
    
    if message is None:
        return await ctx.send("❌ Please provide a message to send!\nUsage: `!dmall Your message here`")
    
    members_to_dm = [m for m in ctx.guild.members if not m.bot]
    
    if not members_to_dm:
        return await ctx.send("❌ No members to DM!")
    
    confirm_msg = await ctx.send(f"⚠️ **WARNING:** You are about to DM **{len(members_to_dm)}** members. This may take a while and could be considered spam.\n\nReact with ✅ to confirm or ❌ to cancel.")
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
    
    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ DM all cancelled.")
        
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Command cancelled.")
    
    await ctx.send(f"📨 Started sending DMs to **{len(members_to_dm)}** members. This may take a while...")
    
    success_count = 0
    fail_count = 0
    failed_members = []
    
    full_message = f"**Announcement from {ctx.author.display_name}** (Server: {ctx.guild.name})\n\n{message}"
    
    status_msg = await ctx.send(f"📨 Progress: 0/{len(members_to_dm)}")
    
    for i, member in enumerate(members_to_dm):
        try:
            await member.send(full_message)
            success_count += 1
        except:
            fail_count += 1
            failed_members.append(member.name)
        
        if (i + 1) % 10 == 0:
            await status_msg.edit(content=f"📨 Progress: {i+1}/{len(members_to_dm)} (✅ {success_count} | ❌ {fail_count})")
        
        await asyncio.sleep(0.5)
    
    await status_msg.edit(content=f"✅ DM All completed! Success: **{success_count}** | Failed: **{fail_count}**")
    
    if failed_members and len(failed_members) <= 10:
        await ctx.send(f"❌ Failed to DM: {', '.join(failed_members)}")
    elif failed_members:
        await ctx.send(f"❌ Failed to DM {len(failed_members)} members (DMs disabled or blocked)")
    
    log.info(f"DM All completed by {ctx.author}: {success_count} success, {fail_count} failed")

# ══════════════════════════════════════════════════════════════════════════════
#  TICKET SYSTEM COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@bot.command(name="setup")
async def prefix_setup(ctx, channel: discord.TextChannel = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission. Server owner or admin only.")
    
    if not channel:
        return await ctx.send("❌ Please specify a channel: `!setup #channel`")
    
    gdata["settings"]["ticket_setup_channel"] = str(channel.id)
    save_data(data)
    
    embed = discord.Embed(
        title="🎫 Ticket System",
        description=(
            "Need help? Create a ticket and our support team will assist you!\n\n"
            "**How it works:**\n"
            "1. Click the button below\n"
            "2. Select a category from the dropdown\n"
            "3. A private channel will be created for you\n"
            "4. Describe your issue in the ticket channel\n\n"
            "**Support team:** Please respond as quickly as possible!"
        ),
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=ctx.guild.name, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
    embed.set_thumbnail(url=ctx.guild.icon.url if ctx.guild.icon else None)
    
    view = discord.ui.View(timeout=None)
    view.add_item(TicketButton(label="Create Ticket", style=discord.ButtonStyle.primary, emoji="🎫"))
    
    message = await channel.send(embed=embed, view=view)
    
    gdata["settings"]["ticket_panel_message_id"] = str(message.id)
    save_data(data)
    
    await ctx.send(f"✅ Ticket system setup complete! Panel created in {channel.mention}")

@bot.command(name="addticketcat")
async def prefix_addticketcat(ctx, category: discord.CategoryChannel, name: str, *, description: str = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission. Server owner or admin only.")
    
    ticket_categories = gdata["settings"].setdefault("ticket_categories", {})
    
    if name in ticket_categories:
        return await ctx.send(f"❌ Category `{name}` already exists!")
    
    ticket_categories[name] = {
        "category_id": str(category.id),
        "enabled": True,
        "description": description or f"Create a ticket for {name}",
        "emoji": "🎫",
        "prefix": name.lower().replace(" ", "-")[:10]
    }
    
    save_data(data)
    await ctx.send(f"✅ Ticket category **{name}** added to category {category.mention}!")

@bot.command(name="removeticketcat")
async def prefix_removeticketcat(ctx, name: str):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    ticket_categories = gdata["settings"].get("ticket_categories", {})
    
    if name not in ticket_categories:
        return await ctx.send(f"❌ Category `{name}` not found!")
    
    del ticket_categories[name]
    save_data(data)
    await ctx.send(f"✅ Ticket category **{name}** removed!")

@bot.command(name="listticketcats")
async def prefix_listticketcats(ctx):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    ticket_categories = gdata["settings"].get("ticket_categories", {})
    
    if not ticket_categories:
        return await ctx.send("ℹ️ No ticket categories configured. Use `!addticketcat` to add one.")
    
    embed = discord.Embed(
        title="📋 Ticket Categories",
        description="Configured ticket categories:",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    for cat_name, cat_config in ticket_categories.items():
        category = ctx.guild.get_channel(int(cat_config["category_id"]))
        status = "✅ Enabled" if cat_config.get("enabled", True) else "❌ Disabled"
        embed.add_field(
            name=f"🎫 {cat_name}",
            value=f"**Category:** {category.mention if category else 'Deleted'}\n"
                  f"**Status:** {status}\n"
                  f"**Prefix:** `{cat_config.get('prefix', 'ticket')}`\n"
                  f"**Description:** {cat_config.get('description', 'No description')}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="toggleticketcat")
async def prefix_toggleticketcat(ctx, name: str):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    ticket_categories = gdata["settings"].get("ticket_categories", {})
    
    if name not in ticket_categories:
        return await ctx.send(f"❌ Category `{name}` not found!")
    
    current = ticket_categories[name].get("enabled", True)
    ticket_categories[name]["enabled"] = not current
    save_data(data)
    
    status = "enabled" if not current else "disabled"
    await ctx.send(f"✅ Category **{name}** has been **{status}**!")

@bot.command(name="setcatemoji")
async def prefix_setcatemoji(ctx, name: str, emoji: str):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    ticket_categories = gdata["settings"].get("ticket_categories", {})
    
    if name not in ticket_categories:
        return await ctx.send(f"❌ Category `{name}` not found!")
    
    ticket_categories[name]["emoji"] = emoji
    save_data(data)
    await ctx.send(f"✅ Emoji for **{name}** set to {emoji}")

@bot.command(name="addticketmanager")
async def prefix_addticketmanager(ctx, role: discord.Role):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    manager_roles = gdata["settings"].setdefault("ticket_manager_roles", [])
    
    if role.id in manager_roles:
        return await ctx.send(f"ℹ️ {role.mention} is already a ticket manager!")
    
    manager_roles.append(role.id)
    save_data(data)
    await ctx.send(f"✅ {role.mention} can now manage tickets!")

@bot.command(name="removeticketmanager")
async def prefix_removeticketmanager(ctx, role: discord.Role):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    manager_roles = gdata["settings"].get("ticket_manager_roles", [])
    
    if role.id not in manager_roles:
        return await ctx.send(f"ℹ️ {role.mention} is not a ticket manager!")
    
    manager_roles.remove(role.id)
    save_data(data)
    await ctx.send(f"✅ Removed {role.mention} from ticket managers!")

@bot.command(name="setticketembed")
async def prefix_setticketembed(ctx, title: str = None, *, description: str = None):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    if title:
        gdata["settings"]["ticket_default_title"] = title
    if description:
        gdata["settings"]["ticket_default_description"] = description
    
    save_data(data)
    
    await ctx.send(f"✅ Ticket embed updated!\n**Title:** {gdata['settings']['ticket_default_title']}\n**Description:** {gdata['settings']['ticket_default_description'][:100]}...")

@bot.command(name="settickettranscript")
async def prefix_settickettranscript(ctx, channel: discord.TextChannel):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    gdata["settings"]["ticket_transcripts"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Ticket transcripts will be sent to {channel.mention}")

@bot.command(name="close")
async def prefix_close(ctx):
    if not ctx.channel.name.startswith("ticket-"):
        return await ctx.send("❌ This is not a ticket channel!")
    
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if ctx.channel.id not in active_tickets:
        return await ctx.send("❌ This ticket is not active!")
    
    support_role_id = gdata["settings"].get("ticket_support_role")
    manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
    
    has_permission = (
        (support_role_id and support_role_id in [r.id for r in ctx.author.roles]) or
        any(rid in [r.id for r in ctx.author.roles] for rid in manager_role_ids) or
        ctx.author.guild_permissions.administrator or
        ctx.author.id == ctx.guild.owner_id
    )
    
    if not has_permission:
        return await ctx.send("❌ You don't have permission to close this ticket!")
    
    await ctx.send("🔒 Closing ticket in 5 seconds...")
    await asyncio.sleep(5)
    
    transcript_channel_id = gdata["settings"].get("ticket_transcripts")
    if transcript_channel_id:
        transcript_channel = ctx.guild.get_channel(int(transcript_channel_id))
        if transcript_channel:
            messages = []
            async for msg in ctx.channel.history(limit=200, oldest_first=True):
                timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
                messages.append(f"[{timestamp}] {msg.author.name}: {msg.content}")
            
            transcript_text = "\n".join(messages)
            transcript_file = discord.File(
                io.BytesIO(transcript_text.encode()),
                filename=f"transcript_{ctx.channel.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )
            
            embed = discord.Embed(
                title="📄 Ticket Transcript",
                description=f"**Channel:** {ctx.channel.name}\n"
                           f"**Closed by:** {ctx.author.mention}\n"
                           f"**Messages:** {len(messages)}",
                color=BOT_COLOR,
                timestamp=datetime.utcnow()
            )
            await transcript_channel.send(embed=embed, file=transcript_file)
    
    await ctx.channel.delete(reason=f"Ticket closed by {ctx.author}")
    
    if ctx.channel.id in active_tickets:
        del active_tickets[ctx.channel.id]
    
    log_channel_id = gdata["settings"].get("log_channel")
    if log_channel_id:
        log_ch = ctx.guild.get_channel(int(log_channel_id))
        if log_ch:
            log_embed = discord.Embed(
                title="🎫 Ticket Closed",
                description=f"**Closed by:** {ctx.author.mention}\n"
                           f"**Channel:** #{ctx.channel.name}",
                color=0xFF0000,
                timestamp=datetime.utcnow()
            )
            await log_ch.send(embed=log_embed)

@bot.command(name="rename")
async def prefix_rename(ctx, *, new_name: str):
    if not ctx.channel.name.startswith("ticket-"):
        return await ctx.send("❌ This is not a ticket channel!")
    
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    support_role_id = gdata["settings"].get("ticket_support_role")
    manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
    
    has_permission = (
        (support_role_id and support_role_id in [r.id for r in ctx.author.roles]) or
        any(rid in [r.id for r in ctx.author.roles] for rid in manager_role_ids) or
        ctx.author.guild_permissions.administrator or
        ctx.author.id == ctx.guild.owner_id
    )
    
    if not has_permission:
        return await ctx.send("❌ You don't have permission to rename tickets!")
    
    clean_name = new_name.lower().replace(" ", "-")[:25]
    try:
        await ctx.channel.edit(name=f"ticket-{clean_name}")
        await ctx.send(f"✅ Ticket renamed to `ticket-{clean_name}`")
    except Exception as e:
        await ctx.send(f"❌ Failed to rename: {e}")

@bot.command(name="delete")
async def prefix_delete(ctx):
    if not ctx.channel.name.startswith("ticket-"):
        return await ctx.send("❌ This is not a ticket channel!")
    
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    support_role_id = gdata["settings"].get("ticket_support_role")
    manager_role_ids = gdata["settings"].get("ticket_manager_roles", [])
    
    has_permission = (
        (support_role_id and support_role_id in [r.id for r in ctx.author.roles]) or
        any(rid in [r.id for r in ctx.author.roles] for rid in manager_role_ids) or
        ctx.author.guild_permissions.administrator or
        ctx.author.id == ctx.guild.owner_id
    )
    
    if not has_permission:
        return await ctx.send("❌ You don't have permission to delete tickets!")
    
    confirm_msg = await ctx.send("⚠️ **WARNING:** This will permanently delete this ticket channel!\nReact with ✅ to confirm or ❌ to cancel.")
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
    
    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ Deletion cancelled.")
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Deletion cancelled.")
    
    await ctx.send("🗑️ Deleting ticket channel...")
    await asyncio.sleep(2)
    
    if ctx.channel.id in active_tickets:
        del active_tickets[ctx.channel.id]
    
    log_channel_id = gdata["settings"].get("log_channel")
    if log_channel_id:
        log_ch = ctx.guild.get_channel(int(log_channel_id))
        if log_ch:
            log_embed = discord.Embed(
                title="🗑️ Ticket Deleted",
                description=f"**Deleted by:** {ctx.author.mention}\n"
                           f"**Channel:** #{ctx.channel.name}",
                color=0xFF0000,
                timestamp=datetime.utcnow()
            )
            await log_ch.send(embed=log_embed)
    
    await ctx.channel.delete(reason=f"Ticket deleted by {ctx.author}")

@bot.command(name="refreshpanel")
async def prefix_refreshpanel(ctx):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    
    panel_channel_id = gdata["settings"].get("ticket_setup_channel")
    panel_message_id = gdata["settings"].get("ticket_panel_message_id")
    
    if not panel_channel_id or not panel_message_id:
        return await ctx.send("❌ No ticket panel configured. Use `!setup #channel` first.")
    
    channel = ctx.guild.get_channel(int(panel_channel_id))
    if not channel:
        return await ctx.send("❌ Ticket panel channel not found!")
    
    try:
        message = await channel.fetch_message(int(panel_message_id))
        view = discord.ui.View(timeout=None)
        view.add_item(TicketButton())
        await message.edit(view=view)
        await ctx.send(f"✅ Ticket panel refreshed in {channel.mention}!")
    except discord.NotFound:
        await ctx.send("❌ Ticket panel message not found. Please run `!setup` again.")
    except Exception as e:
        await ctx.send(f"❌ Failed to refresh panel: {e}")
@bot.command(name="checkroles")
async def prefix_checkroles(ctx):
    """Check current role threshold settings"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    thresholds = gdata["settings"].get("role_thresholds", {})
    
    if not thresholds:
        await ctx.send("❌ No role thresholds set! Use `!setrole @Official 100` to set one.")
        return
    
    embed = discord.Embed(title="📋 Current Role Thresholds", color=BOT_COLOR)
    for role_id, req in thresholds.items():
        role = ctx.guild.get_role(int(role_id))
        if role:
            embed.add_field(name=role.name, value=f"Required: **{req:,}** messages", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="resetsettings")
async def prefix_resetsettings(ctx, confirmation: str = None):
    """Reset ALL bot settings for this server (Server Owner only)
    Usage: !resetsettings confirm
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    # Check if user is server owner or has admin permissions
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can reset server settings.")
    
    if confirmation != "confirm":
        embed = discord.Embed(
            title="⚠️ WARNING: Reset Server Settings",
            description=(
                "This will **permanently reset ALL bot settings** for this server, including:\n\n"
                "• Role thresholds (!setrole settings)\n"
                "• VC role thresholds (!setvcrole settings)\n"
                "• Conditional paths (!addpath settings)\n"
                "• Welcome/Goodbye channels and messages\n"
                "• AutoMod settings (banned words, max mentions, caps)\n"
                "• Spam interval settings\n"
                "• Channel whitelist/blacklist\n"
                "• Owner list (bot owners)\n"
                "• Milestone settings\n"
                "• XP settings (min/max, cooldown)\n"
                "• Log channel configuration\n"
                "• Mute role configuration\n"
                "• DM permissions (!dmaddrole settings)\n"
                "• Ticket system configuration\n\n"
                "⚠️ **User stats (messages, VC time, XP) WILL NOT be affected.**\n\n"
                f"To confirm, type: `!resetsettings confirm`"
            ),
            color=0xFF4444,
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Requested by {ctx.author.display_name}")
        return await ctx.send(embed=embed)
    
    # Save current user data before resetting settings
    user_data = gdata.get("users", {})
    
    # Reset settings to defaults
    gdata["settings"] = _default_settings()
    
    # Restore user data (preserve stats)
    gdata["users"] = user_data
    
    save_data(data)
    
    embed = discord.Embed(
        title="✅ Server Settings Reset",
        description=(
            "All bot settings for this server have been reset to default.\n\n"
            "**Preserved:** User stats (messages, VC time, XP, streaks)\n"
            "**Reset:** All role thresholds, welcome system, AutoMod, etc.\n\n"
            "Use `!settings` to view current settings."
        ),
        color=0x57F287,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Reset by {ctx.author.display_name}")
    await ctx.send(embed=embed)
    log.info(f"Server settings reset in guild {ctx.guild.id} by {ctx.author}")

@bot.command(name="resetallsettings")
async def prefix_resetallsettings(ctx, confirmation: str = None):
    """Reset ALL bot settings AND user stats for this server (Server Owner only)
    Usage: !resetallsettings confirm
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    # Check if user is server owner or has admin permissions
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can reset server settings.")
    
    if confirmation != "confirm":
        embed = discord.Embed(
            title="⚠️⚠️⚠️ DANGER ZONE ⚠️⚠️⚠️",
            description=(
                "This will **COMPLETELY WIPE EVERYTHING** for this server, including:\n\n"
                "**❌ ALL SETTINGS WILL BE RESET:**\n"
                "• Role thresholds (!setrole settings)\n"
                "• VC role thresholds\n"
                "• Conditional paths\n"
                "• Welcome/Goodbye system\n"
                "• AutoMod configuration\n"
                "• Channel whitelist/blacklist\n"
                "• All other bot settings\n\n"
                "**❌ ALL USER STATS WILL BE DELETED:**\n"
                "• Message counts (total, daily, weekly, monthly)\n"
                "• Voice time stats\n"
                "• XP and levels\n"
                "• Streaks and activity history\n"
                "• Milestones achieved\n\n"
                "⚠️ **THIS ACTION CANNOT BE UNDONE!** ⚠️\n\n"
                f"To confirm, type: `!resetallsettings confirm`"
            ),
            color=0xFF0000,
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Requested by {ctx.author.display_name}")
        return await ctx.send(embed=embed)
    
    # Double confirmation
    confirm_msg = await ctx.send(
        "⚠️ **FINAL WARNING** ⚠️\n"
        f"{ctx.author.mention}, are you **ABSOLUTELY SURE** you want to reset EVERYTHING?\n\n"
        "This will delete ALL settings and ALL user stats permanently.\n\n"
        "Type `YES` within 30 seconds to confirm."
    )
    
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content == "YES"
    
    try:
        await bot.wait_for("message", timeout=30.0, check=check)
    except asyncio.TimeoutError:
        return await ctx.send("❌ Reset cancelled (timeout).")
    
    # Complete reset - remove entire guild data
    gid = str(ctx.guild.id)
    if gid in data:
        # Create fresh settings with empty users
        data[gid] = {"settings": _default_settings(), "users": {}}
    
    save_data(data)
    
    embed = discord.Embed(
        title="🗑️ Complete Server Reset",
        description=(
            "**EVERYTHING has been reset** for this server.\n\n"
            "• All settings restored to defaults\n"
            "• All user stats deleted\n\n"
            "The bot is now in a clean state for this server."
        ),
        color=0xFF0000,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Reset by {ctx.author.display_name}")
    await ctx.send(embed=embed)
    log.warning(f"COMPLETE SERVER RESET in guild {ctx.guild.id} by {ctx.author}")

@bot.command(name="backupsettings")
async def prefix_backupsettings(ctx):
    """Create a backup of current server settings (Server Owner only)"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can backup settings.")
    
    # Create backup of settings
    backup = {
        "guild_id": ctx.guild.id,
        "guild_name": ctx.guild.name,
        "timestamp": datetime.now().isoformat(),
        "settings": gdata["settings"].copy()
    }
    
    # Save backup to file
    backup_filename = f"backup_{ctx.guild.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_filename, "w") as f:
        json.dump(backup, f, indent=2)
    
    # Send backup file
    embed = discord.Embed(
        title="💾 Settings Backup Created",
        description=f"Backup created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\nUse `!restoresettings` to restore this backup.",
        color=0x57F287,
        timestamp=datetime.utcnow()
    )
    await ctx.send(embed=embed, file=discord.File(backup_filename))
    
    # Clean up file after sending
    os.remove(backup_filename)
    log.info(f"Settings backup created for guild {ctx.guild.id} by {ctx.author}")

@bot.command(name="restoresettings")
async def prefix_restoresettings(ctx):
    """Restore server settings from a backup file (Server Owner only)"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can restore settings.")
    
    if not ctx.message.attachments:
        return await ctx.send("❌ Please attach a backup JSON file with this command.\nUsage: `!restoresettings` (with file attached)")
    
    attachment = ctx.message.attachments[0]
    
    if not attachment.filename.endswith('.json'):
        return await ctx.send("❌ Please attach a valid JSON backup file.")
    
    try:
        # Download and parse backup file
        backup_content = await attachment.read()
        backup = json.loads(backup_content)
        
        # Validate backup
        if "guild_id" not in backup or backup["guild_id"] != ctx.guild.id:
            return await ctx.send("❌ This backup file is from a different server!")
        
        if "settings" not in backup:
            return await ctx.send("❌ Invalid backup file format.")
        
        # Confirm restore
        confirm_embed = discord.Embed(
            title="⚠️ Restore Settings Backup",
            description=(
                f"This will restore settings from backup created on: **{backup.get('timestamp', 'Unknown')}**\n\n"
                "**This will overwrite current settings.**\n\n"
                f"Type `CONFIRM` within 30 seconds to proceed."
            ),
            color=0xFFA500
        )
        await ctx.send(embed=confirm_embed)
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel and m.content == "CONFIRM"
        
        try:
            await bot.wait_for("message", timeout=30.0, check=check)
        except asyncio.TimeoutError:
            return await ctx.send("❌ Restore cancelled (timeout).")
        
        # Save current user data
        user_data = gdata.get("users", {})
        
        # Restore settings
        gdata["settings"] = backup["settings"]
        
        # Restore user data
        gdata["users"] = user_data
        
        save_data(data)
        
        embed = discord.Embed(
            title="✅ Settings Restored",
            description=f"Settings restored from backup created on: {backup.get('timestamp', 'Unknown')}\n\nUser stats were preserved.",
            color=0x57F287,
            timestamp=datetime.utcnow()
        )
        await ctx.send(embed=embed)
        log.info(f"Settings restored for guild {ctx.guild.id} by {ctx.author}")
        
    except json.JSONDecodeError:
        await ctx.send("❌ Invalid JSON file. Please provide a valid backup.")
    except Exception as e:
        await ctx.send(f"❌ Failed to restore backup: {e}")

@bot.command(name="viewsettings")
async def prefix_viewsettings(ctx, category: str = None):
    """View detailed server settings (Admin only)
    Categories: roles, automod, welcome, tickets, xp, all
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ Permission Denied — Admin or bot owner only.")
    
    s = gdata["settings"]
    
    if category and category.lower() == "roles":
        embed = discord.Embed(title="📋 Role Settings", color=BOT_COLOR)
        
        # Message roles
        msg_roles = s.get("role_thresholds", {})
        if msg_roles:
            roles_text = ""
            for rid, req in sorted(msg_roles.items(), key=lambda x: x[1]):
                role = ctx.guild.get_role(int(rid))
                if role:
                    roles_text += f"• {role.mention} → **{req:,}** messages\n"
            embed.add_field(name="Message Roles", value=roles_text or "None", inline=False)
        else:
            embed.add_field(name="Message Roles", value="None set", inline=False)
        
        # VC roles
        vc_roles = s.get("vc_role_thresholds", {})
        if vc_roles:
            vc_text = ""
            for rid, req in sorted(vc_roles.items(), key=lambda x: x[1]):
                role = ctx.guild.get_role(int(rid))
                if role:
                    vc_text += f"• {role.mention} → **{req:,}** minutes\n"
            embed.add_field(name="Voice Roles", value=vc_text or "None", inline=False)
        else:
            embed.add_field(name="Voice Roles", value="None set", inline=False)
        
        # Conditional paths
        paths = s.get("conditional_paths", [])
        if paths:
            paths_text = ""
            for path in paths:
                trigger = ctx.guild.get_role(int(path["trigger_role_id"]))
                if trigger:
                    paths_text += f"• **{trigger.name}** → "
                    thresholds_text = []
                    for rid, req in path.get("thresholds", {}).items():
                        role = ctx.guild.get_role(int(rid))
                        if role:
                            thresholds_text.append(f"{role.name} ({req:,})")
                    paths_text += ", ".join(thresholds_text) + "\n"
            embed.add_field(name="Conditional Paths", value=paths_text or "None", inline=False)
        
        await ctx.send(embed=embed)
        
    elif category and category.lower() == "automod":
        embed = discord.Embed(title="🛡️ AutoMod Settings", color=BOT_COLOR)
        embed.add_field(name="Enabled", value="✅ Yes" if s.get("automod_enabled") else "❌ No", inline=True)
        embed.add_field(name="Max Mentions", value=str(s.get("automod_max_mentions", 5)), inline=True)
        embed.add_field(name="Max Caps %", value=f"{s.get('automod_max_caps_pct', 80)}%", inline=True)
        
        banned_words = s.get("automod_banned_words", [])
        embed.add_field(name="Banned Words", value=", ".join(f"`{w}`" for w in banned_words) or "None", inline=False)
        
        log_ch = s.get("log_channel")
        embed.add_field(name="Log Channel", value=f"<#{log_ch}>" if log_ch else "Not set", inline=False)
        
        await ctx.send(embed=embed)
        
    elif category and category.lower() == "welcome":
        embed = discord.Embed(title="👋 Welcome System", color=BOT_COLOR)
        
        welcome_chs = s.get("welcome_channels", [])
        embed.add_field(name="Welcome Channels", value="\n".join(f"<#{c}>" for c in welcome_chs) or "None", inline=False)
        
        goodbye_chs = s.get("goodbye_channels", [])
        embed.add_field(name="Goodbye Channels", value="\n".join(f"<#{c}>" for c in goodbye_chs) or "None", inline=False)
        
        embed.add_field(name="Welcome Embed", value="✅ Enabled" if s.get("welcome_use_embed") else "❌ Disabled", inline=True)
        embed.add_field(name="Goodbye Embed", value="✅ Enabled" if s.get("goodbye_use_embed") else "❌ Disabled", inline=True)
        embed.add_field(name="Auto-Delete", value=f"{s.get('welcome_delete_seconds', 0)} seconds", inline=True)
        
        embed.add_field(name="Welcome Message", value=s.get("welcome_message", "")[:100], inline=False)
        embed.add_field(name="Goodbye Message", value=s.get("goodbye_message", "")[:100], inline=False)
        
        await ctx.send(embed=embed)
        
    elif category and category.lower() == "tickets":
        embed = discord.Embed(title="🎫 Ticket System Settings", color=BOT_COLOR)
        
        setup_ch = s.get("ticket_setup_channel")
        embed.add_field(name="Setup Channel", value=f"<#{setup_ch}>" if setup_ch else "Not set", inline=False)
        
        transcript_ch = s.get("ticket_transcripts")
        embed.add_field(name="Transcript Channel", value=f"<#{transcript_ch}>" if transcript_ch else "Not set", inline=False)
        
        support_role = s.get("ticket_support_role")
        embed.add_field(name="Support Role", value=f"<@&{support_role}>" if support_role else "Not set", inline=False)
        
        manager_roles = s.get("ticket_manager_roles", [])
        embed.add_field(name="Manager Roles", value="\n".join(f"<@&{rid}>" for rid in manager_roles) or "None", inline=False)
        
        categories = s.get("ticket_categories", {})
        if categories:
            cats_text = ""
            for cat_name, cat_config in categories.items():
                status = "✅" if cat_config.get("enabled", True) else "❌"
                cats_text += f"{status} **{cat_name}**\n"
            embed.add_field(name="Categories", value=cats_text or "None", inline=False)
        
        await ctx.send(embed=embed)
        
    elif category and category.lower() == "xp":
        embed = discord.Embed(title="✨ XP Settings", color=BOT_COLOR)
        embed.add_field(name="XP Enabled", value="✅ Yes" if s.get("xp_enabled") else "❌ No", inline=True)
        embed.add_field(name="XP Range", value=f"{s.get('xp_min', 1)} - {s.get('xp_max', 5)} XP", inline=True)
        embed.add_field(name="XP Cooldown", value=f"{s.get('xp_cooldown', 10)} seconds", inline=True)
        embed.add_field(name="Streaks Enabled", value="✅ Yes" if s.get("streak_enabled") else "❌ No", inline=True)
        
        await ctx.send(embed=embed)
        
    else:
        # Show all settings summary
        embed = discord.Embed(
            title="⚙️ Server Settings Summary",
            description="Use `!viewsettings <category>` for detailed view\nCategories: `roles`, `automod`, `welcome`, `tickets`, `xp`",
            color=BOT_COLOR,
            timestamp=datetime.utcnow()
        )
        
        # General
        embed.add_field(name="Spam Interval", value=f"{s.get('spam_interval', 3)}s", inline=True)
        embed.add_field(name="Emoji Counting", value="✅ Yes" if s.get("count_emojis") else "❌ No", inline=True)
        embed.add_field(name="VC in Msg Roles", value="✅ Yes" if s.get("include_vc_in_roles") else "❌ No", inline=True)
        
        # Counts
        embed.add_field(name="Message Roles", value=str(len(s.get("role_thresholds", {}))), inline=True)
        embed.add_field(name="VC Roles", value=str(len(s.get("vc_role_thresholds", {}))), inline=True)
        embed.add_field(name="Conditional Paths", value=str(len(s.get("conditional_paths", []))), inline=True)
        
        # Channels
        embed.add_field(name="Welcome Channels", value=len(s.get("welcome_channels", [])), inline=True)
        embed.add_field(name="Goodbye Channels", value=len(s.get("goodbye_channels", [])), inline=True)
        embed.add_field(name="Log Channel", value="✅" if s.get("log_channel") else "❌", inline=True)
        
        # Features
        embed.add_field(name="AutoMod", value="✅ Enabled" if s.get("automod_enabled") else "❌ Disabled", inline=True)
        embed.add_field(name="XP System", value="✅ Enabled" if s.get("xp_enabled") else "❌ Disabled", inline=True)
        embed.add_field(name="Ticket System", value="✅ Configured" if s.get("ticket_setup_channel") else "❌ Not configured", inline=True)
        
        embed.add_field(name="Milestone Channel", value=f"<#{s['milestone_announce']}>" if s.get("milestone_announce") else "Not set", inline=False)
        
        embed.set_footer(text=f"Guild ID: {ctx.guild.id}")
        await ctx.send(embed=embed)
    
@bot.command(name="purgebot")
async def prefix_purgebot(ctx, limit: int = 100):
    """Delete bot messages in the current channel (Admin only)
    Usage: !purgebot [limit] - Default limit is 100 messages
    """
    # Check if user has permission
    if not (ctx.author.guild_permissions.manage_messages or is_owner(ctx.author, ctx.guild, get_guild_data(get_data(), ctx.guild.id))):
        return await ctx.send("❌ You need `Manage Messages` permission to use this command.")
    
    # Validate limit
    if limit < 1:
        return await ctx.send("❌ Limit must be at least 1.")
    if limit > 500:
        limit = 500
        await ctx.send("⚠️ Limit capped at 500 messages.")
    
    # Delete bot messages
    def is_bot(m):
        return m.author.bot
    
    deleted = await ctx.channel.purge(limit=limit, check=is_bot)
    
    await ctx.send(f"🤖 Deleted **{len(deleted)}** bot message(s).", delete_after=5)
    log.info(f"Purged {len(deleted)} bot messages in {ctx.guild.name}#{ctx.channel.name} by {ctx.author}")

@bot.command(name="serverbanner")
async def prefix_serverbanner(ctx):
    """Get the server's banner image (if exists)"""
    guild = ctx.guild
    
    if guild.banner:
        embed = discord.Embed(
            title=f"🖼️ {guild.name}'s Banner",
            color=BOT_COLOR,
            timestamp=datetime.utcnow()
        )
        embed.set_image(url=guild.banner.url)
        embed.set_footer(text=f"Server ID: {guild.id}")
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"❌ This server doesn't have a banner set.")

@bot.command(name="servericon")
async def prefix_servericon(ctx):
    """Get the server's icon (server logo)"""
    guild = ctx.guild
    
    if guild.icon:
        embed = discord.Embed(
            title=f"🖼️ {guild.name}'s Icon",
            color=BOT_COLOR,
            timestamp=datetime.utcnow()
        )
        embed.set_image(url=guild.icon.url)
        embed.set_footer(text=f"Server ID: {guild.id}")
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"❌ This server doesn't have an icon set.")

@bot.command(name="si", aliases=["serverinfo2", "guildinfo"])
async def prefix_si(ctx):
    """Display detailed server information"""
    guild = ctx.guild
    
    # Basic information
    embed = discord.Embed(
        title=f"📊 {guild.name}",
        color=guild.owner.color if guild.owner.color.value else BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    # Server icon and banner
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    if guild.banner:
        embed.set_image(url=guild.banner.url)
    
    # Owner and creation info
    embed.add_field(
        name="👑 Server Owner",
        value=f"{guild.owner.mention}\n{guild.owner.name}#{guild.owner.discriminator}" if guild.owner.discriminator != "0" else f"{guild.owner.mention}\n{guild.owner.name}",
        inline=True
    )
    embed.add_field(
        name="📅 Created On",
        value=f"<t:{int(guild.created_at.timestamp())}:D>\n<t:{int(guild.created_at.timestamp())}:R>",
        inline=True
    )
    embed.add_field(
        name="🆔 Server ID",
        value=f"`{guild.id}`",
        inline=True
    )
    
    # Statistics
    embed.add_field(
        name="👥 Members",
        value=f"**Total:** {guild.member_count}\n"
              f"**Humans:** {len([m for m in guild.members if not m.bot])}\n"
              f"**Bots:** {len([m for m in guild.members if m.bot])}",
        inline=True
    )
    embed.add_field(
        name="💬 Channels",
        value=f"**Text:** {len(guild.text_channels)}\n"
              f"**Voice:** {len(guild.voice_channels)}\n"
              f"**Categories:** {len(guild.categories)}\n"
              f"**Total:** {len(guild.channels)}",
        inline=True
    )
    embed.add_field(
        name="🎭 Roles",
        value=f"**Total:** {len(guild.roles)}\n"
              f"**Highest:** {guild.roles[-1].mention if guild.roles else 'None'}",
        inline=True
    )
    
    # Boost info
    if guild.premium_subscription_count > 0:
        embed.add_field(
            name="✨ Boosts",
            value=f"**Level:** {guild.premium_tier}\n"
                  f"**Boosts:** {guild.premium_subscription_count}\n"
                  f"**Boosters:** {len(guild.premium_subscribers)}",
            inline=True
        )
    else:
        embed.add_field(name="✨ Boosts", value="No boosts", inline=True)
    
    # Features
    features = []
    feature_map = {
        "ANIMATED_ICON": "🎨 Animated Icon",
        "BANNER": "🖼️ Banner",
        "COMMUNITY": "🏘️ Community",
        "DISCOVERABLE": "🔍 Discoverable",
        "FEATURABLE": "⭐ Featurable",
        "INVITE_SPLASH": "💧 Invite Splash",
        "MEMBER_VERIFICATION_GATE_ENABLED": "✅ Verification Gate",
        "NEWS": "📰 News Channels",
        "PARTNERED": "🤝 Partnered",
        "PREVIEW_ENABLED": "👀 Preview Enabled",
        "ROLE_ICONS": "🎭 Role Icons",
        "SEVEN_DAY_THREAD_ARCHIVE": "📌 7-Day Thread Archive",
        "THREE_DAY_THREAD_ARCHIVE": "📌 3-Day Thread Archive",
        "THREADS_ENABLED": "🧵 Threads",
        "TICKETED_EVENTS_ENABLED": "🎟️ Ticketed Events",
        "VANITY_URL": "✨ Vanity URL",
        "VERIFIED": "✅ Verified",
        "VIP_REGIONS": "🌟 VIP Voice Regions",
        "WELCOME_SCREEN_ENABLED": "👋 Welcome Screen"
    }
    
    for feature in guild.features:
        if feature in feature_map:
            features.append(feature_map[feature])
    
    if features:
        embed.add_field(
            name="🌟 Features",
            value="\n".join(features[:10]) + ("\n*and more...*" if len(features) > 10 else ""),
            inline=False
        )
    
    # Security levels
    verification_levels = {
        discord.VerificationLevel.none: "None",
        discord.VerificationLevel.low: "Low - Must have verified email",
        discord.VerificationLevel.medium: "Medium - Must be registered for 5 minutes",
        discord.VerificationLevel.high: "High - Must be in server for 10 minutes",
        discord.VerificationLevel.highest: "Highest - Must have verified phone"
    }
    
    embed.add_field(
        name="🔒 Security",
        value=f"**Verification:** {verification_levels.get(guild.verification_level, 'Unknown')}\n"
              f"**2FA Required:** {'✅' if guild.mfa_level == discord.MFALevel.require_2fa else '❌'}",
        inline=False
    )
    
    # Bot owners (custom bot owners added via !addowner)
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    bot_owners = gdata["settings"].get("owner_ids", [])
    
    if bot_owners:
        owner_mentions = []
        for owner_id in bot_owners:
            owner = guild.get_member(owner_id)
            if owner:
                owner_mentions.append(f"{owner.mention} ({owner.name})")
            else:
                owner_mentions.append(f"User ID: {owner_id} (Left server)")
        
        embed.add_field(
            name="🤖 Extra Owners",
            value="\n".join(owner_mentions[:15]) + (f"\n*and {len(owner_mentions) - 15} more...*" if len(owner_mentions) > 15 else ""),
            inline=False
        )
    
    embed.set_footer(text=f"Requested by {ctx.author.display_name} | Server created")
    await ctx.send(embed=embed)

@bot.command(name="serverfeatures")
async def prefix_serverfeatures(ctx):
    """List all features enabled on the server"""
    guild = ctx.guild
    
    if not guild.features:
        return await ctx.send("ℹ️ This server has no special features enabled.")
    
    embed = discord.Embed(
        title=f"✨ {guild.name} - Server Features",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    
    feature_names = {
        "ANIMATED_ICON": "🎨 Animated Server Icon",
        "BANNER": "🖼️ Server Banner",
        "COMMUNITY": "🏘️ Community Server",
        "DISCOVERABLE": "🔍 Discoverable",
        "FEATURABLE": "⭐ Featurable",
        "INVITE_SPLASH": "💧 Invite Splash Background",
        "MEMBER_VERIFICATION_GATE_ENABLED": "✅ Member Verification Gate",
        "MORE_EMOJI": "😀 More Emoji Slots",
        "MORE_STICKERS": "📸 More Sticker Slots",
        "NEWS": "📰 News Channels",
        "PARTNERED": "🤝 Discord Partner",
        "PREVIEW_ENABLED": "👀 Preview Enabled",
        "ROLE_ICONS": "🎭 Role Icons",
        "SEVEN_DAY_THREAD_ARCHIVE": "📌 7-Day Thread Archive",
        "THREE_DAY_THREAD_ARCHIVE": "📌 3-Day Thread Archive",
        "THREADS_ENABLED": "🧵 Threads Enabled",
        "TICKETED_EVENTS_ENABLED": "🎟️ Ticketed Events",
        "VANITY_URL": "✨ Custom Vanity URL",
        "VERIFIED": "✅ Verified Server",
        "VIP_REGIONS": "🌟 VIP Voice Regions",
        "WELCOME_SCREEN_ENABLED": "👋 Welcome Screen"
    }
    
    features_list = []
    for feature in sorted(guild.features):
        name = feature_names.get(feature, feature.replace("_", " ").title())
        features_list.append(f"✅ {name}")
    
    # Split features into chunks of 25
    for i in range(0, len(features_list), 25):
        embed.add_field(
            name="Features" if i == 0 else "Continued",
            value="\n".join(features_list[i:i+25]),
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="serverroles")
async def prefix_serverroles(ctx):
    """List all roles in the server with counts"""
    guild = ctx.guild
    
    # Get roles sorted by position (highest first)
    roles = sorted(guild.roles, key=lambda r: r.position, reverse=True)
    
    embed = discord.Embed(
        title=f"🎭 {guild.name} - Roles ({len(roles)})",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    
    role_descriptions = []
    for role in roles:
        if role.name != "@everyone":
            member_count = len(role.members)
            role_descriptions.append(f"**{role.mention}** - `{member_count}` member{'s' if member_count != 1 else ''}")
    
    # Split into multiple fields if needed (max 1024 chars per field)
    current_field = []
    current_length = 0
    
    for desc in role_descriptions:
        if current_length + len(desc) + 1 > 1000:
            embed.add_field(name="Roles", value="\n".join(current_field), inline=False)
            current_field = [desc]
            current_length = len(desc)
        else:
            current_field.append(desc)
            current_length += len(desc) + 1
    
    if current_field:
        embed.add_field(name="Roles", value="\n".join(current_field), inline=False)
    
    if not role_descriptions:
        embed.description = "No custom roles found (only @everyone)"
    
    await ctx.send(embed=embed)

@bot.command(name="addserverowner")
async def prefix_addserverowner(ctx, member: discord.Member):
    """Add a owner for this server (Server Owner only)
    Bot owners can use restricted commands like !resetallstats, !resetsettings etc.
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    # Only server owner can add bot owners
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can add bot owners.")
    
    owners = gdata["settings"].setdefault("owner_ids", [])
    
    if member.id in owners:
        return await ctx.send(f"ℹ️ {member.mention} is already a owner for this server.")
    
    owners.append(member.id)
    save_data(data)
    
    embed = discord.Embed(
        title="✅ Owner Added",
        description=f"{member.mention} has been added as a server owner for this server.\n\nThey can now use:\n• `!resetsettings`\n• `!resetallsettings`\n• `!addowner` / `!removeowner`\n• And other admin commands",
        color=0x57F287,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Added by {ctx.author.display_name}")
    await ctx.send(embed=embed)
    log.info(f"Bot owner {member} added in guild {ctx.guild.id} by {ctx.author}")

@bot.command(name="removeserverowner")
async def prefix_removeserverowner(ctx, member: discord.Member):
    """Remove a owner for this server (Server Owner only)"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    # Only server owner can remove bot owners
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("❌ Permission Denied — Only the server owner can remove owners.")
    
    owners = gdata["settings"].get("owner_ids", [])
    
    if member.id not in owners:
        return await ctx.send(f"ℹ️ {member.mention} is not a owner for this server.")
    
    owners.remove(member.id)
    save_data(data)
    
    embed = discord.Embed(
        title="✅ Owner Removed",
        description=f"{member.mention} is no longer a owner for this server.",
        color=0xFFA500,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Removed by {ctx.author.display_name}")
    await ctx.send(embed=embed)
    log.info(f"Server owner {member} removed in guild {ctx.guild.id} by {ctx.author}")

@bot.command(name="listserverowners")
async def prefix_listserverowners(ctx):
    """List all owners for this server"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    owners = gdata["settings"].get("owner_ids", [])
    
    if not owners:
        return await ctx.send("ℹ️ No owners added for this server. Only the server owner can use restricted commands.")
    
    embed = discord.Embed(
        title="👑 Extra Owners",
        description=f"Total: **{len(owners)}** owner(s) for this server",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    owner_list = []
    for owner_id in owners:
        member = ctx.guild.get_member(owner_id)
        if member:
            owner_list.append(f"• {member.mention} - **{member.display_name}**")
        else:
            owner_list.append(f"• User ID: `{owner_id}` (Left server)")
    
    embed.add_field(name="Owners", value="\n".join(owner_list), inline=False)
    embed.set_footer(text="Server owner always has all permissions by default")
    
    await ctx.send(embed=embed)

@bot.command(name="setprogresschannel")
async def prefix_setprogresschannel(ctx, channel: discord.TextChannel = None):
    """Set the channel for role progression announcements
    Usage: !setprogresschannel #channel (or !setprogresschannel to disable)
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    # Check permission
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ Permission Denied — Admin or bot owner only.")
    
    if channel is None:
        gdata["settings"]["role_progress_channel"] = None
        save_data(data)
        return await ctx.send("✅ Role progression announcements disabled.")
    
    gdata["settings"]["role_progress_channel"] = str(channel.id)
    save_data(data)
    
    embed = discord.Embed(
        title="✅ Role Progression Channel Set",
        description=f"Role achievement announcements will be sent to {channel.mention}\n\n"
                    f"Use `!testprogress` to test the announcement system.",
        color=0x57F287,
        timestamp=datetime.utcnow()
    )
    await ctx.send(embed=embed)

@bot.command(name="addprogressrole")
async def prefix_addprogressrole(ctx, role: discord.Role):
    """Add a role to ping when someone achieves a new rank
    Usage: !addprogressrole @RoleName
    """
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ Permission Denied — Admin or bot owner only.")
    
    announce_roles = gdata["settings"].setdefault("role_announce_roles", [])
    
    if role.id in announce_roles:
        return await ctx.send(f"ℹ️ {role.mention} is already being pinged.")
    
    announce_roles.append(role.id)
    save_data(data)
    await ctx.send(f"✅ {role.mention} will now be pinged when someone achieves a new rank.")

@bot.command(name="removeprogressrole")
async def prefix_removeprogressrole(ctx, role: discord.Role):
    """Remove a role from being pinged for rank achievements"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ Permission Denied — Admin or bot owner only.")
    
    announce_roles = gdata["settings"].get("role_announce_roles", [])
    
    if role.id not in announce_roles:
        return await ctx.send(f"ℹ️ {role.mention} is not being pinged.")
    
    announce_roles.remove(role.id)
    save_data(data)
    await ctx.send(f"✅ {role.mention} will no longer be pinged for rank achievements.")

@bot.command(name="listprogressroles")
async def prefix_listprogressroles(ctx):
    """List all roles that get pinged for rank achievements"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    announce_roles = gdata["settings"].get("role_announce_roles", [])
    
    if not announce_roles:
        return await ctx.send("ℹ️ No roles are set to be pinged. Use `!addprogressrole @Role` to add one.")
    
    embed = discord.Embed(
        title="📢 Role Progression Ping Roles",
        description="These roles will be pinged when someone achieves a new rank:",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    role_list = []
    for role_id in announce_roles:
        role = ctx.guild.get_role(role_id)
        if role:
            role_list.append(f"• {role.mention}")
        else:
            role_list.append(f"• Deleted Role ({role_id})")
    
    embed.add_field(name="Roles", value="\n".join(role_list), inline=False)
    await ctx.send(embed=embed)

@bot.command(name="testprogress")
async def prefix_testprogress(ctx, member: discord.Member = None):
    """Test the role progression announcement system"""
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ Permission Denied — Admin or bot owner only.")
    
    member = member or ctx.author
    progress_channel_id = gdata["settings"].get("role_progress_channel")
    
    if not progress_channel_id:
        return await ctx.send("❌ No role progression channel set. Use `!setprogresschannel #channel` first.")
    
    channel = ctx.guild.get_channel(int(progress_channel_id))
    if not channel:
        return await ctx.send("❌ Role progression channel not found! Please reset it.")
    
    # Get thresholds to find a test role
    thresholds = get_applicable_thresholds(member, gdata)
    test_role = None
    for rid_str, req in thresholds.items():
        test_role = ctx.guild.get_role(int(rid_str))
        if test_role:
            break
    
    if not test_role:
        test_role = ctx.guild.get_role(ctx.guild.roles[-1].id) if ctx.guild.roles else None
    
    # Create a test announcement card
    card = await generate_role_achievement_card(member, test_role, udata=None, gdata=gdata)
    
    # Get roles to ping
    announce_roles = gdata["settings"].get("role_announce_roles", [])
    ping_text = " ".join([f"<@&{rid}>" for rid in announce_roles]) if announce_roles else ""
    
    await channel.send(content=ping_text, file=card)
    await ctx.send(f"✅ Test announcement sent to {channel.mention}!")

# ══════════════════════════════════════════════════════════════════════════════
#  WELCOME SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
@bot.group(name="welcome", invoke_without_command=True)
async def welcome_group(ctx):
    data     = get_data()
    gdata    = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")
    s = gdata["settings"]
    welcome_chs = s.get("welcome_channels", [])
    goodbye_chs = s.get("goodbye_channels", [])
    embed = discord.Embed(title="👋 Welcome System Management", description="Configure welcome and goodbye messages",
                          color=BOT_COLOR, timestamp=datetime.utcnow())
    embed.add_field(name="📢 Welcome Channels",
        value="\n".join(f"<#{c}>" for c in welcome_chs) or "None", inline=False)
    embed.add_field(name="👋 Goodbye Channels",
        value="\n".join(f"<#{c}>" for c in goodbye_chs) or "None", inline=False)
    embed.add_field(name="⚙️ Settings", value=(
        f"**Welcome Embed:** {'✅' if s.get('welcome_use_embed',True) else '❌'}\n"
        f"**Goodbye Embed:** {'✅' if s.get('goodbye_use_embed',True) else '❌'}\n"
        f"**Welcome Auto-Delete:** {s.get('welcome_delete_seconds',0)}s\n"
        f"**Goodbye Auto-Delete:** {s.get('goodbye_delete_seconds',0)}s"
    ), inline=False)
    embed.add_field(name="📝 Welcome Message", value=s.get("welcome_message","")[:100], inline=False)
    embed.add_field(name="📝 Goodbye Message", value=s.get("goodbye_message","")[:100], inline=False)
    embed.add_field(name="🔧 Commands", value=(
        "`!welcome add #channel` — Add welcome channel\n"
        "`!welcome remove #channel` — Remove welcome channel\n"
        "`!welcome addgoodbye #channel` — Add goodbye channel\n"
        "`!welcome removegoodbye #channel` — Remove goodbye channel\n"
        "`!welcome embed` — Toggle embed mode\n"
        "`!welcome setdelete <sec>` — Set auto-delete (0=off)\n"
        "`!welcome setmessage <msg>` — Set welcome message\n"
        "`!welcome setgoodbye <msg>` — Set goodbye message\n"
        "`!welcome test` — Test welcome\n"
        "`!welcome testgoodbye` — Test goodbye"
    ), inline=False)
    await ctx.send(embed=embed)

@welcome_group.command(name="add")
async def welcome_add(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    chs = gdata["settings"].setdefault("welcome_channels", [])
    if channel.id in chs: return await ctx.send(f"ℹ️ {channel.mention} is already a welcome channel.")
    chs.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ Added {channel.mention} as a welcome channel.")

@welcome_group.command(name="remove")
async def welcome_remove(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    chs = gdata["settings"].get("welcome_channels", [])
    if channel.id not in chs: return await ctx.send(f"❌ {channel.mention} is not a welcome channel.")
    chs.remove(channel.id)
    save_data(data)
    await ctx.send(f"✅ Removed {channel.mention} from welcome channels.")

@welcome_group.command(name="addgoodbye")
async def welcome_addgoodbye(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    chs = gdata["settings"].setdefault("goodbye_channels", [])
    if channel.id in chs: return await ctx.send(f"ℹ️ {channel.mention} is already a goodbye channel.")
    chs.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ Added {channel.mention} as a goodbye channel.")

@welcome_group.command(name="removegoodbye")
async def welcome_removegoodbye(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    chs = gdata["settings"].get("goodbye_channels", [])
    if channel.id not in chs: return await ctx.send(f"❌ {channel.mention} is not a goodbye channel.")
    chs.remove(channel.id)
    save_data(data)
    await ctx.send(f"✅ Removed {channel.mention} from goodbye channels.")

@welcome_group.command(name="embed")
async def welcome_embed_toggle(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    cur = gdata["settings"].get("welcome_use_embed", True)
    gdata["settings"]["welcome_use_embed"] = not cur
    gdata["settings"]["goodbye_use_embed"] = not cur
    save_data(data)
    await ctx.send(f"✅ Embed mode **{'enabled' if not cur else 'disabled'}** for welcome & goodbye messages.")

@welcome_group.command(name="setdelete")
async def welcome_setdelete(ctx, seconds: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if seconds < 0: return await ctx.send("❌ Seconds cannot be negative.")
    gdata["settings"]["welcome_delete_seconds"] = seconds
    gdata["settings"]["goodbye_delete_seconds"] = seconds
    save_data(data)
    if seconds == 0: await ctx.send("✅ Auto-delete **disabled** for welcome & goodbye messages.")
    else:            await ctx.send(f"✅ Messages will auto-delete after **{seconds} seconds**.")

@welcome_group.command(name="setmessage")
async def welcome_setmessage(ctx, *, message: str):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["welcome_message"] = message
    save_data(data)
    await ctx.send(f"✅ Welcome message updated! Placeholders: `{{mention}}` `{{name}}` `{{server}}` `{{count}}`")

@welcome_group.command(name="setgoodbye")
async def welcome_setgoodbye_msg(ctx, *, message: str):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["goodbye_message"] = message
    save_data(data)
    await ctx.send(f"✅ Goodbye message updated! Placeholders: `{{mention}}` `{{name}}` `{{server}}` `{{count}}`")

@welcome_group.command(name="test")
async def welcome_test(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    s     = gdata["settings"]
    embed_mode = s.get("welcome_use_embed", True)
    delete_sec = s.get("welcome_delete_seconds", 0)
    msg = (s.get("welcome_message", "Welcome {mention}!")
           .replace("{mention}", ctx.author.mention)
           .replace("{name}", ctx.author.display_name)
           .replace("{server}", ctx.guild.name)
           .replace("{count}", str(ctx.guild.member_count)))
    if embed_mode:
        embed = discord.Embed(description=msg, color=0x57F287)
        embed.set_thumbnail(url=ctx.author.display_avatar.url)
        if ctx.guild.icon: embed.set_author(name=ctx.guild.name, icon_url=ctx.guild.icon.url)
        sent = await ctx.send(embed=embed)
    else:
        sent = await ctx.send(msg)
    if delete_sec > 0:
        await ctx.send(f"⏱️ This message will delete in **{delete_sec} seconds**")
        await asyncio.sleep(delete_sec)
        try: await sent.delete()
        except: pass

@welcome_group.command(name="testgoodbye")
async def welcome_testgoodbye(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    s     = gdata["settings"]
    embed_mode = s.get("goodbye_use_embed", True)
    delete_sec = s.get("goodbye_delete_seconds", 0)
    msg = (s.get("goodbye_message", "**{name}** has left the server.")
           .replace("{mention}", ctx.author.mention)
           .replace("{name}", ctx.author.display_name)
           .replace("{server}", ctx.guild.name)
           .replace("{count}", str(ctx.guild.member_count)))
    if embed_mode:
        embed = discord.Embed(description=msg, color=0xED4245)
        sent  = await ctx.send(embed=embed)
    else:
        sent  = await ctx.send(msg)
    if delete_sec > 0:
        await ctx.send(f"⏱️ This message will delete in **{delete_sec} seconds**")
        await asyncio.sleep(delete_sec)
        try: await sent.delete()
        except: pass

# ── Giveaway ───────────────────────────────────────────────────────────────────
active_giveaways = {}

@bot.command(name="gcreate")
@commands.has_permissions(manage_guild=True)
async def prefix_gcreate(ctx):
    def check(m): return m.author == ctx.author and m.channel == ctx.channel
    await ctx.send("🎁 **Giveaway Setup**\nHow long should it last? (e.g. `10m`, `1h`, `2d`)")
    try: dur_msg = await bot.wait_for("message", check=check, timeout=60)
    except asyncio.TimeoutError: return await ctx.send("❌ Timed out.")
    dur_raw = dur_msg.content.strip().lower()
    seconds = 0
    if dur_raw.endswith("d"):   seconds = int(dur_raw[:-1]) * 86400
    elif dur_raw.endswith("h"): seconds = int(dur_raw[:-1]) * 3600
    elif dur_raw.endswith("m"): seconds = int(dur_raw[:-1]) * 60
    else:
        try: seconds = int(dur_raw)
        except: return await ctx.send("❌ Invalid duration.")
    await ctx.send("How many winners?")
    try: w_msg = await bot.wait_for("message", check=check, timeout=60)
    except asyncio.TimeoutError: return await ctx.send("❌ Timed out.")
    try: winners = max(1, int(w_msg.content.strip()))
    except: return await ctx.send("❌ Invalid number.")
    await ctx.send("What's the prize?")
    try: p_msg = await bot.wait_for("message", check=check, timeout=60)
    except asyncio.TimeoutError: return await ctx.send("❌ Timed out.")
    prize    = p_msg.content.strip()
    end_time = datetime.utcnow() + timedelta(seconds=seconds)
    embed = discord.Embed(
        title=f"🎁 GIVEAWAY — {prize}",
        description=f"React with 🎉 to enter!\n\nWinners: **{winners}**\nEnds: <t:{int(end_time.timestamp())}:R>",
        color=0xF1C40F,
    )
    embed.set_footer(text=f"Hosted by {ctx.author.display_name}")
    ga_msg = await ctx.send(embed=embed)
    await ga_msg.add_reaction("🎉")
    active_giveaways[ga_msg.id] = {
        "channel_id": ctx.channel.id, "end_time": end_time.isoformat(),
        "prize": prize, "winners_count": winners, "host_id": ctx.author.id,
    }
    await asyncio.sleep(seconds)
    await _end_giveaway(ga_msg.id, ctx.guild)

async def _end_giveaway(msg_id: int, guild: discord.Guild):
    ga = active_giveaways.pop(msg_id, None)
    if not ga: return
    ch = guild.get_channel(ga["channel_id"])
    if not ch: return
    try: msg = await ch.fetch_message(msg_id)
    except: return
    react = discord.utils.get(msg.reactions, emoji="🎉")
    users = [u async for u in react.users() if not u.bot] if react else []
    if not users:
        await ch.send("🎁 Giveaway ended — no valid entries!")
        return
    count   = min(ga["winners_count"], len(users))
    winners = random.sample(users, count)
    w_str   = ", ".join(w.mention for w in winners)
    await ch.send(f"🎉 Congratulations {w_str}! You won **{ga['prize']}**!")

@bot.command(name="gend")
@commands.has_permissions(manage_guild=True)
async def prefix_gend(ctx, msg_id: int):
    if msg_id not in active_giveaways: return await ctx.send("❌ No active giveaway with that ID.")
    await _end_giveaway(msg_id, ctx.guild)

@bot.command(name="greroll")
@commands.has_permissions(manage_guild=True)
async def prefix_greroll(ctx, msg_id: int):
    try:
        msg   = await ctx.channel.fetch_message(msg_id)
        react = discord.utils.get(msg.reactions, emoji="🎉")
        users = [u async for u in react.users() if not u.bot] if react else []
        if not users: return await ctx.send("❌ No entries found.")
        winner = random.choice(users)
        await ctx.send(f"🎉 Rerolled! New winner: **{winner.mention}**! Congratulations!")
    except Exception:
        await ctx.send("❌ Could not fetch that message.")

@bot.command(name="glist")
async def prefix_glist(ctx):
    if not active_giveaways: return await ctx.send("ℹ️ No active giveaways.")
    lines = [f"• **{ga['prize']}** — ends <t:{int(datetime.fromisoformat(ga['end_time']).timestamp())}:R>"
             for _, ga in active_giveaways.items()]
    await ctx.send(embed=discord.Embed(title="🎁 Active Giveaways", description="\n".join(lines), color=0xF1C40F))

# ── Ticket system (legacy simple commands) ─────────────────────────────────────
@bot.command(name="setticketcategory")
async def prefix_setticketcat(ctx, category: discord.CategoryChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["ticket_category"] = str(category.id)
    save_data(data)
    await ctx.send(f"✅ Ticket category: **{category.name}**")

@bot.command(name="setticketsupport")
async def prefix_setticketsupport(ctx, role: discord.Role):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["ticket_support_role"] = str(role.id)
    save_data(data)
    await ctx.send(f"✅ Support role: **{role.name}**")

@bot.command(name="newticket")
async def prefix_newticket(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    cat_id = s.get("ticket_category")
    if not cat_id: return await ctx.send("❌ No ticket category set. Ask an admin to use `!setticketcategory`.")
    cat = ctx.guild.get_channel(int(cat_id))
    if not cat: return await ctx.send("❌ Ticket category not found.")
    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        ctx.author:             discord.PermissionOverwrite(view_channel=True, send_messages=True),
    }
    sup_rid = s.get("ticket_support_role")
    if sup_rid:
        sup_role = ctx.guild.get_role(int(sup_rid))
        if sup_role: overwrites[sup_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
    ch = await ctx.guild.create_text_channel(f"ticket-{ctx.author.name}", category=cat, overwrites=overwrites)
    embed = discord.Embed(title="🎫 Support Ticket",
        description=f"Hello {ctx.author.mention}! Support will be with you shortly.\nUse `!closeticket` to close.",
        color=BOT_COLOR)
    await ch.send(embed=embed)
    await ctx.send(f"✅ Ticket created: {ch.mention}", delete_after=10)

@bot.command(name="closeticket")
async def prefix_closeticket(ctx):
    if not ctx.channel.name.startswith("ticket-"):
        return await ctx.send("❌ This is not a ticket channel.")
    await ctx.send("🔒 Closing ticket in 5 seconds...")
    await asyncio.sleep(5)
    await ctx.channel.delete(reason="Ticket closed")

# ── Fun commands ───────────────────────────────────────────────────────────────
@bot.command(name="coinflip")
async def prefix_coinflip(ctx):
    await ctx.send(f"🪙 **{random.choice(['Heads','Tails'])}!**")

@bot.command(name="roll")
async def prefix_roll(ctx, sides: int = 6):
    if sides < 2: return await ctx.send("❌ Need at least 2 sides.")
    await ctx.send(f"🎲 You rolled a **{random.randint(1,sides)}** (d{sides})")

@bot.command(name="8ball")
async def prefix_8ball(ctx, *, question: str):
    answers = [
        "It is certain.", "Without a doubt.", "Yes, definitely.",
        "Most likely.", "Outlook good.", "Yes.",
        "Reply hazy, try again.", "Ask again later.", "Cannot predict now.",
        "Don't count on it.", "My reply is no.", "Very doubtful.",
    ]
    embed = discord.Embed(title="🎱 Magic 8-Ball", color=BOT_COLOR)
    embed.add_field(name="Question", value=question)
    embed.add_field(name="Answer",   value=random.choice(answers))
    await ctx.send(embed=embed)

@bot.command(name="choose")
async def prefix_choose(ctx, *, choices: str):
    opts = [c.strip() for c in choices.split("|") if c.strip()]
    if len(opts) < 2: return await ctx.send("❌ Provide at least 2 options separated by `|`.")
    await ctx.send(f"🎯 I choose: **{random.choice(opts)}**")

@bot.command(name="reverse")
async def prefix_reverse(ctx, *, text: str):
    await ctx.send(text[::-1])

# ── GAMES SECTION ──────────────────────────────────────────────────────────────
# Store active games
active_games = {}  # {channel_id: {"type": "number", "number": 50, "answer": "eiffel tower", "start_time": timestamp, "owner_id": user_id, "game_data": {}}}

# Guess the Number Game with Custom Range
@bot.command(name="guessnumber", aliases=["gn"])
async def prefix_guessnumber(ctx, min_num: int = None, max_num: int = None):
    """Start a 'Guess the Number' game with custom range
    Usage: !guessnumber [min] [max]
    Examples:
      !guessnumber        - Default 1-100
      !guessnumber 1 50   - Range 1-50
      !guessnumber 1 500  - Range 1-500
      !guessnumber 10 20  - Range 10-20
    """
    
    # Check if there's already an active game in this channel
    if ctx.channel.id in active_games:
        return await ctx.send("❌ A game is already active in this channel! Wait for it to finish or ask the owner to end it with `!endgame`.")
    
    # Set range based on parameters
    if min_num is None and max_num is None:
        min_num, max_num = 1, 100
    elif min_num is not None and max_num is None:
        # If only one number provided, treat as max with min=1
        max_num = min_num
        min_num = 1
    elif min_num is not None and max_num is not None:
        # Both provided, validate order
        if min_num > max_num:
            min_num, max_num = max_num, min_num
    
    # Validate range
    if min_num < 1:
        return await ctx.send("❌ Minimum number must be at least 1!")
    if max_num - min_num > 10000:
        return await ctx.send("❌ Range too large! Maximum difference is 10,000 numbers.")
    if max_num < min_num:
        return await ctx.send("❌ Max number must be greater than min number!")
    
    # Generate number within range
    number = random.randint(min_num, max_num)
    owner_id = ctx.author.id
    
    # Store game info
    active_games[ctx.channel.id] = {
        "type": "number",
        "number": number,
        "answer": str(number),
        "min": min_num,
        "max": max_num,
        "owner_id": owner_id,
        "start_time": datetime.now().timestamp(),
        "channel_id": ctx.channel.id,
        "guessed": False
    }
    
    # Create range display text
    if min_num == 1 and max_num == 100:
        range_text = "1 to 100 (Default)"
    else:
        range_text = f"{min_num} to {max_num}"
    
    embed = discord.Embed(
        title="🎲 Guess the Number Game Started!",
        description=f"A new number guessing game has started!\n\n"
                   f"**Range:** {range_text}\n"
                   f"**How to play:** Type your guess in the chat\n"
                   f"**Game ends:** When someone guesses correctly OR after 3 minutes\n\n"
                   f"*The number will be revealed when someone wins!*\n"
                   f"*Server Owner/Game Owner:* Use `!endgame` to end the game early.",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    # Send private message to game owner with the number
    try:
        owner_dm = await ctx.author.create_dm()
        await owner_dm.send(f"🎲 **Game Master Info**\n"
                           f"The number for this game is: **{number}**\n"
                           f"Range: {range_text}\n"
                           f"Channel: {ctx.channel.mention}\n"
                           f"Use `!endgame` in the channel to end the game early.")
    except:
        pass
    
    embed.set_footer(text=f"Game started by {ctx.author.display_name} | Auto-ends in 3 minutes")
    await ctx.send(embed=embed)
    
    # Auto-end after 3 minutes
    await asyncio.sleep(180)  # 3 minutes
    
    # Check if game is still active and not guessed
    if ctx.channel.id in active_games and not active_games[ctx.channel.id].get("guessed", False):
        game_data = active_games.pop(ctx.channel.id)
        end_embed = discord.Embed(
            title="⏰ Game Ended - Time's Up!",
            description=f"The number was **{game_data['number']}**!\n"
                       f"Range: {game_data['min']} to {game_data['max']}\n"
                       f"No one guessed it in time.\n"
                       f"Use `!guessnumber` to start a new game.",
            color=0xFFA500,
            timestamp=datetime.utcnow()
        )
        await ctx.send(embed=end_embed)

# Guess the Place Game
@bot.command(name="guessplace", aliases=["gp"])
async def prefix_guessplace(ctx):
    """Start a 'Guess the Place' game - Owner sees the answer"""
    
    if ctx.channel.id in active_games:
        return await ctx.send("❌ A game is already active in this channel! Wait for it to finish or ask the owner to end it with `!endgame`.")
    
    places = {
        "eiffel tower": {
            "description": "This iconic iron lattice tower is located in Paris, France.",
            "hint": "Located in the City of Love 🇫🇷",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/8/85/Eiffel_Tower_from_the_Trocadero%2C_March_2022.jpg"
        },
        "great wall of china": {
            "description": "This ancient fortification stretches over 13,000 miles across northern China.",
            "hint": "Visible from space? Actually a myth! But still massive 🏯",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/2/23/The_Great_Wall_of_China_at_Jinshanling.jpg"
        },
        "taj mahal": {
            "description": "This white marble mausoleum was built in memory of a beloved wife.",
            "hint": "A symbol of eternal love 💕",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/6/67/Taj_Mahal_in_India_-_Kristian_Bertel.jpg"
        },
        "statue of liberty": {
            "description": "This colossal neoclassical sculpture was a gift from France to the US.",
            "hint": "Welcomes visitors to the Big Apple 🗽",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/a/a1/Statue_of_Liberty_7.jpg"
        },
        "colosseum": {
            "description": "This ancient amphitheater could hold up to 80,000 spectators.",
            "hint": "When in Rome! 🏛️",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/d/de/Colosseo_2020.jpg"
        },
        "sydney opera house": {
            "description": "This performing arts centre is known for its distinctive sail-like design.",
            "hint": "Down under! 🇦🇺",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/e/e5/Sydney_Opera_House_-_Dec_2008.jpg"
        },
        "pyramids of giza": {
            "description": "These ancient pyramids are among the Seven Wonders of the Ancient World.",
            "hint": "Ancient wonders of the world 🐪",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/e/e3/Kheops-Pyramid.jpg"
        },
        "machu picchu": {
            "description": "This 15th-century Inca citadel sits high in the Andes Mountains.",
            "hint": "Lost city of the Incas 🦙",
            "image_url": "https://upload.wikimedia.org/wikipedia/commons/e/eb/Machu_Picchu%2C_Peru.jpg"
        }
    }
    
    place_name, place_info = random.choice(list(places.items()))
    owner_id = ctx.author.id
    
    active_games[ctx.channel.id] = {
        "type": "place",
        "answer": place_name,
        "place_info": place_info,
        "owner_id": owner_id,
        "start_time": datetime.now().timestamp(),
        "channel_id": ctx.channel.id,
        "guessed": False
    }
    
    embed = discord.Embed(
        title="🌍 Guess the Famous Place Game Started!",
        description=f"Can you identify this famous landmark?\n\n"
                   f"**Clue:** {place_info['hint']}\n\n"
                   f"**How to play:** Type your answer in the chat\n"
                   f"**Game ends:** When someone guesses correctly OR after 3 minutes\n\n"
                   f"*Server Owner only:* Use `!endgame` to end the game early.",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    embed.set_image(url=place_info["image_url"])
    
    # Send private message to game owner with the answer
    try:
        owner_dm = await ctx.author.create_dm()
        await owner_dm.send(f"🌍 **Game Master Info**\nThe answer for this game is: **{place_name.title()}**\nChannel: {ctx.channel.mention}\nUse `!endgame` in the channel to end the game early.")
    except:
        pass
    
    embed.set_footer(text=f"Game started by {ctx.author.display_name} | Auto-ends in 3 minutes")
    await ctx.send(embed=embed)
    
    # Auto-end after 3 minutes
    await asyncio.sleep(180)
    
    if ctx.channel.id in active_games and not active_games[ctx.channel.id].get("guessed", False):
        game_data = active_games.pop(ctx.channel.id)
        end_embed = discord.Embed(
            title="⏰ Game Ended - Time's Up!",
            description=f"The answer was **{game_data['answer'].title()}**!\n"
                       f"No one guessed it in time.\n"
                       f"Use `!guessplace` to start a new game.",
            color=0xFFA500,
            timestamp=datetime.utcnow()
        )
        await ctx.send(embed=end_embed)

# Guess the Emoji Game
@bot.command(name="guessemoji", aliases=["ge"])
async def prefix_guessemoji(ctx):
    """Start a 'Guess the Emoji' game - Owner sees the answer"""
    
    if ctx.channel.id in active_games:
        return await ctx.send("❌ A game is already active in this channel! Wait for it to finish or ask the owner to end it with `!endgame`.")
    
    emoji_puzzles = {
        "🇺🇸 + 🦅": "usa",
        "🍎 + 📱": "apple",
        "🐱 + 🎩": "cat in the hat",
        "🌞 + 🕶️": "sunglasses",
        "🐝 + 🍯": "honey",
        "🎄 + 🎅": "christmas",
        "🐧 + ❄️": "penguin",
        "🐒 + 🍌": "monkey",
        "🐨 + 🌳": "koala",
        "🐳 + 💦": "whale",
        "🐘 + 🐘": "elephant",
        "🦁 + 👑": "lion king",
        "🐢 + 🐇": "tortoise and hare",
        "🐺 + 🌕": "wolf",
        "🐍 + 🍎": "snake",
        "🕷️ + 🕸️": "spider",
        "🐝 + 🏠": "beehive",
        "🌧️ + ☂️": "rain",
        "⚡ + 🌩️": "thunder",
        "❄️ + ☃️": "snowman",
        "🌋 + 🔥": "volcano",
        "🏠 + 👻": "haunted house",
        "👸 + 🏰": "princess",
        "🧙 + 🔮": "wizard",
        "🧛 + 🧄": "vampire",
        "🧟 + 🧠": "zombie",
        "👽 + 🛸": "alien"
    }
    
    puzzle, answer = random.choice(list(emoji_puzzles.items()))
    owner_id = ctx.author.id
    
    active_games[ctx.channel.id] = {
        "type": "emoji",
        "answer": answer,
        "puzzle": puzzle,
        "owner_id": owner_id,
        "start_time": datetime.now().timestamp(),
        "channel_id": ctx.channel.id,
        "guessed": False
    }
    
    embed = discord.Embed(
        title="❓ Guess the Emoji Game Started!",
        description=f"**Decode this emoji puzzle:**\n\n"
                   f"# {puzzle}\n\n"
                   f"What word/phrase does this represent?\n\n"
                   f"**How to play:** Type your answer in the chat\n"
                   f"**Game ends:** When someone guesses correctly OR after 3 minutes\n\n"
                   f"*Server Owner only:* Use `!endgame` to end the game early.",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    
    # Send private message to game owner with the answer
    try:
        owner_dm = await ctx.author.create_dm()
        await owner_dm.send(f"❓ **Game Master Info**\nThe answer for this game is: **{answer.title()}**\nPuzzle: {puzzle}\nChannel: {ctx.channel.mention}\nUse `!endgame` in the channel to end the game early.")
    except:
        pass
    
    embed.set_footer(text=f"Game started by {ctx.author.display_name} | Auto-ends in 3 minutes")
    await ctx.send(embed=embed)
    
    # Auto-end after 3 minutes
    await asyncio.sleep(180)
    
    if ctx.channel.id in active_games and not active_games[ctx.channel.id].get("guessed", False):
        game_data = active_games.pop(ctx.channel.id)
        end_embed = discord.Embed(
            title="⏰ Game Ended - Time's Up!",
            description=f"The answer was **{game_data['answer'].title()}**!\n"
                       f"No one guessed it in time.\n"
                       f"Use `!guessemoji` to start a new game.",
            color=0xFFA500,
            timestamp=datetime.utcnow()
        )
        await ctx.send(embed=end_embed)

# End Game Command (Owner only)
@bot.command(name="endgame")
async def prefix_endgame(ctx):
    """End the current active game (Server Owner or Game Owner only)"""
    
    if ctx.channel.id not in active_games:
        return await ctx.send("❌ No active game in this channel!")
    
    game_data = active_games[ctx.channel.id]
    
    # Check if user is server owner OR the game owner
    if not (ctx.author.guild_permissions.administrator or ctx.author.id == ctx.guild.owner_id or ctx.author.id == game_data["owner_id"]):
        return await ctx.send("❌ Only the server owner or the game owner can end this game!")
    
    game_type = game_data["type"]
    answer = game_data["answer"]
    
    end_embed = discord.Embed(
        title="🎮 Game Ended by Owner",
        description=f"The game has been ended by {ctx.author.mention}.\n\n"
                   f"**Answer:** {answer.title()}",
        color=0xFF4444,
        timestamp=datetime.utcnow()
    )
    
    # Add range info for number game
    if game_type == "number":
        end_embed.description += f"\n**Range:** {game_data['min']} to {game_data['max']}"
        end_embed.description += f"\n\nUse `!guessnumber` or `!guessnumber 1 500` to start a new game with custom range!"
    elif game_type == "place" and "place_info" in game_data:
        end_embed.set_image(url=game_data["place_info"]["image_url"])
        end_embed.add_field(name="📍 Fun Fact", value=game_data["place_info"]["description"], inline=False)
        end_embed.description += f"\n\nUse `!guessplace` to start a new game!"
    elif game_type == "emoji" and "puzzle" in game_data:
        end_embed.description = f"The game has been ended by {ctx.author.mention}.\n\n**Puzzle:** {game_data['puzzle']}\n**Answer:** {answer.title()}\n\nUse `!guessemoji` to start a new game!"
    
    active_games.pop(ctx.channel.id)
    await ctx.send(embed=end_embed)

# Message listener for guesses
@bot.event
async def on_message(message):
    # Process commands first
    await bot.process_commands(message)
    
    # Check if it's a guess in an active game
    if message.author.bot:
        return
    
    if message.channel.id in active_games:
        game_data = active_games[message.channel.id]
        
        # Don't process if game already ended
        if game_data.get("guessed", False):
            return
        
        guess = message.content.lower().strip()
        answer = game_data["answer"].lower()
        game_type = game_data["type"]
        
        # Check if guess is correct (exact match or contains answer)
        is_correct = False
        
        if game_type == "number":
            try:
                guess_num = int(guess)
                if guess_num == int(answer):
                    is_correct = True
            except:
                pass
        else:
            # For word games, check if guess matches answer
            if guess == answer or guess in answer or answer in guess:
                is_correct = True
        
        if is_correct:
            game_data["guessed"] = True
            
            # Create winner announcement
            win_embed = discord.Embed(
                title="🎉 CORRECT GUESS! 🎉",
                description=f"**{message.author.mention}** guessed the answer correctly!\n\n"
                           f"**Answer:** {answer.title()}\n"
                           f"The game has ended. Use `!guess{game_type}` to start a new game!",
                color=0x00FF00,
                timestamp=datetime.utcnow()
            )
            
            if game_type == "place" and "place_info" in game_data:
                win_embed.set_image(url=game_data["place_info"]["image_url"])
                win_embed.add_field(name="📍 Fun Fact", value=game_data["place_info"]["description"], inline=False)
            elif game_type == "emoji" and "puzzle" in game_data:
                win_embed.description = f"**{message.author.mention}** solved the puzzle!\n\n**Puzzle:** {game_data['puzzle']}\n**Answer:** {answer.title()}\n\nUse `!guessemoji` to start a new game!"
            
            await message.channel.send(embed=win_embed)
            
            # Remove game from active games
            active_games.pop(message.channel.id)

# Game Menu with Dropdown
class GameDropdown(discord.ui.Select):
    def __init__(self, user_id):
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Guess the Number", value="number", description="Guess a number between 1-100", emoji="🎲"),
            discord.SelectOption(label="Guess the Place", value="place", description="Identify famous landmarks from images", emoji="🌍"),
            discord.SelectOption(label="Guess the Emoji", value="emoji", description="Decode emoji puzzles", emoji="❓"),
        ]
        
        super().__init__(placeholder="🎮 Select a game to play...", options=options, min_values=1, max_values=1)
    
    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu is not for you! Use `!games` to open your own.", ephemeral=True)
        
        game = self.values[0]
        
        if game == "number":
            await interaction.response.send_message("🎲 Starting **Guess the Number** game! Check the chat.", ephemeral=True)
            await prefix_guessnumber(await self.get_context(interaction))
        elif game == "place":
            await interaction.response.send_message("🌍 Starting **Guess the Place** game! Check the chat.", ephemeral=True)
            await prefix_guessplace(await self.get_context(interaction))
        elif game == "emoji":
            await interaction.response.send_message("❓ Starting **Guess the Emoji** game! Check the chat.", ephemeral=True)
            await prefix_guessemoji(await self.get_context(interaction))
    
    async def get_context(self, interaction):
        ctx = await bot.get_context(interaction)
        return ctx

class GameView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=120)
        self.add_item(GameDropdown(user_id))
        
        close_btn = discord.ui.Button(label="❌ Close", style=discord.ButtonStyle.danger, row=1)
        async def close_cb(interaction: discord.Interaction):
            if interaction.user.id != user_id:
                return await interaction.response.send_message("❌ Not your menu!", ephemeral=True)
            await interaction.message.delete()
        close_btn.callback = close_cb
        self.add_item(close_btn)

@bot.command(name="games")
async def prefix_games(ctx):
    """Open the games menu to play fun games"""
    embed = discord.Embed(
        title="🎮 Game Center",
        description="Select a game from the dropdown below to start playing!\n\n"
                   "**Available Games:**\n"
                   "🎲 **Guess the Number** - Guess a random number between 1-100\n"
                   "🌍 **Guess the Place** - Identify famous landmarks from images\n"
                   "❓ **Guess the Emoji** - Decode emoji puzzles to find the word\n\n"
                   "*Server owners see the answers privately. Games auto-end after 3 minutes.*",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Requested by {ctx.author.display_name}")
    view = GameView(ctx.author.id)
    await ctx.send(embed=embed, view=view)

@bot.command(name="gamehelp")
async def prefix_gamehelp(ctx):
    """Show help for all games"""
    embed = discord.Embed(
        title="🎮 Game Commands Help",
        description="Here are all the available game commands:",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    embed.add_field(
        name="🎲 Guess the Number",
        value="`!guessnumber` or `!gn` - Start a number guessing game (default 1-100)\n"
              "`!guessnumber 1 50` - Start with range 1-50\n"
              "`!guessnumber 1 500` - Start with range 1-500\n"
              "`!guessnumber 50 100` - Start with range 50-100\n\n"
              "**Owner:** Receives the number via DM\n"
              "**Players:** Type any number to guess\n"
              "**Note:** Maximum range difference is 10,000 numbers\n"
              "**End:** Auto-ends after 3 minutes or when guessed correctly",
        inline=False
    )
    embed.add_field(
        name="🌍 Guess the Place",
        value="`!guessplace` or `!gp` - Start a landmark identification game\n"
              "**Owner:** Receives the answer via DM\n"
              "**Players:** Type the name of the landmark\n"
              "**End:** Auto-ends after 3 minutes or when guessed correctly",
        inline=False
    )
    embed.add_field(
        name="❓ Guess the Emoji",
        value="`!guessemoji` or `!ge` - Start an emoji puzzle game\n"
              "**Owner:** Receives the answer via DM\n"
              "**Players:** Type the word/phrase from emojis\n"
              "**End:** Auto-ends after 3 minutes or when guessed correctly",
        inline=False
    )
    embed.add_field(
        name="🎮 Game Menu",
        value="`!games` - Open the interactive game menu",
        inline=False
    )
    embed.add_field(
        name="🛑 End Game",
        value="`!endgame` - End the current active game (Server Owner or Game Owner only)",
        inline=False
    )
    embed.set_footer(text="Have fun playing! 🎉 | Games auto-end after 3 minutes")
    await ctx.send(embed=embed)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        await bot.process_commands(message)
        return
    
    data = get_data()
    gdata = get_guild_data(data, message.guild.id)
    
    # Check AutoMod
    if await run_automod(message, gdata):
        return
    
    # Check if message should be counted
    if should_count_message(message, gdata):
        interval = gdata["settings"].get("spam_interval", 3)
        if check_spam(message.guild.id, message.author.id, interval):
            udata = get_user_data(gdata, message.author.id)
            increment_counts(udata)
            
            # XP gain
            if gdata["settings"].get("xp_enabled", True):
                now = time.time()
                last_xp = udata.get("last_xp_time", 0.0)
                xp_cd = gdata["settings"].get("xp_cooldown", 10)
                if now - last_xp >= xp_cd:
                    xp_gain = random.randint(
                        gdata["settings"].get("xp_min", 1),
                        gdata["settings"].get("xp_max", 5),
                    )
                    increment_xp(udata, xp_gain)
                    udata["last_xp_time"] = now
            
            # Streak update
            if gdata["settings"].get("streak_enabled", True):
                update_streak(udata)
            
            # Assign roles and check milestones
            await assign_roles(message.guild, message.author, udata, gdata)
            await check_and_announce_milestones(message.guild, message.author, udata, gdata)
    
    # ── Games guess checker ────────────────────────────────────────────
    if message.channel.id in active_games:
        game_data = active_games[message.channel.id]
        if not game_data.get("guessed", False):
            guess = message.content.lower().strip()
            answer = game_data["answer"].lower()
            game_type = game_data["type"]
            is_correct = False
            
            if game_type == "number":
                try:
                    if int(guess) == int(answer):
                        is_correct = True
                except:
                    pass
            else:
                if guess == answer or guess in answer or answer in guess:
                    is_correct = True
            
            if is_correct:
                game_data["guessed"] = True
                win_embed = discord.Embed(
                    title="🎉 CORRECT GUESS! 🎉",
                    description=f"**{message.author.mention}** guessed it!\n\n**Answer:** {answer.title()}",
                    color=0x00FF00,
                    timestamp=datetime.utcnow()
                )
                if game_type == "place" and "place_info" in game_data:
                    win_embed.set_image(url=game_data["place_info"]["image_url"])
                    win_embed.add_field(name="📍 Fun Fact", value=game_data["place_info"]["description"], inline=False)
                elif game_type == "emoji" and "puzzle" in game_data:
                    win_embed.description = f"**{message.author.mention}** solved it!\n\n**Puzzle:** {game_data['puzzle']}\n**Answer:** {answer.title()}"
                await message.channel.send(embed=win_embed)
                active_games.pop(message.channel.id)
    # ── End games ──────────────────────────────────────────────────────
    
    await bot.process_commands(message)

# ── Role thresholds & paths ────────────────────────────────────────────────────
@bot.command(name="setrole")
async def prefix_setrole(ctx, role: discord.Role, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if count < 1: return await ctx.send("❌ Count must be ≥ 1.")
    gdata["settings"]["role_thresholds"][str(role.id)] = count
    save_data(data)
    await ctx.send(f"✅ **{role.name}** assigned at **{count:,}** messages.")

@bot.command(name="removerole")
async def prefix_removerole(ctx, role: discord.Role):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    t = gdata["settings"].get("role_thresholds", {})
    if str(role.id) not in t: return await ctx.send(f"❌ No threshold for **{role.name}**.")
    del t[str(role.id)]
    save_data(data)
    await ctx.send(f"✅ Removed threshold for **{role.name}**.")

@bot.command(name="listroles")
async def prefix_listroles(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    t     = gdata["settings"].get("role_thresholds", {})
    if not t: return await ctx.send("ℹ️ No message role thresholds set.")
    lines = [f"• <@&{rid}> — **{c:,}** msgs" for rid, c in sorted(t.items(), key=lambda x: x[1])]
    await ctx.send(embed=discord.Embed(title="🎭 Message Role Thresholds", description="\n".join(lines), color=BOT_COLOR))

@bot.command(name="setvcrole")
async def prefix_setvcrole(ctx, role: discord.Role, minutes: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["vc_role_thresholds"][str(role.id)] = minutes
    save_data(data)
    await ctx.send(f"✅ **{role.name}** assigned at **{minutes:,}** VC minutes.")

@bot.command(name="removevcrole")
async def prefix_removevcrole(ctx, role: discord.Role):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    t = gdata["settings"].get("vc_role_thresholds", {})
    if str(role.id) not in t: return await ctx.send(f"❌ No VC threshold for **{role.name}**.")
    del t[str(role.id)]
    save_data(data)
    await ctx.send(f"✅ Removed VC threshold for **{role.name}**.")

@bot.command(name="listvcroles")
async def prefix_listvcroles(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    t     = gdata["settings"].get("vc_role_thresholds", {})
    if not t: return await ctx.send("ℹ️ No VC role thresholds set.")
    lines = [f"• <@&{rid}> — **{c:,}** min" for rid, c in sorted(t.items(), key=lambda x: x[1])]
    await ctx.send(embed=discord.Embed(title="🎙 VC Role Thresholds", description="\n".join(lines), color=BOT_COLOR))

@bot.command(name="addpath")
async def prefix_addpath(ctx, trigger_role: discord.Role, promoted_role: discord.Role, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    paths = gdata["settings"]["conditional_paths"]
    path  = next((p for p in paths if p["trigger_role_id"] == str(trigger_role.id)), None)
    if not path:
        path = {"trigger_role_id": str(trigger_role.id), "thresholds": {}}
        paths.append(path)
    path["thresholds"][str(promoted_role.id)] = count
    save_data(data)
    await ctx.send(f"✅ Members with **{trigger_role.name}** → **{promoted_role.name}** at **{count:,}** messages.")

@bot.command(name="removepath")
async def prefix_removepath(ctx, trigger_role: discord.Role):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    paths = gdata["settings"]["conditional_paths"]
    new   = [p for p in paths if p["trigger_role_id"] != str(trigger_role.id)]
    if len(new) == len(paths): return await ctx.send(f"❌ No path for **{trigger_role.name}**.")
    gdata["settings"]["conditional_paths"] = new
    save_data(data)
    await ctx.send(f"✅ Removed path for **{trigger_role.name}**.")

@bot.command(name="listpaths")
async def prefix_listpaths(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    paths = gdata["settings"].get("conditional_paths", [])
    if not paths: return await ctx.send("ℹ️ No conditional paths. Use `!addpath @trigger @role <count>`.")
    embed = discord.Embed(title="🔀 Conditional Role Paths", color=BOT_COLOR)
    for path in paths:
        trigger = ctx.guild.get_role(int(path["trigger_role_id"]))
        lines   = [f"  • <@&{rid}> at {c:,} msgs" for rid, c in sorted(path["thresholds"].items(), key=lambda x: x[1])]
        embed.add_field(name=f"Has role: {trigger.name if trigger else path['trigger_role_id']}",
                        value="\n".join(lines) or "No roles", inline=False)
    await ctx.send(embed=embed)

# ── Settings / channel / member controls ─────────────────────────────────────
@bot.command(name="setspam")
async def prefix_setspam(ctx, seconds: float):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if seconds < 1: return await ctx.send("❌ Minimum 1 second.")
    gdata["settings"]["spam_interval"] = seconds
    save_data(data)
    await ctx.send(f"✅ Anti-spam: **{seconds}s** interval.")

@bot.command(name="toggleemoji")
async def prefix_toggleemoji(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    cur = gdata["settings"].get("count_emojis", True)
    gdata["settings"]["count_emojis"] = not cur
    save_data(data)
    await ctx.send(f"✅ Emoji-only messages: **{'counted' if not cur else 'ignored'}**.")

@bot.command(name="togglevc")
async def prefix_togglevc(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    cur = gdata["settings"].get("include_vc_in_roles", False)
    gdata["settings"]["include_vc_in_roles"] = not cur
    save_data(data)
    await ctx.send(f"✅ VC time in msg role thresholds: **{'yes' if not cur else 'no'}**.")

@bot.command(name="whitelist")
async def prefix_whitelist(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    wl = gdata["settings"]["whitelisted_channels"]
    if channel.id in wl: return await ctx.send(f"ℹ️ {channel.mention} already whitelisted.")
    wl.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ {channel.mention} whitelisted.")

@bot.command(name="blacklist")
async def prefix_blacklist(ctx, channel: discord.TextChannel):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_channels"]
    if channel.id in bl: return await ctx.send(f"ℹ️ {channel.mention} already blacklisted.")
    bl.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ {channel.mention} blacklisted.")

@bot.command(name="clearwhitelist")
async def prefix_clearwhitelist(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["whitelisted_channels"] = []
    save_data(data)
    await ctx.send("✅ Whitelist cleared.")

@bot.command(name="clearblacklist")
async def prefix_clearblacklist(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["blacklisted_channels"] = []
    save_data(data)
    await ctx.send("✅ Blacklist cleared.")

@bot.command(name="listchannels")
async def prefix_listchannels(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    wl    = [f"<#{c}>" for c in s.get("whitelisted_channels", [])]
    bl    = [f"<#{c}>" for c in s.get("blacklisted_channels", [])]
    embed = discord.Embed(title="📋 Channel Settings", color=BOT_COLOR)
    embed.add_field(name="✅ Whitelisted", value="\n".join(wl) or "None (all count)", inline=False)
    embed.add_field(name="❌ Blacklisted", value="\n".join(bl) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="blacklistmember")
async def prefix_blacklistmember(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_members"]
    if member.id in bl: return await ctx.send(f"ℹ️ **{member.display_name}** already blacklisted.")
    bl.append(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** blocked from role upgrades.")

@bot.command(name="unblacklistmember")
async def prefix_unblacklistmember(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_members"]
    if member.id not in bl: return await ctx.send(f"ℹ️ **{member.display_name}** is not blacklisted.")
    bl.remove(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** unblacklisted.")

@bot.command(name="addmsgs")
async def prefix_addmsgs(ctx, member: discord.Member, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    udata = get_user_data(gdata, member.id)
    increment_counts(udata, count)
    save_data(data)
    await assign_roles(ctx.guild, member, udata, gdata)
    await ctx.send(f"✅ Added **{count:,}** messages to **{member.display_name}** (total: {udata['total']:,}).")

@bot.command(name="removemsgs")
async def prefix_removemsgs(ctx, member: discord.Member, count: int):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    udata = get_user_data(gdata, member.id)
    udata["total"] = max(0, udata.get("total", 0) - count)
    save_data(data)
    await ctx.send(f"✅ Removed **{count:,}** from **{member.display_name}** (total: {udata['total']:,}).")

@bot.command(name="resetuser")
async def prefix_resetuser(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["users"].pop(str(member.id), None)
    save_data(data)
    await ctx.send(f"✅ Reset all stats for **{member.display_name}**.")

@bot.command(name="resetallstats")
async def prefix_resetallstats(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")

    confirm_msg = await ctx.send(
        "⚠️ **DANGER ZONE** ⚠️\n"
        "This will **permanently wipe ALL stats** (messages, VC time, XP, streaks, milestones) "
        "for **every member** in this server.\n\n"
        "React with ✅ to confirm or ❌ to cancel."
    )
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id

    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ Reset cancelled.")
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Reset cancelled.")

    member_count = len(gdata.get("users", {}))
    gdata["users"] = {}
    save_data(data)
    log.info(f"[resetallstats] {ctx.author} wiped all stats for {member_count} users in guild {ctx.guild.id}")

    embed = discord.Embed(
        title="🗑️ Server Stats Reset",
        description=(
            f"✅ **All stats have been wiped** for **{member_count}** member(s).\n\n"
            "Messages · VC time · XP · Streaks · Milestones — all cleared."
        ),
        color=0xFF4444,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Reset by {ctx.author.display_name}")
    await ctx.send(embed=embed)

@bot.command(name="resetallmessages")
async def prefix_resetallmessages(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")

    confirm_msg = await ctx.send(
        "⚠️ **WARNING** ⚠️\n"
        "This will **permanently reset all message stats** (total, daily, weekly, monthly) "
        "for **every member** in this server.\n\n"
        "React with ✅ to confirm or ❌ to cancel."
    )
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id

    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ Reset cancelled.")
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Reset cancelled.")

    member_count = 0
    for uid, udata in gdata.get("users", {}).items():
        udata["total"]   = 0
        udata["daily"]   = {}
        udata["weekly"]  = {}
        udata["monthly"] = {}
        udata["xp"]         = 0
        udata["xp_daily"]   = {}
        udata["xp_weekly"]  = {}
        udata["xp_monthly"] = {}
        udata["streak"]          = 0
        udata["longest_streak"]  = 0
        udata["last_msg_date"]   = ""
        udata["first_msg_date"]  = ""
        udata["achieved_milestones"] = []
        member_count += 1

    save_data(data)
    log.info(f"[resetallmessages] {ctx.author} wiped message stats for {member_count} users in guild {ctx.guild.id}")

    embed = discord.Embed(
        title="💬 Server Message Stats Reset",
        description=(
            f"✅ **Message stats have been wiped** for **{member_count}** member(s).\n\n"
            "Total · Daily · Weekly · Monthly messages — all cleared.\n"
            "XP and streaks (which are message-driven) were also reset."
        ),
        color=0xFF8C00,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Reset by {ctx.author.display_name}")
    await ctx.send(embed=embed)

@bot.command(name="resetallvc")
async def prefix_resetallvc(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata):
        return await ctx.send("❌ No permission.")

    confirm_msg = await ctx.send(
        "⚠️ **WARNING** ⚠️\n"
        "This will **permanently reset all VC time stats** (total, daily, weekly, monthly) "
        "for **every member** in this server.\n\n"
        "React with ✅ to confirm or ❌ to cancel."
    )
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id

    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            return await ctx.send("❌ Reset cancelled.")
    except asyncio.TimeoutError:
        return await ctx.send("❌ Confirmation timed out. Reset cancelled.")

    member_count = 0
    for uid, udata in gdata.get("users", {}).items():
        udata["vc_total"]   = 0
        udata["vc_daily"]   = {}
        udata["vc_weekly"]  = {}
        udata["vc_monthly"] = {}
        udata["vc_achieved_milestones"] = []
        member_count += 1

    gid = ctx.guild.id
    if gid in vc_sessions:
        now = time.time()
        for uid in vc_sessions[gid]:
            vc_sessions[gid][uid] = now

    save_data(data)
    log.info(f"[resetallvc] {ctx.author} wiped VC stats for {member_count} users in guild {ctx.guild.id}")

    embed = discord.Embed(
        title="🎙️ Server VC Stats Reset",
        description=(
            f"✅ **VC time stats have been wiped** for **{member_count}** member(s).\n\n"
            "Total · Daily · Weekly · Monthly VC time — all cleared.\n"
            "VC milestones were also reset."
        ),
        color=0x5865F2,
        timestamp=datetime.utcnow()
    )
    embed.set_footer(text=f"Reset by {ctx.author.display_name}")
    await ctx.send(embed=embed)

@bot.command(name="addowner")
async def prefix_addowner(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    owners = gdata["settings"].setdefault("owner_ids", [])
    if member.id in owners: return await ctx.send(f"ℹ️ Already has bot-owner permissions.")
    owners.append(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** granted bot-owner permissions.")

@bot.command(name="removeowner")
async def prefix_removeowner(ctx, member: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    owners = gdata["settings"].get("owner_ids", [])
    if member.id not in owners: return await ctx.send(f"ℹ️ No bot-owner permissions.")
    owners.remove(member.id)
    save_data(data)
    await ctx.send(f"✅ Removed bot-owner from **{member.display_name}**.")

@bot.command(name="settings")
async def prefix_settings(ctx):
    data  = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    embed = discord.Embed(title="⚙️ Server Settings", color=BOT_COLOR)
    if ctx.guild.icon: embed.set_thumbnail(url=ctx.guild.icon.url)
    embed.add_field(name="Spam Interval",    value=f"{s.get('spam_interval',3)}s",                inline=True)
    embed.add_field(name="Emoji Counting",   value=str(s.get("count_emojis",True)),               inline=True)
    embed.add_field(name="VC in Msg Roles",  value=str(s.get("include_vc_in_roles",False)),       inline=True)
    embed.add_field(name="AutoMod",          value=str(s.get("automod_enabled",False)),           inline=True)
    embed.add_field(name="XP Enabled",       value=str(s.get("xp_enabled",True)),                inline=True)
    embed.add_field(name="XP Range",         value=f"{s.get('xp_min',1)}–{s.get('xp_max',5)}",   inline=True)
    embed.add_field(name="XP Cooldown",      value=f"{s.get('xp_cooldown',10)}s",                inline=True)
    embed.add_field(name="Streaks",          value=str(s.get("streak_enabled",True)),             inline=True)
    embed.add_field(name="Msg Thresholds",   value=str(len(s.get("role_thresholds",{}))),         inline=True)
    embed.add_field(name="VC Thresholds",    value=str(len(s.get("vc_role_thresholds",{}))),      inline=True)
    embed.add_field(name="Cond. Paths",      value=str(len(s.get("conditional_paths",[]))),       inline=True)
    embed.add_field(name="Milestones",       value=str(len(s.get("milestone_thresholds",[]))),    inline=True)
    embed.add_field(name="Milestone Ch.",    value=f"<#{s['milestone_announce']}>" if s.get("milestone_announce") else "Not set", inline=True)
    embed.add_field(name="Whitelisted Ch.",  value=str(len(s.get("whitelisted_channels",[]))),    inline=True)
    embed.add_field(name="Blacklisted Ch.",  value=str(len(s.get("blacklisted_channels",[]))),    inline=True)
    embed.add_field(name="Blacklisted Mbrs", value=str(len(s.get("blacklisted_members",[]))),     inline=True)
    embed.add_field(name="Welcome Ch.",      value=str(len(s.get("welcome_channels",[]))) + " channel(s)", inline=True)
    embed.add_field(name="Goodbye Ch.",      value=str(len(s.get("goodbye_channels",[]))) + " channel(s)", inline=True)
    embed.add_field(name="Log Ch.",          value=f"<#{s['log_channel']}>" if s.get("log_channel") else "Not set", inline=True)
    await ctx.send(embed=embed)

# ── DM Permission Management ──────────────────────────────────────────────────
@bot.command(name="dmaddrole")
async def prefix_dmaddrole(ctx, role: discord.Role):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("🚫 Permission Denied — Only the server owner can grant DM role permissions.")
    
    allowed_roles = gdata["settings"].setdefault("dm_allowed_roles", [])
    
    if role.id in allowed_roles:
        return await ctx.send(f"ℹ️ {role.mention} already has DM permissions.")
    
    allowed_roles.append(role.id)
    save_data(data)
    await ctx.send(f"✅ {role.mention} can now use !dm (single-member DM only). !dmall and !dmrole remain server owner only.")

@bot.command(name="dmremoverole")
async def prefix_dmremoverole(ctx, role: discord.Role):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    if not is_actual_server_owner(ctx.author, ctx.guild):
        return await ctx.send("🚫 Permission Denied — Only the server owner can revoke DM role permissions.")
    
    allowed_roles = gdata["settings"].setdefault("dm_allowed_roles", [])
    
    if role.id not in allowed_roles:
        return await ctx.send(f"ℹ️ {role.mention} does not have DM permissions.")
    
    allowed_roles.remove(role.id)
    save_data(data)
    await ctx.send(f"✅ Removed DM permissions from {role.mention}")

@bot.command(name="dmlistroles")
async def prefix_dmlistroles(ctx):
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    
    allowed_roles = gdata["settings"].get("dm_allowed_roles", [])
    
    if not allowed_roles:
        return await ctx.send("ℹ️ No roles have DM permissions. Only the server owner can use DM commands.")
    
    role_mentions = []
    for role_id in allowed_roles:
        role = ctx.guild.get_role(role_id)
        if role:
            role_mentions.append(role.mention)
        else:
            role_mentions.append(f"Deleted Role ({role_id})")
    
    embed = discord.Embed(
        title="👑 DM Command Permissions",
        description="These roles can use `!dm @member` (single-member DM only). `!dmall` and `!dmrole` are server owner only:",
        color=BOT_COLOR,
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="Allowed Roles", value="\n".join(role_mentions) or "None", inline=False)
    embed.set_footer(text="Server owner always has permission by default")
    
    await ctx.send(embed=embed)

# ══════════════════════════════════════════════════════════════════════════════
#  SLASH COMMANDS
# ══════════════════════════════════════════════════════════════════════════════
@bot.tree.command(name="help", description="Show the interactive help menu")
async def slash_help(interaction: discord.Interaction):
    banner = generate_banner()
    view   = HelpView(interaction.guild, interaction.user.id, bot_ref=bot)
    embed  = help_home_embed(interaction.guild)
    await interaction.response.defer()
    await interaction.followup.send(file=banner)
    await interaction.followup.send(embed=embed, view=view)

@bot.tree.command(name="rank", description="View rank card with progress bars, XP and streak")
@app_commands.describe(member="User to check (leave empty for yourself)")
async def slash_rank(interaction: discord.Interaction, member: discord.Member = None):
    member   = member or interaction.user
    data     = get_data()
    gdata    = get_guild_data(data, interaction.guild.id)
    udata    = get_user_data(gdata, member.id)
    rank_pos = get_rank_position(interaction.guild, gdata, member.id)
    await interaction.response.defer()
    try:
        card = await generate_rank_card(member, udata, gdata, rank_pos)
        await interaction.followup.send(file=card)
    except Exception as e:
        log.error(f"Rank card: {e}", exc_info=True)
        await interaction.followup.send("❌ Could not generate rank card.")

@bot.tree.command(name="stats", description="Full stats: messages, voice, XP, streak, ranks")
@app_commands.describe(member="User to check")
async def slash_stats(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    await interaction.response.send_message(embed=stats_embed(member, udata, gdata, interaction.guild))

@bot.tree.command(name="lb", description="Leaderboard — messages, voice, XP, or streaks")
@app_commands.describe(mode="What to rank by", period="Time period")
@app_commands.choices(
    mode=[
        app_commands.Choice(name="Messages",   value="msg"),
        app_commands.Choice(name="Voice Time", value="vc"),
        app_commands.Choice(name="XP",         value="xp"),
        app_commands.Choice(name="Streak",     value="streak"),
    ],
    period=[
        app_commands.Choice(name="All Time",   value="total"),
        app_commands.Choice(name="Today",      value="daily"),
        app_commands.Choice(name="This Week",  value="weekly"),
        app_commands.Choice(name="This Month", value="monthly"),
    ],
)
async def slash_lb(interaction: discord.Interaction, mode: str = "msg", period: str = "total"):
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    scores = build_leaderboard(interaction.guild, gdata, period, mode=mode)
    await interaction.response.send_message(embed=leaderboard_embed(scores, period, interaction.guild, mode=mode))

@bot.tree.command(name="vclb", description="Voice time leaderboard")
@app_commands.choices(period=[
    app_commands.Choice(name="All Time",   value="total"),
    app_commands.Choice(name="Today",      value="daily"),
    app_commands.Choice(name="This Week",  value="weekly"),
    app_commands.Choice(name="This Month", value="monthly"),
])
async def slash_vclb(interaction: discord.Interaction, period: str = "total"):
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    scores = build_leaderboard(interaction.guild, gdata, period, mode="vc")
    await interaction.response.send_message(embed=leaderboard_embed(scores, period, interaction.guild, mode="vc"))

@bot.tree.command(name="xp", description="Check XP for yourself or another user")
@app_commands.describe(member="User to check")
async def slash_xp(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    xp     = udata.get("xp", 0)
    rank   = get_xp_rank_position(interaction.guild, gdata, member.id)
    embed  = discord.Embed(description=f"✨ **{member.display_name}** — **{xp:,} XP** (XP Rank #{rank})", color=BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="streak", description="Check daily message streak")
@app_commands.describe(member="User to check")
async def slash_streak(interaction: discord.Interaction, member: discord.Member = None):
    member  = member or interaction.user
    data    = get_data()
    gdata   = get_guild_data(data, interaction.guild.id)
    udata   = get_user_data(gdata, member.id)
    streak  = udata.get("streak", 0)
    longest = udata.get("longest_streak", 0)
    embed   = discord.Embed(title=f"🔥 Streak — {member.display_name}", color=0xFF8C00 if streak >= 7 else BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="🔥 Current", value=f"**{streak}** days", inline=True)
    embed.add_field(name="🏆 Longest", value=f"**{longest}** days", inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="compare", description="Side-by-side stat comparison of two users")
@app_commands.describe(member1="First user", member2="Second user")
async def slash_compare(interaction: discord.Interaction, member1: discord.Member, member2: discord.Member):
    data  = get_data()
    gdata = get_guild_data(data, interaction.guild.id)
    u1    = get_user_data(gdata, member1.id)
    u2    = get_user_data(gdata, member2.id)
    def w(a, b): return "✅" if a > b else ("❌" if a < b else "🤝")
    t1=u1.get("total",0); t2=u2.get("total",0)
    v1=u1.get("vc_total",0); v2=u2.get("vc_total",0)
    x1=u1.get("xp",0); x2=u2.get("xp",0)
    s1=u1.get("streak",0); s2=u2.get("streak",0)
    embed = discord.Embed(title=f"⚔️ {member1.display_name} vs {member2.display_name}", color=BOT_COLOR)
    embed.add_field(name=f"{w(t1,t2)} {member1.display_name}",
        value=f"💬 **{t1:,}** msgs\n🎙 **{int(v1//60)}h {int(v1%60)}m**\n✨ **{x1:,}** XP\n🔥 **{s1}d**", inline=True)
    embed.add_field(name=f"{w(t2,t1)} {member2.display_name}",
        value=f"💬 **{t2:,}** msgs\n🎙 **{int(v2//60)}h {int(v2%60)}m**\n✨ **{x2:,}** XP\n🔥 **{s2}d**", inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="goal", description="Check message goal progress")
@app_commands.describe(member="User to check")
async def slash_goal(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    goals  = gdata["settings"].get("msg_goal", {})
    goal   = goals.get(str(member.id))
    total  = udata.get("total", 0)
    if not goal:
        return await interaction.response.send_message(
            f"ℹ️ **{member.display_name}** has no goal set. Use `!setgoal <count>`.", ephemeral=True)
    pct   = min(total / goal, 1.0)
    bar   = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
    embed = discord.Embed(title=f"🎯 Goal — {member.display_name}", color=0x57F287 if total >= goal else BOT_COLOR)
    embed.add_field(name="Progress", value=f"`{bar}` {int(pct*100)}%")
    embed.add_field(name="Messages", value=f"**{total:,}** / **{goal:,}**")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="msgcount", description="Quick message count for a user")
@app_commands.describe(member="User to check")
async def slash_msgcount(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    total  = udata.get("total", 0)
    rank   = get_rank_position(interaction.guild, gdata, member.id)
    await interaction.response.send_message(embed=discord.Embed(
        description=f"💬 **{member.display_name}** — **{total:,} messages** (Rank #{rank})", color=BOT_COLOR))

@bot.tree.command(name="vctime", description="Check voice time for a user")
@app_commands.describe(member="User to check")
async def slash_vctime(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = get_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    vc_t   = udata.get("vc_total", 0)
    h      = int(vc_t // 60); m = int(vc_t % 60)
    rank   = get_vc_rank_position(interaction.guild, gdata, member.id)
    await interaction.response.send_message(embed=discord.Embed(
        description=f"🎙 **{member.display_name}** — **{h}h {m}m** in voice (VC Rank #{rank})", color=BOT_COLOR))

@bot.tree.command(name="ping", description="Check bot latency")
async def slash_ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"🏓 Pong! **{round(bot.latency*1000)}ms**")

@bot.tree.command(name="serverinfo", description="Server information")
async def slash_serverinfo(interaction: discord.Interaction):
    g = interaction.guild
    embed = discord.Embed(title=g.name, color=BOT_COLOR)
    if g.icon: embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name="Owner",   value=str(g.owner))
    embed.add_field(name="Members", value=str(g.member_count))
    embed.add_field(name="Created", value=g.created_at.strftime("%Y-%m-%d"))
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="userinfo", description="User information")
@app_commands.describe(member="User to check")
async def slash_userinfo(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    embed  = discord.Embed(title=str(member), color=member.color if member.color.value else BOT_COLOR)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="ID",      value=str(member.id))
    embed.add_field(name="Joined",  value=member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "?")
    embed.add_field(name="Created", value=member.created_at.strftime("%Y-%m-%d"))
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="avatar", description="View a user's avatar")
@app_commands.describe(member="User to check")
async def slash_avatar(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    embed  = discord.Embed(title=f"{member.display_name}'s Avatar", color=BOT_COLOR)
    embed.set_image(url=member.display_avatar.url)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="warn", description="Warn a member")
@app_commands.describe(member="Member to warn", reason="Reason for the warning")
async def slash_warn(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided"):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ No permission.", ephemeral=True)
    data  = get_data()
    gdata = get_guild_data(data, interaction.guild.id)
    udata = get_user_data(gdata, member.id)
    udata["warnings"] = udata.get("warnings", 0) + 1
    save_data(data)
    await interaction.response.send_message(
        f"⚠️ **{member.display_name}** warned. Reason: {reason} (Total: {udata['warnings']})")
    try: await member.send(f"You were warned in **{interaction.guild.name}**: {reason}")
    except: pass

@bot.tree.command(name="purge", description="Delete messages from this channel")
@app_commands.describe(count="Number of messages to delete (default 10)", mode="all / bot")
async def slash_purge(interaction: discord.Interaction, count: int = 10, mode: str = "all"):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ No permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    count = min(max(count, 1), 100)
    if mode.lower() == "bot":
        deleted = await interaction.channel.purge(limit=200, check=lambda m: m.author.bot)
    else:
        deleted = await interaction.channel.purge(limit=count + 1)
    await interaction.followup.send(f"🗑️ Deleted **{len(deleted)}** messages.", ephemeral=True)

@bot.tree.command(name="settings", description="View server settings overview")
async def slash_settings(interaction: discord.Interaction):
    data  = get_data()
    gdata = get_guild_data(data, interaction.guild.id)
    s     = gdata["settings"]
    embed = discord.Embed(title="⚙️ Server Settings", color=BOT_COLOR)
    if interaction.guild.icon: embed.set_thumbnail(url=interaction.guild.icon.url)
    embed.add_field(name="Spam Interval",  value=f"{s.get('spam_interval',3)}s",    inline=True)
    embed.add_field(name="XP Enabled",     value=str(s.get("xp_enabled",True)),     inline=True)
    embed.add_field(name="AutoMod",        value=str(s.get("automod_enabled",False)),inline=True)
    embed.add_field(name="Msg Thresholds", value=str(len(s.get("role_thresholds",{}))), inline=True)
    embed.add_field(name="VC Thresholds",  value=str(len(s.get("vc_role_thresholds",{}))), inline=True)
    embed.add_field(name="Milestones",     value=str(len(s.get("milestone_thresholds",[]))), inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="listmilestones", description="List all configured milestones")
async def slash_listmilestones(interaction: discord.Interaction):
    data  = get_data()
    gdata = get_guild_data(data, interaction.guild.id)
    ms    = gdata["settings"].get("milestone_thresholds", [])
    vc_ms = gdata["settings"].get("vc_milestone_thresholds", [])
    embed = discord.Embed(title="🎯 Milestones", color=BOT_COLOR)
    embed.add_field(name="💬 Message",     value=", ".join(f"**{m:,}**" for m in ms) or "None",   inline=False)
    embed.add_field(name="🎙 Voice (min)", value=", ".join(f"**{m:,}**" for m in vc_ms) or "None", inline=False)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="dm", description="DM a specific member (Server Owner or allowed roles only)")
@app_commands.describe(member="Member to DM", message="Message to send")
async def slash_dm(interaction: discord.Interaction, member: discord.Member, message: str):
    data = get_data()
    gdata = get_guild_data(data, interaction.guild.id)
    
    if not can_use_dm_commands(interaction.user, interaction.guild, gdata):
        return await interaction.response.send_message(
            "🚫 Permission Denied — You don't have permission to use DM commands.", ephemeral=True)
    
    try:
        full_message = f"**Message from {interaction.user.display_name}** (Server: {interaction.guild.name})\n\n{message}"
        await member.send(full_message)
        await interaction.response.send_message(f"✅ Message sent to **{member.display_name}**!", ephemeral=True)
        log.info(f"Slash DM sent by {interaction.user} to {member}: {message[:50]}...")
    except discord.Forbidden:
        await interaction.response.send_message(
            f"❌ Cannot DM **{member.display_name}**! They have DMs disabled.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to send message: {e}", ephemeral=True)

# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════════════════

@tasks.loop(hours=24)
async def cleanup_old_data():
    """
    Prune date keys older than retention windows from the shared data dict.
    This is the ONLY periodic cleanup — no more check_daily_reset that was
    wiping all daily data on every midnight tick.
    Retention: 7 days daily | 8 weeks weekly | 12 months monthly.
    """
    data = get_data()
    now_ist = get_ist_now()
    
    cd = (now_ist - timedelta(days=7)).strftime("%Y-%m-%d")
    cw_t = now_ist - timedelta(weeks=8)
    cw = f"{cw_t.year}-W{cw_t.strftime('%W')}"
    cm = (now_ist.replace(day=1) - timedelta(days=365)).strftime("%Y-%m")
    
    for gid, gd in data.items():
        if gid.startswith("_"):
            continue
        for ud in gd.get("users", {}).values():
            for key, cutoff in [
                ("daily", cd), ("weekly", cw), ("monthly", cm),
                ("vc_daily", cd), ("vc_weekly", cw), ("vc_monthly", cm),
                ("xp_daily", cd), ("xp_weekly", cw), ("xp_monthly", cm),
            ]:
                if key in ud:
                    ud[key] = {k: v for k, v in ud[key].items() if k >= cutoff}
    save_data(data)
    log.info("✅ Cleaned up old data (IST timezone)")

@tasks.loop(minutes=5)
async def flush_vc_sessions_task():
    """
    Every 5 minutes: commit accumulated VC time to user data and write to disk.
    Session start times are reset to now so we don't double-count on the next flush.
    """
    data = get_data()
    _flush_vc_to_data(data)
    save_vc_sessions(data)
    save_data(data)
    log.info("🎙 VC sessions flushed to disk")

@tasks.loop(minutes=1)
async def auto_save_data():
    """
    Save the shared in-memory data dict to disk every minute.
    Does NOT re-load from disk — that would discard in-memory changes.
    """
    data = get_data()
    save_vc_sessions(data)   # keep VC sessions in the file too
    save_data(data)
    log.info("💾 Auto-save completed")

@bot.command(name="checkstats")
async def check_stats(ctx, member: discord.Member = None):
    """Check if stats are being counted"""
    member = member or ctx.author
    data = get_data()
    gdata = get_guild_data(data, ctx.guild.id)
    udata = get_user_data(gdata, member.id)
    
    embed = discord.Embed(title="📊 Stats Check", color=BOT_COLOR)
    embed.add_field(name="Total Messages", value=str(udata.get("total", 0)), inline=True)
    embed.add_field(name="Today's Messages", value=str(udata.get("daily", {}).get(today_key(), 0)), inline=True)
    embed.add_field(name="XP", value=str(udata.get("xp", 0)), inline=True)
    embed.add_field(name="Streak", value=str(udata.get("streak", 0)), inline=True)
    embed.add_field(name="VC Total", value=str(udata.get("vc_total", 0)), inline=True)
    
    await ctx.send(embed=embed)

# ── Error handlers ─────────────────────────────────────────────────────────────
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing: `{error.param.name}`. Use `!help`.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Invalid argument. @mention roles/users correctly.")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You don't have permission for that.")
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        log.error(f"Unhandled: {error}", exc_info=True)
        await ctx.send("❌ An unexpected error occurred.")

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:    await interaction.response.send_message(f"❌ Error: `{error}`", ephemeral=True)
    except: await interaction.followup.send(f"❌ Error: `{error}`", ephemeral=True)
    log.error(f"Slash error: {error}", exc_info=True)

@bot.command(name="forcesave")
async def force_save(ctx):
    """Force save current data (Owner only)"""
    data = get_data()
    save_data(data)
    await ctx.send("✅ Data force saved!")

# ── Run ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not TOKEN:
        log.critical("DISCORD_TOKEN not found in .env — aborting.")
    else:
        try:
            # Load data ONCE into the shared in-memory store at startup
            loaded = load_data()
            _data.update(loaded)
            log.info(f"📂 Loaded data from {DATA_FILE}")
            
            # Restore VC sessions from disk so users in VC at restart time
            # don't lose their accumulated time
            restore_vc_sessions(_data)
            
            # Run bot
            bot.run(TOKEN)
        except KeyboardInterrupt:
            log.info("🛑 Keyboard interrupt received - saving data...")
            _flush_vc_to_data(_data)
            save_vc_sessions(_data)
            save_data(_data)
            log.info("💾 Data saved. Goodbye!")
        except Exception as e:
            log.error(f"❌ Fatal error: {e}")
            _flush_vc_to_data(_data)
            save_vc_sessions(_data)
            save_data(_data)
            raise 
