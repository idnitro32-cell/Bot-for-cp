"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         CHILL PILL · Discord Activity Bot                      ║
║                           Premium Rank & Moderation System                     ║
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
BOT_COLOR = 0x7C3AED   # primary purple

# ── In-memory trackers ─────────────────────────────────────────────────────────
spam_tracker: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
vc_sessions:  dict[int, dict[int, float]]        = defaultdict(dict)

# ══════════════════════════════════════════════════════════════════════════════
#  DATA HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            try:    return json.load(f)
            except: return {}
    return {}

def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

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
        # ── Rank / roles
        "role_thresholds":      {},   # {role_id: msg_count}
        "vc_role_thresholds":   {},   # {role_id: vc_minutes}
        "conditional_paths":    [],   # [{trigger_role_id, thresholds:{role_id:count}}]
        # ── Channels
        "whitelisted_channels": [],
        "blacklisted_channels": [],
        # ── Members
        "blacklisted_members":  [],
        "owner_ids":            [],
        # ── Anti-spam
        "spam_interval":        3,
        # ── Toggles
        "count_emojis":         True,
        "include_vc_in_roles":  False,
        # ── Welcome
        "welcome_channel":      None,
        "welcome_message":      "Welcome to the server, {mention}! 🎉",
        "goodbye_channel":      None,
        "goodbye_message":      "**{name}** has left the server.",
        # ── Moderation / AutoMod
        "mute_role":            None,
        "log_channel":          None,
        "automod_enabled":      False,
        "automod_banned_words": [],
        "automod_max_mentions": 5,
        "automod_max_caps_pct": 80,
        # ── Tickets
        "ticket_category":      None,
        "ticket_support_role":  None,
        # ── Giveaways  (stored per guild in data["giveaways"])
    }

def get_user_data(guild_data: dict, user_id: int) -> dict:
    uid = str(user_id)
    if uid not in guild_data["users"]:
        guild_data["users"][uid] = {
            "total": 0, "daily": {}, "weekly": {}, "monthly": {},
            "vc_total": 0, "vc_daily": {}, "vc_weekly": {}, "vc_monthly": {},
            "warnings": 0,
        }
    u = guild_data["users"][uid]
    for k, v in [("vc_total",0),("vc_daily",{}),("vc_weekly",{}),("vc_monthly",{}),("warnings",0)]:
        u.setdefault(k, v)
    return u

def today_key():  return datetime.utcnow().strftime("%Y-%m-%d")
def week_key():
    d = datetime.utcnow()
    return f"{d.year}-W{d.strftime('%W')}"
def month_key():  return datetime.utcnow().strftime("%Y-%m")

def increment_counts(udata: dict, amount: int = 1):
    udata["total"]                = udata.get("total", 0) + amount
    udata["daily"][today_key()]   = udata["daily"].get(today_key(),  0) + amount
    udata["weekly"][week_key()]   = udata["weekly"].get(week_key(),  0) + amount
    udata["monthly"][month_key()] = udata["monthly"].get(month_key(),0) + amount

def increment_vc(udata: dict, minutes: float):
    m = round(minutes, 2)
    udata["vc_total"]                = round(udata.get("vc_total",0)+m, 2)
    udata["vc_daily"][today_key()]   = round(udata["vc_daily"].get(today_key(),0)+m, 2)
    udata["vc_weekly"][week_key()]   = round(udata["vc_weekly"].get(week_key(),0)+m, 2)
    udata["vc_monthly"][month_key()] = round(udata["vc_monthly"].get(month_key(),0)+m, 2)

def is_owner(user: discord.Member, guild: discord.Guild, guild_data: dict) -> bool:
    return (
        user.id == guild.owner_id
        or user.guild_permissions.administrator
        or user.id in guild_data["settings"].get("owner_ids", [])
    )

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
    bl  = s.get("blacklisted_channels",  [])
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

# ── Role assignment ────────────────────────────────────────────────────────────
def get_applicable_thresholds(member: discord.Member, guild_data: dict) -> dict:
    s    = guild_data["settings"]
    mids = {r.id for r in member.roles}
    for path in s.get("conditional_paths", []):
        tid = path.get("trigger_role_id")
        if tid and int(tid) in mids:
            return path.get("thresholds", {})
    return s.get("role_thresholds", {})

async def assign_roles(guild: discord.Guild, member: discord.Member, udata: dict, gdata: dict):
    s          = gdata["settings"]
    total      = udata.get("total", 0)
    vc_total   = udata.get("vc_total", 0)
    include_vc = s.get("include_vc_in_roles", False)
    if member.id in s.get("blacklisted_members", []): return

    effective  = total + (int(vc_total) if include_vc else 0)
    thresholds = get_applicable_thresholds(member, gdata)
    for rid_str, req in thresholds.items():
        role = guild.get_role(int(rid_str))
        if role and effective >= req and role not in member.roles:
            try:
                await member.add_roles(role, reason=f"Reached {req} messages")
                log.info(f"[Role] {role.name} → {member}")
            except discord.Forbidden: pass

    for rid_str, req_min in s.get("vc_role_thresholds", {}).items():
        role = guild.get_role(int(rid_str))
        if role and vc_total >= req_min and role not in member.roles:
            try:    await member.add_roles(role, reason=f"Reached {req_min} VC min")
            except: pass

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
    scores = sorted(gdata["users"].items(), key=lambda x: x[1].get("total",0), reverse=True)
    for i, (uid, _) in enumerate(scores, 1):
        if int(uid) == user_id: return i
    return len(scores)+1

# ══════════════════════════════════════════════════════════════════════════════
#  RANK CARD GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    suffix = "-Bold" if bold else ""
    for path in [
        f"/usr/share/fonts/truetype/liberation/LiberationSans{suffix}.ttf",
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
    ]:
        try: return ImageFont.truetype(path, size)
        except: pass
    return ImageFont.load_default()

def _lerp(c1, c2, t):
    return tuple(int(a + (b-a)*t) for a,b in zip(c1,c2))

def _progress_color(pct: float):
    if pct < 0.5: return _lerp((110,20,220),(230,30,160), pct*2)
    return _lerp((230,30,160),(255,200,30),(pct-0.5)*2)

def _draw_bar(draw, x1, y, x2, h, r, pct, c1, c2):
    draw.rounded_rectangle([x1,y,x2,y+h], radius=r, fill=(30,20,55,200))
    fw = max(int((x2-x1)*pct), 0)
    if fw > r*2:
        for i in range(fw):
            t   = i/max(fw-1,1)
            col = _lerp(c1,c2,t)
            draw.line([(x1+i,y+1),(x1+i,y+h-1)], fill=(*col,220))
        draw.rounded_rectangle([x1,y,x1+fw,y+h], radius=r, fill=None, outline=(*c2,155), width=1)
    return fw

async def fetch_avatar(url: str) -> Image.Image:
    async with aiohttp.ClientSession() as s:
        async with s.get(str(url)) as r:
            data = await r.read()
    return Image.open(io.BytesIO(data)).convert("RGBA")

def _make_circle(img: Image.Image, size: int) -> Image.Image:
    img  = img.resize((size,size), Image.LANCZOS)
    mask = Image.new("L",(size,size),0)
    ImageDraw.Draw(mask).ellipse((0,0,size,size), fill=255)
    img.putalpha(mask)
    return img

async def generate_rank_card(member: discord.Member, udata: dict, gdata: dict, rank_pos: int) -> discord.File:
    print("!!! RANK CARD GENERATOR CALLED - USING NEW VERSION !!!")  # DEBUG
    print(f"Avatar size will be: 100px (was 172)")  # DEBUG
    # ... rest of the function
    
async def generate_rank_card(member: discord.Member, udata: dict, gdata: dict, rank_pos: int) -> discord.File:
    """Generate a beautiful rank card with user stats and progress bars"""
    
    # Card dimensions - you can adjust these
    W, H = 950, 420  # Width, Height (taller for more space)
    AV = 110         # Avatar size (smaller = cleaner look)
    PAD = 32         # Padding from edges
    BAR_H = 30       # Progress bar height
    BAR_R = 15       # Progress bar roundness

    # Get user stats
    total = udata.get("total", 0)
    vc_total = udata.get("vc_total", 0.0)
    msgs_needed, next_role = get_next_milestone(member, total, gdata)
    vc_needed, vc_role = get_next_vc_milestone(member, vc_total, gdata)

    # Calculate message progress percentage
    thresholds = get_applicable_thresholds(member, gdata)
    prev_t, next_t = 0, None
    for _, req in sorted(thresholds.items(), key=lambda x: x[1]):
        if req > total:
            next_t = req
            break
        prev_t = req
    msg_pct = min((total - prev_t) / max((next_t or total + 1) - prev_t, 1), 1.0)

    # Calculate voice progress percentage
    vc_thresholds = gdata["settings"].get("vc_role_thresholds", {})
    prev_vc, next_vc = 0, None
    for _, req in sorted(vc_thresholds.items(), key=lambda x: x[1]):
        if req > vc_total:
            next_vc = req
            break
        prev_vc = req
    vc_pct = min((vc_total - prev_vc) / max((next_vc or vc_total + 1) - prev_vc, 1), 1.0) if (vc_thresholds or vc_total) else 0.0

    # ── Create the image canvas ──────────────────────────────────────────────
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Beautiful gradient background (purple to magenta)
    for y in range(H):
        t = y / H
        r = int(20 + 15 * t)   # Dark purple to lighter
        g = int(10 + 25 * t)   # Deep purple to magenta
        b = int(40 + 60 * t)   # Rich blue to purple
        draw.line([(0, y), (W, y)], fill=(r, g, b, 255))

    # Add glowing effects
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((-150, -100, 450, 320), fill=(120, 30, 220, 35))
    gd.ellipse((600, -80, 1100, 350), fill=(220, 40, 140, 30))
    gd.ellipse((250, 150, 650, 450), fill=(40, 20, 100, 20))
    img = Image.alpha_composite(img, glow)
    draw = ImageDraw.Draw(img)

    # Decorative borders
    draw.rounded_rectangle([3, 3, W - 3, H - 3], radius=24, fill=None, outline=(140, 70, 240, 180), width=3)
    draw.rounded_rectangle([8, 8, W - 8, H - 8], radius=20, fill=None, outline=(80, 40, 160, 100), width=2)

    # ── Avatar with glowing ring ─────────────────────────────────────────────
    pc = _progress_color(msg_pct)
    ring_size = AV + 20
    ax, ay = PAD, (H - ring_size) // 2
    
    # Draw the ring
    ring = Image.new("RGBA", (ring_size, ring_size), (0, 0, 0, 0))
    rd = ImageDraw.Draw(ring)
    rd.ellipse((0, 0, ring_size, ring_size), fill=(*pc, 140))
    rd.ellipse((4, 4, ring_size - 4, ring_size - 4), fill=(0, 0, 0, 0))
    rd.ellipse((8, 8, ring_size - 8, ring_size - 8), fill=(*pc, 80))
    img.paste(ring, (ax, ay), ring)

    # Get and paste the avatar
    av_img = await fetch_avatar(member.display_avatar.with_format("png").with_size(256).url)
    av_circle = _make_circle(av_img, AV)
    img.paste(av_circle, (ax + 10, ay + 10), av_circle)
    draw = ImageDraw.Draw(img)

    # ── BIG TEXT FONTS (customize these numbers!) ───────────────────────────
    f_name = _get_font(58, True)      # Username size (big!)
    f_rank = _get_font(34, True)      # Rank text size
    f_label = _get_font(24, True)     # Label text size
    f_small = _get_font(19)           # Small text size
    f_count = _get_font(48, True)     # Message count size (huge!)
    f_footer_lb = _get_font(20, True) # Footer label size
    f_footer_v = _get_font(36, True)  # Footer value size (big numbers!)

    # Position calculations
    tx = ax + ring_size + PAD
    bx2 = W - PAD

    # ── Draw Username ────────────────────────────────────────────────────────
    draw.text((tx, 22), member.display_name, font=f_name, fill=(255, 250, 255, 255))
    # Add a subtle shadow
    draw.text((tx + 2, 24), member.display_name, font=f_name, fill=(0, 0, 0, 30))

    # Discriminator (the #1234 part)
    if member.discriminator not in ("0", ""):
        draw.text((tx, 88), f"#{member.discriminator}", font=f_small, fill=(200, 170, 255, 200))

    # Rank text with stars
    rank_col = _progress_color(msg_pct)
    draw.text((tx, 122), f"⭐ Rank #{rank_pos} ⭐", font=f_rank, fill=(*rank_col, 255))

    # Message count (top right corner)
    draw.text((bx2, 22), f"{total:,}", font=f_count, fill=(220, 200, 255, 230), anchor="ra")
    draw.text((bx2 - 10, 42), "messages", font=f_small, fill=(180, 150, 220, 200), anchor="ra")

    # Voice time
    vc_h = int(vc_total // 60)
    vc_m = int(vc_total % 60)
    vc_text = f"{vc_h}h {vc_m}m" if vc_h else f"{vc_m}m"
    draw.text((bx2, 80), f"🎙️ {vc_text}", font=f_small, fill=(160, 220, 255, 220), anchor="ra")

    # ── Message Progress Bar ─────────────────────────────────────────────────
    bar_y = 175
    if next_role:
        lbl = f"🎯 NEXT ROLE: {next_role.name}  |  {msgs_needed:,} messages remaining"
        draw.text((tx, bar_y - 36), lbl, font=f_label, fill=(220, 200, 255, 230))
    else:
        draw.text((tx, bar_y - 36), "✨ MAXIMUM RANK ACHIEVED! ✨", font=f_label, fill=(255, 215, 0, 255))

    fw = _draw_bar(draw, tx, bar_y, bx2, BAR_H, BAR_R, msg_pct, (100, 20, 200), _progress_color(msg_pct))
    pct_text = f"{int(msg_pct * 100)}%"
    pct_x = tx + max(fw - draw.textlength(pct_text, font=f_small) - 8, 8)
    draw.text((pct_x, bar_y + 6), pct_text, font=f_small, fill=(255, 255, 255, 240))

    # ── Voice Progress Bar ───────────────────────────────────────────────────
    vc_y = bar_y + BAR_H + 60
    if vc_role:
        lbl = f"🎤 VOICE ROLE: {vc_role.name}  |  {vc_needed} minutes needed"
        draw.text((tx, vc_y - 36), lbl, font=f_label, fill=(180, 220, 255, 230))
    elif vc_thresholds:
        draw.text((tx, vc_y - 36), "🏆 MAX VC RANK ACHIEVED! 🏆", font=f_label, fill=(255, 215, 0, 255))
    else:
        draw.text((tx, vc_y - 36), "🎙️ No VC roles configured", font=f_label, fill=(180, 180, 200, 200))

    vfw = _draw_bar(draw, tx, vc_y, bx2, BAR_H, BAR_R, vc_pct, (20, 100, 200), (60, 210, 255))
    vc_pct_text = f"{int(vc_pct * 100)}%"
    vc_pct_x = tx + max(vfw - draw.textlength(vc_pct_text, font=f_small) - 8, 8)
    draw.text((vc_pct_x, vc_y + 6), vc_pct_text, font=f_small, fill=(255, 255, 255, 240))

    # ── Footer Stats (Daily, Weekly, Monthly, Total) ─────────────────────────
    sep_y = H - 88
    for i in range(3):
        draw.line([(tx, sep_y + i), (bx2, sep_y + i)], fill=(100, 70, 160, 80 - i * 20), width=1)

    daily = udata["daily"].get(today_key(), 0)
    weekly = udata["weekly"].get(week_key(), 0)
    monthly = udata["monthly"].get(month_key(), 0)

    stats = [("📅 TODAY", daily), ("📊 WEEK", weekly), ("📈 MONTH", monthly), ("🏆 TOTAL", total)]
    col_w = (bx2 - tx) // 4

    for i, (lbl, val) in enumerate(stats):
        sx = tx + i * col_w + (col_w // 2)
        draw.text((sx, sep_y + 14), lbl, font=f_footer_lb, fill=(160, 130, 210, 220), anchor="ma")
        draw.text((sx, sep_y + 44), f"{val:,}", font=f_footer_v, fill=(240, 230, 255, 255), anchor="ma")

    # ── Decorative Corner Accents ───────────────────────────────────────────
    corner_size = 30
    # Top-left
    draw.line([(10, 15), (10 + corner_size, 15)], fill=(140, 70, 240, 100), width=2)
    draw.line([(10, 15), (10, 15 + corner_size)], fill=(140, 70, 240, 100), width=2)
    # Top-right
    draw.line([(W - 10, 15), (W - 10 - corner_size, 15)], fill=(140, 70, 240, 100), width=2)
    draw.line([(W - 10, 15), (W - 10, 15 + corner_size)], fill=(140, 70, 240, 100), width=2)
    # Bottom-left
    draw.line([(10, H - 15), (10 + corner_size, H - 15)], fill=(140, 70, 240, 100), width=2)
    draw.line([(10, H - 15), (10, H - 15 - corner_size)], fill=(140, 70, 240, 100), width=2)
    # Bottom-right
    draw.line([(W - 10, H - 15), (W - 10 - corner_size, H - 15)], fill=(140, 70, 240, 100), width=2)
    draw.line([(W - 10, H - 15), (W - 10, H - 15 - corner_size)], fill=(140, 70, 240, 100), width=2)

    # ── Save and return the image ───────────────────────────────────────────
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="rank_card.png")
    
# ══════════════════════════════════════════════════════════════════════════════
#  LEADERBOARD / STATS HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def build_leaderboard(guild: discord.Guild, gdata: dict, period: str, top: int = 10, mode: str = "msg"):
    key_fn = {"daily":today_key,"weekly":week_key,"monthly":month_key}
    scores = []
    for uid_str, udata in gdata["users"].items():
        if mode == "msg":
            count = udata.get("total",0) if period=="total" else udata.get(period,{}).get(key_fn[period](),0)
        else:
            count = udata.get("vc_total",0) if period=="total" else udata.get(f"vc_{period}",{}).get(key_fn[period](),0)
        if count == 0: continue
        m = guild.get_member(int(uid_str))
        if m: scores.append((m, count))
    scores.sort(key=lambda x:x[1], reverse=True)
    return scores[:top]

def leaderboard_embed(scores, period: str, guild: discord.Guild, mode: str = "msg") -> discord.Embed:
    medals     = {1:"🥇",2:"🥈",3:"🥉"}
    period_lbl = {"total":"All Time","daily":"Today","weekly":"This Week","monthly":"This Month"}
    title      = f"{'📊 Message' if mode=='msg' else '🎙 Voice'} Leaderboard — {period_lbl[period]}"
    embed      = discord.Embed(title=title, color=BOT_COLOR, timestamp=datetime.utcnow())
    if not scores:
        embed.description = "No data yet."
    else:
        unit  = "msgs" if mode=="msg" else "min"
        lines = [f"{medals.get(i,f'`{i:>2}.`')} **{m.display_name}** — {c:,} {unit}"
                 for i,(m,c) in enumerate(scores,1)]
        embed.description = "\n".join(lines)
    embed.set_footer(text=guild.name, icon_url=guild.icon.url if guild.icon else None)
    return embed

def stats_embed(member: discord.Member, udata: dict, gdata: dict, guild: discord.Guild) -> discord.Embed:
    daily  = udata["daily"].get(today_key(),0)
    weekly = udata["weekly"].get(week_key(),0)
    monthly= udata["monthly"].get(month_key(),0)
    total  = udata.get("total",0)
    vc_d   = udata.get("vc_daily",{}).get(today_key(),0)
    vc_w   = udata.get("vc_weekly",{}).get(week_key(),0)
    vc_m   = udata.get("vc_monthly",{}).get(month_key(),0)
    vc_t   = udata.get("vc_total",0)
    warns  = udata.get("warnings",0)
    embed  = discord.Embed(
        title=f"📈 Stats — {member.display_name}",
        color=member.color if member.color.value else BOT_COLOR,
        timestamp=datetime.utcnow())
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="💬 Messages",
        value=f"Today: **{daily:,}**\nWeek: **{weekly:,}**\nMonth: **{monthly:,}**\nTotal: **{total:,}**", inline=True)
    embed.add_field(name="🎙 Voice (min)",
        value=f"Today: **{vc_d}**\nWeek: **{vc_w}**\nMonth: **{vc_m}**\nTotal: **{vc_t}**", inline=True)
    embed.add_field(name="⚠️ Warnings", value=str(warns), inline=True)
    mn, nr = get_next_milestone(member, total, gdata)
    vn, vr = get_next_vc_milestone(member, vc_t, gdata)
    ms = []
    if nr: ms.append(f"🎯 **{mn:,}** msgs → **{nr.name}**")
    if vr: ms.append(f"🎙 **{vn}** min → **{vr.name}**")
    if ms: embed.add_field(name="Next Milestones", value="\n".join(ms), inline=False)
    return embed

# ══════════════════════════════════════════════════════════════════════════════
#  AUTOMOD HELPER
# ══════════════════════════════════════════════════════════════════════════════
async def run_automod(message: discord.Message, gdata: dict) -> bool:
    """Returns True if message was actioned (deleted). Sends warning if needed."""
    s = gdata["settings"]
    if not s.get("automod_enabled", False): return False
    if message.author.guild_permissions.manage_messages: return False

    content  = message.content
    reasons  = []

    # Banned words
    for word in s.get("automod_banned_words", []):
        if word.lower() in content.lower():
            reasons.append(f"banned word: `{word}`")
            break

    # Excessive mentions
    max_men = s.get("automod_max_mentions", 5)
    if len(message.mentions) > max_men:
        reasons.append(f"too many mentions ({len(message.mentions)})")

    # Caps spam
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
            f"⚠️ {message.author.mention} message removed — {reason_str}. "
            f"(Warning {udata['warnings']})", delete_after=8)
    except: pass

    # Log
    log_cid = s.get("log_channel")
    if log_cid:
        log_ch = message.guild.get_channel(int(log_cid))
        if log_ch:
            embed = discord.Embed(title="🛡️ AutoMod Action", color=0xFF4444,
                                  timestamp=datetime.utcnow())
            embed.add_field(name="User",    value=str(message.author))
            embed.add_field(name="Reason",  value=reason_str)
            embed.add_field(name="Channel", value=message.channel.mention)
            embed.add_field(name="Content", value=content[:500] or "(empty)", inline=False)
            try: await log_ch.send(embed=embed)
            except: pass
    return True

# ══════════════════════════════════════════════════════════════════════════════
#  INTERACTIVE HELP MENU  (dropdown UI like the screenshot)
# ══════════════════════════════════════════════════════════════════════════════
HELP_CATEGORIES = {
    "counter":    {
        "emoji": "⚡", "label": "Counter", "desc": "Activity tracking",
        "value": (
            "`!rank [@user]` — Rank card with progress bars\n"
            "`!stats [@user]` — Message & VC stats breakdown\n"
            "`!lb [period]` — Message leaderboard\n"
            "`!vclb [period]` — Voice time leaderboard\n"
            "Periods: `total` · `daily` · `weekly` · `monthly`"
        ),
    },
    "vanity":     {
        "emoji": "🏅", "label": "Vanity Roles", "desc": "Status role systems",
        "value": (
            "`!setrole @role <count>` — Set message role threshold\n"
            "`!removerole @role` — Remove message threshold\n"
            "`!listroles` — List all message thresholds\n"
            "`!setvcrole @role <min>` — Set VC role threshold\n"
            "`!removevcrole @role` — Remove VC threshold\n"
            "`!listvcroles` — List VC role thresholds"
        ),
    },
    "guild_tags": {
        "emoji": "🔀", "label": "Guild Tags", "desc": "Conditional role paths",
        "value": (
            "`!addpath @trigger @role <count>` — Add conditional role path\n"
            "`  ` ↳ Members with @trigger get @role at <count> msgs\n"
            "`  ` ↳ Separate from default role track\n"
            "`!removepath @trigger` — Remove a path\n"
            "`!listpaths` — List all conditional paths"
        ),
    },
    "admin":      {
        "emoji": "🔧", "label": "Admin", "desc": "Server administration",
        "value": (
            "`!settings` — View server config overview\n"
            "`!setspam <sec>` — Anti-spam interval\n"
            "`!toggleemoji` — Toggle emoji-only counting\n"
            "`!togglevc` — Toggle VC time in role thresholds\n"
            "`!whitelist #ch` / `!blacklist #ch` — Channel control\n"
            "`!clearwhitelist` / `!clearblacklist`\n"
            "`!listchannels` — Show channel settings\n"
            "`!addowner @user` / `!removeowner @user`"
        ),
    },
    "moderation": {
        "emoji": "🔨", "label": "Moderation", "desc": "Moderation tools",
        "value": (
            "`!warn @user [reason]` — Warn a user\n"
            "`!warnings @user` — View warnings\n"
            "`!clearwarnings @user` — Clear all warnings\n"
            "`!mute @user [minutes] [reason]` — Mute a user\n"
            "`!unmute @user` — Unmute a user\n"
            "`!kick @user [reason]` — Kick a user\n"
            "`!ban @user [reason]` — Ban a user\n"
            "`!unban <user_id>` — Unban a user\n"
            "`!purge <count>` — Delete messages (up to 100)"
        ),
    },
    "automod":    {
        "emoji": "🤖", "label": "AutoMod", "desc": "Automated moderation",
        "value": (
            "`!automod on/off` — Enable/disable AutoMod\n"
            "`!addword <word>` — Add banned word\n"
            "`!removeword <word>` — Remove banned word\n"
            "`!listwords` — List banned words\n"
            "`!setmaxmentions <n>` — Max mentions per message\n"
            "`!setmaxcaps <pct>` — Max caps % (e.g. 80)\n"
            "`!setlogchannel #ch` — Channel for AutoMod logs"
        ),
    },
    "welcome":    {
        "emoji": "👋", "label": "Welcome", "desc": "Welcome/goodbye systems",
        "value": (
            "`!setwelcome #channel` — Set welcome channel\n"
            "`!setwelcomemsg <msg>` — Set welcome message\n"
            "`  ` Placeholders: `{mention}` `{name}` `{server}`\n"
            "`!setgoodbye #channel` — Set goodbye channel\n"
            "`!setgoodbyemsg <msg>` — Set goodbye message\n"
            "`!testwelcome` — Preview welcome message\n"
            "`!testgoodbye` — Preview goodbye message"
        ),
    },
    "info":       {
        "emoji": "ℹ️", "label": "Info", "desc": "Information & stats",
        "value": (
            "`!serverinfo` — Server information\n"
            "`!userinfo [@user]` — User information\n"
            "`!roleinfo @role` — Role information\n"
            "`!botinfo` — About this bot\n"
            "`!ping` — Bot latency\n"
            "`!avatar [@user]` — View user avatar"
        ),
    },
    "giveaway":   {
        "emoji": "🎁", "label": "Giveaway", "desc": "Host giveaways",
        "value": (
            "`!gcreate` — Start a giveaway (interactive)\n"
            "`!gend <msg_id>` — End a giveaway early\n"
            "`!greroll <msg_id>` — Reroll a giveaway winner\n"
            "`!glist` — List active giveaways"
        ),
    },
    "ticket":     {
        "emoji": "🎫", "label": "Ticket", "desc": "Support ticket system",
        "value": (
            "`!ticketsetup` — Set up ticket system\n"
            "`!setticketcategory #cat` — Set ticket category\n"
            "`!setticketsupport @role` — Set support role\n"
            "`!closeticket` — Close current ticket\n"
            "`!newticket` — Open a new ticket"
        ),
    },
    "boycott":    {
        "emoji": "🚫", "label": "Boycott", "desc": "Blacklist system",
        "value": (
            "`!blacklistmember @user` — Block role upgrades\n"
            "`!unblacklistmember @user` — Unblock\n"
            "`!addmsgs @user <n>` — Manually add messages\n"
            "`!removemsgs @user <n>` — Manually remove messages\n"
            "`!resetuser @user` — Reset all stats for a user"
        ),
    },
    "voice":      {
        "emoji": "🔊", "label": "Voice", "desc": "Voice management",
        "value": (
            "`!vclb [period]` — Voice time leaderboard\n"
            "`!setvcrole @role <minutes>` — VC role threshold\n"
            "`!removevcrole @role` — Remove VC threshold\n"
            "`!listvcroles` — List VC role thresholds\n"
            "`!togglevc` — Include VC time in msg role check\n"
            "`  ` VC time tracked automatically on join/leave"
        ),
    },
   
    "utility":    {
        "emoji": "⚙️", "label": "Utility", "desc": "Useful utilities",
        "value": (
            "`!help` — This help menu\n"
            "`!settings` — Server settings overview\n"
            "`!listchannels` — Channel whitelist/blacklist\n"
            "`!listpaths` — Conditional role paths\n"
            "`!listroles` — Message role thresholds\n"
            "`!listvcroles` — VC role thresholds"
        ),
    },
}

class HelpCategorySelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label=v["label"],
                value=k,
                description=v["desc"],
                emoji=v["emoji"],
            )
            for k, v in HELP_CATEGORIES.items()
        ]
        super().__init__(placeholder="Select a command category...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        cat   = HELP_CATEGORIES[self.values[0]]
        embed = discord.Embed(
            title=f"{cat['emoji']} {cat['label']} — {cat['desc']}",
            description=cat["value"],
            color=BOT_COLOR,
        )
        embed.set_footer(text=f"{BOT_NAME} • Use !help to return to this menu")
        await interaction.response.edit_message(embed=embed, view=self.view)

class HelpView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(HelpCategorySelect())

def help_home_embed(guild: discord.Guild, prefix: str = "!") -> discord.Embed:
    embed = discord.Embed(
        color=BOT_COLOR,
        description=(
            f"I'm **{BOT_NAME}**, your ultimate Discord companion!\n"
            f"My prefix for this server is `{prefix}`\n\n"
            "**▶ Command Categories**"
        ),
    )
    # Two-column layout of categories
    col1, col2 = [], []
    items = list(HELP_CATEGORIES.values())
    for i, cat in enumerate(items):
        line = f"{cat['emoji']} **{cat['label']}** — {cat['desc']}"
        (col1 if i % 2 == 0 else col2).append(line)
    embed.add_field(name="\u200b", value="\n".join(col1), inline=True)
    embed.add_field(name="\u200b", value="\n".join(col2), inline=True)
    embed.set_footer(text=f"{BOT_NAME} • Select a category below")
    return embed

# ══════════════════════════════════════════════════════════════════════════════
#  BOT SETUP
# ══════════════════════════════════════════════════════════════════════════════
intents                = discord.Intents.default()
intents.message_content= True
intents.members        = True
intents.voice_states   = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None, case_insensitive=True)

# ══════════════════════════════════════════════════════════════════════════════
#  EVENTS
# ══════════════════════════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    await bot.tree.sync()
    log.info(f"Logged in as {bot.user} ({bot.user.id})")
    cleanup_old_data.start()
    flush_vc_sessions.start()

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild: return
    data  = load_data()
    gdata = get_guild_data(data, message.guild.id)

    # AutoMod first
    if await run_automod(message, gdata):
        return

    if not should_count_message(message, gdata):
        await bot.process_commands(message); return

    interval = gdata["settings"].get("spam_interval", 3)
    if not check_spam(message.guild.id, message.author.id, interval):
        await bot.process_commands(message); return

    udata = get_user_data(gdata, message.author.id)
    increment_counts(udata)
    save_data(data)
    await assign_roles(message.guild, message.author, udata, gdata)
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return
    gid = member.guild.id
    if after.channel and not before.channel:
        vc_sessions[gid][member.id] = time.time()
    elif before.channel and not after.channel:
        join_time = vc_sessions[gid].pop(member.id, None)
        if join_time:
            minutes = (time.time()-join_time)/60
            data    = load_data()
            gdata   = get_guild_data(data, gid)
            udata   = get_user_data(gdata, member.id)
            increment_vc(udata, minutes)
            save_data(data)
            await assign_roles(member.guild, member, udata, gdata)

@bot.event
async def on_member_join(member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, member.guild.id)
    s     = gdata["settings"]
    ch_id = s.get("welcome_channel")
    if not ch_id: return
    ch = member.guild.get_channel(int(ch_id))
    if not ch: return
    msg = s.get("welcome_message", "Welcome {mention}!")
    msg = msg.replace("{mention}", member.mention).replace("{name}", member.display_name).replace("{server}", member.guild.name)
    embed = discord.Embed(description=msg, color=0x57F287)
    embed.set_thumbnail(url=member.display_avatar.url)
    try: await ch.send(embed=embed)
    except: pass

@bot.event
async def on_member_remove(member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, member.guild.id)
    s     = gdata["settings"]
    ch_id = s.get("goodbye_channel")
    if not ch_id: return
    ch = member.guild.get_channel(int(ch_id))
    if not ch: return
    msg = s.get("goodbye_message", "**{name}** has left.")
    msg = msg.replace("{mention}", member.mention).replace("{name}", member.display_name).replace("{server}", member.guild.name)
    embed = discord.Embed(description=msg, color=0xED4245)
    try: await ch.send(embed=embed)
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  PREFIX COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

# ── Help ───────────────────────────────────────────────────────────────────────
@bot.command(name="help")
async def prefix_help(ctx):
    banner = generate_banner()
    view   = HelpView()
    embed  = help_home_embed(ctx.guild)
    await ctx.send(file=banner)
    await ctx.send(embed=embed, view=view)

# ── Rank / Stats ───────────────────────────────────────────────────────────────
@bot.command(name="rank")
async def prefix_rank(ctx, member: discord.Member = None):
    member   = member or ctx.author
    data     = load_data()
    gdata    = get_guild_data(data, ctx.guild.id)
    udata    = get_user_data(gdata, member.id)
    rank_pos = get_rank_position(ctx.guild, gdata, member.id)
    async with ctx.typing():
        try:
            card = await generate_rank_card(member, udata, gdata, rank_pos)
            await ctx.send(file=card)
        except Exception as e:
            log.error(f"Rank card: {e}", exc_info=True)
            await ctx.send("❌ Could not generate rank card. Check `Pillow` and `aiohttp` are installed.")

@bot.command(name="stats")
async def prefix_stats(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = load_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    await ctx.send(embed=stats_embed(member, udata, gdata, ctx.guild))

@bot.command(name="lb")
async def prefix_lb(ctx, period: str = "total"):
    period = period.lower()
    if period not in ("total","daily","weekly","monthly"):
        return await ctx.send("❌ Choose: `total` `daily` `weekly` `monthly`.")
    data   = load_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, period)
    await ctx.send(embed=leaderboard_embed(scores, period, ctx.guild))

@bot.command(name="vclb")
async def prefix_vclb(ctx, period: str = "total"):
    period = period.lower()
    if period not in ("total","daily","weekly","monthly"):
        return await ctx.send("❌ Choose: `total` `daily` `weekly` `monthly`.")
    data   = load_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    scores = build_leaderboard(ctx.guild, gdata, period, mode="vc")
    await ctx.send(embed=leaderboard_embed(scores, period, ctx.guild, mode="vc"))

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
    await ctx.send(embed=embed)

@bot.command(name="serverinfo")
async def prefix_serverinfo(ctx):
    g = ctx.guild
    embed = discord.Embed(title=g.name, color=BOT_COLOR)
    embed.set_thumbnail(url=g.icon.url if g.icon else None)
    embed.add_field(name="Owner",    value=str(g.owner))
    embed.add_field(name="Members",  value=str(g.member_count))
    embed.add_field(name="Channels", value=str(len(g.channels)))
    embed.add_field(name="Roles",    value=str(len(g.roles)))
    embed.add_field(name="Created",  value=g.created_at.strftime("%Y-%m-%d"))
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
    embed.add_field(name="ID",       value=str(role.id))
    embed.add_field(name="Members",  value=str(len(role.members)))
    embed.add_field(name="Hoisted",  value=str(role.hoist))
    embed.add_field(name="Mentionable", value=str(role.mentionable))
    embed.add_field(name="Position", value=str(role.position))
    await ctx.send(embed=embed)

# ── Moderation ─────────────────────────────────────────────────────────────────
@bot.command(name="warn")
@commands.has_permissions(manage_messages=True)
async def prefix_warn(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    udata = get_user_data(gdata, member.id)
    udata["warnings"] = udata.get("warnings",0)+1
    save_data(data)
    await ctx.send(f"⚠️ **{member.display_name}** warned. Reason: {reason} (Total: {udata['warnings']})")
    try: await member.send(f"You were warned in **{ctx.guild.name}**: {reason}")
    except: pass

@bot.command(name="warnings")
async def prefix_warnings(ctx, member: discord.Member = None):
    member = member or ctx.author
    data   = load_data()
    gdata  = get_guild_data(data, ctx.guild.id)
    udata  = get_user_data(gdata, member.id)
    await ctx.send(f"⚠️ **{member.display_name}** has **{udata.get('warnings',0)}** warning(s).")

@bot.command(name="clearwarnings")
@commands.has_permissions(manage_messages=True)
async def prefix_clearwarnings(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    udata = get_user_data(gdata, member.id)
    udata["warnings"] = 0
    save_data(data)
    await ctx.send(f"✅ Cleared all warnings for **{member.display_name}**.")

@bot.command(name="mute")
@commands.has_permissions(manage_roles=True)
async def prefix_mute(ctx, member: discord.Member, minutes: int = 0, *, reason: str = "No reason"):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    mr_id = s.get("mute_role")
    if not mr_id:
        return await ctx.send("❌ No mute role set. Use `!setmuterole @role`.")
    role = ctx.guild.get_role(int(mr_id))
    if not role: return await ctx.send("❌ Mute role not found.")
    await member.add_roles(role, reason=reason)
    await ctx.send(f"🔇 **{member.display_name}** muted. Reason: {reason}"
                   + (f" ({minutes}min)" if minutes else ""))
    if minutes > 0:
        await asyncio.sleep(minutes*60)
        await member.remove_roles(role, reason="Auto-unmute")

@bot.command(name="unmute")
@commands.has_permissions(manage_roles=True)
async def prefix_unmute(ctx, member: discord.Member):
    data  = load_data()
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
async def prefix_purge(ctx, count: int):
    if count < 1 or count > 100:
        return await ctx.send("❌ Count must be 1–100.")
    deleted = await ctx.channel.purge(limit=count+1)
    await ctx.send(f"🗑️ Deleted **{len(deleted)-1}** messages.", delete_after=5)

@bot.command(name="setmuterole")
async def prefix_setmuterole(ctx, role: discord.Role):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["mute_role"] = str(role.id)
    save_data(data)
    await ctx.send(f"✅ Mute role set to **{role.name}**.")

# ── AutoMod settings ───────────────────────────────────────────────────────────
@bot.command(name="automod")
async def prefix_automod(ctx, state: str):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    enabled = state.lower() in ("on","true","enable","1")
    gdata["settings"]["automod_enabled"] = enabled
    save_data(data)
    await ctx.send(f"✅ AutoMod **{'enabled' if enabled else 'disabled'}**.")

@bot.command(name="addword")
async def prefix_addword(ctx, *, word: str):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    words = gdata["settings"].setdefault("automod_banned_words", [])
    if word.lower() in words: return await ctx.send("ℹ️ Word already banned.")
    words.append(word.lower())
    save_data(data)
    await ctx.send(f"✅ Added `{word}` to banned words.")

@bot.command(name="removeword")
async def prefix_removeword(ctx, *, word: str):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    words = gdata["settings"].get("automod_banned_words", [])
    if word.lower() not in words: return await ctx.send("ℹ️ Word not in list.")
    words.remove(word.lower())
    save_data(data)
    await ctx.send(f"✅ Removed `{word}` from banned words.")

@bot.command(name="listwords")
async def prefix_listwords(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    words = gdata["settings"].get("automod_banned_words", [])
    if not words: return await ctx.send("ℹ️ No banned words.")
    await ctx.send(embed=discord.Embed(
        title="🚫 Banned Words", description=", ".join(f"`{w}`" for w in words), color=BOT_COLOR))

@bot.command(name="setmaxmentions")
async def prefix_setmaxmentions(ctx, n: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["automod_max_mentions"] = n
    save_data(data)
    await ctx.send(f"✅ Max mentions per message: **{n}**.")

@bot.command(name="setmaxcaps")
async def prefix_setmaxcaps(ctx, pct: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["automod_max_caps_pct"] = pct
    save_data(data)
    await ctx.send(f"✅ Max caps percentage: **{pct}%**.")

@bot.command(name="setlogchannel")
async def prefix_setlogchannel(ctx, channel: discord.TextChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["log_channel"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Log channel set to {channel.mention}.")

# ── Welcome / Goodbye ──────────────────────────────────────────────────────────
@bot.command(name="setwelcome")
async def prefix_setwelcome(ctx, channel: discord.TextChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["welcome_channel"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Welcome channel: {channel.mention}")

@bot.command(name="setwelcomemsg")
async def prefix_setwelcomemsg(ctx, *, msg: str):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["welcome_message"] = msg
    save_data(data)
    await ctx.send(f"✅ Welcome message updated.")

@bot.command(name="setgoodbye")
async def prefix_setgoodbye(ctx, channel: discord.TextChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["goodbye_channel"] = str(channel.id)
    save_data(data)
    await ctx.send(f"✅ Goodbye channel: {channel.mention}")

@bot.command(name="setgoodbyemsg")
async def prefix_setgoodbyemsg(ctx, *, msg: str):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["goodbye_message"] = msg
    save_data(data)
    await ctx.send(f"✅ Goodbye message updated.")

@bot.command(name="testwelcome")
async def prefix_testwelcome(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    msg   = s.get("welcome_message","Welcome {mention}!")
    msg   = msg.replace("{mention}",ctx.author.mention).replace("{name}",ctx.author.display_name).replace("{server}",ctx.guild.name)
    embed = discord.Embed(description=msg, color=0x57F287)
    embed.set_thumbnail(url=ctx.author.display_avatar.url)
    await ctx.send("**Preview:**", embed=embed)

@bot.command(name="testgoodbye")
async def prefix_testgoodbye(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    msg   = s.get("goodbye_message","**{name}** has left.")
    msg   = msg.replace("{mention}",ctx.author.mention).replace("{name}",ctx.author.display_name).replace("{server}",ctx.guild.name)
    embed = discord.Embed(description=msg, color=0xED4245)
    await ctx.send("**Preview:**", embed=embed)

# ── Giveaway ───────────────────────────────────────────────────────────────────
active_giveaways = {}   # {message_id: {channel_id, end_time, prize, winners_count, participants}}

@bot.command(name="gcreate")
@commands.has_permissions(manage_guild=True)
async def prefix_gcreate(ctx):
    """Interactive giveaway creation."""
    def check(m): return m.author==ctx.author and m.channel==ctx.channel

    await ctx.send("🎁 **Giveaway Setup**\nHow long should it last? (e.g. `10m`, `1h`, `2d`)")
    try: dur_msg = await bot.wait_for("message", check=check, timeout=60)
    except asyncio.TimeoutError: return await ctx.send("❌ Timed out.")

    dur_raw = dur_msg.content.strip().lower()
    seconds = 0
    if dur_raw.endswith("d"):   seconds = int(dur_raw[:-1])*86400
    elif dur_raw.endswith("h"): seconds = int(dur_raw[:-1])*3600
    elif dur_raw.endswith("m"): seconds = int(dur_raw[:-1])*60
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
    prize = p_msg.content.strip()

    end_time = datetime.utcnow() + timedelta(seconds=seconds)
    embed = discord.Embed(
        title=f"🎁 GIVEAWAY — {prize}",
        description=(
            f"React with 🎉 to enter!\n\n"
            f"Winners: **{winners}**\n"
            f"Ends: <t:{int(end_time.timestamp())}:R>"
        ),
        color=0xF1C40F,
    )
    embed.set_footer(text=f"Hosted by {ctx.author.display_name}")
    ga_msg = await ctx.send(embed=embed)
    await ga_msg.add_reaction("🎉")

    active_giveaways[ga_msg.id] = {
        "channel_id":   ctx.channel.id,
        "end_time":     end_time.isoformat(),
        "prize":        prize,
        "winners_count":winners,
        "host_id":      ctx.author.id,
    }

    await asyncio.sleep(seconds)
    await _end_giveaway(ga_msg.id, ctx.guild)

async def _end_giveaway(msg_id: int, guild: discord.Guild):
    ga = active_giveaways.pop(msg_id, None)
    if not ga: return
    ch  = guild.get_channel(ga["channel_id"])
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
    if msg_id not in active_giveaways:
        return await ctx.send("❌ No active giveaway with that ID.")
    await _end_giveaway(msg_id, ctx.guild)

@bot.command(name="glist")
async def prefix_glist(ctx):
    if not active_giveaways:
        return await ctx.send("ℹ️ No active giveaways.")
    lines = [f"• **{ga['prize']}** — ends <t:{int(datetime.fromisoformat(ga['end_time']).timestamp())}:R>"
             for mid, ga in active_giveaways.items()]
    await ctx.send(embed=discord.Embed(title="🎁 Active Giveaways",
                                        description="\n".join(lines), color=0xF1C40F))

# ── Ticket system ──────────────────────────────────────────────────────────────
@bot.command(name="setticketcategory")
async def prefix_setticketcat(ctx, category: discord.CategoryChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["ticket_category"] = str(category.id)
    save_data(data)
    await ctx.send(f"✅ Ticket category: **{category.name}**")

@bot.command(name="setticketsupport")
async def prefix_setticketsupport(ctx, role: discord.Role):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["ticket_support_role"] = str(role.id)
    save_data(data)
    await ctx.send(f"✅ Support role: **{role.name}**")

@bot.command(name="newticket")
async def prefix_newticket(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    cat_id = s.get("ticket_category")
    if not cat_id: return await ctx.send("❌ No ticket category set. Ask an admin to use `!setticketcategory`.")
    cat   = ctx.guild.get_channel(int(cat_id))
    if not cat: return await ctx.send("❌ Ticket category not found.")

    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        ctx.author:             discord.PermissionOverwrite(view_channel=True, send_messages=True),
    }
    sup_rid = s.get("ticket_support_role")
    if sup_rid:
        sup_role = ctx.guild.get_role(int(sup_rid))
        if sup_role: overwrites[sup_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

    ch = await ctx.guild.create_text_channel(
        f"ticket-{ctx.author.name}", category=cat, overwrites=overwrites)
    embed = discord.Embed(
        title="🎫 Support Ticket",
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
        "It is certain.","Without a doubt.","Yes, definitely.",
        "Most likely.","Outlook good.","Yes.",
        "Reply hazy, try again.","Ask again later.","Cannot predict now.",
        "Don't count on it.","My reply is no.","Very doubtful.",
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

# ── Role thresholds ────────────────────────────────────────────────────────────
@bot.command(name="setrole")
async def prefix_setrole(ctx, role: discord.Role, count: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if count < 1: return await ctx.send("❌ Count must be ≥ 1.")
    gdata["settings"]["role_thresholds"][str(role.id)] = count
    save_data(data)
    await ctx.send(f"✅ **{role.name}** assigned at **{count:,}** messages.")

@bot.command(name="removerole")
async def prefix_removerole(ctx, role: discord.Role):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    t = gdata["settings"].get("role_thresholds", {})
    if str(role.id) not in t: return await ctx.send(f"❌ No threshold for **{role.name}**.")
    del t[str(role.id)]
    save_data(data)
    await ctx.send(f"✅ Removed threshold for **{role.name}**.")

@bot.command(name="listroles")
async def prefix_listroles(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    t     = gdata["settings"].get("role_thresholds", {})
    if not t: return await ctx.send("ℹ️ No message role thresholds set.")
    lines = [f"• <@&{rid}> — **{c:,}** msgs" for rid, c in sorted(t.items(), key=lambda x: x[1])]
    await ctx.send(embed=discord.Embed(title="🎭 Message Role Thresholds",
                                       description="\n".join(lines), color=BOT_COLOR))

@bot.command(name="setvcrole")
async def prefix_setvcrole(ctx, role: discord.Role, minutes: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["vc_role_thresholds"][str(role.id)] = minutes
    save_data(data)
    await ctx.send(f"✅ **{role.name}** assigned at **{minutes:,}** VC minutes.")

@bot.command(name="removevcrole")
async def prefix_removevcrole(ctx, role: discord.Role):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    t = gdata["settings"].get("vc_role_thresholds", {})
    if str(role.id) not in t: return await ctx.send(f"❌ No VC threshold for **{role.name}**.")
    del t[str(role.id)]
    save_data(data)
    await ctx.send(f"✅ Removed VC threshold for **{role.name}**.")

@bot.command(name="listvcroles")
async def prefix_listvcroles(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    t     = gdata["settings"].get("vc_role_thresholds", {})
    if not t: return await ctx.send("ℹ️ No VC role thresholds set.")
    lines = [f"• <@&{rid}> — **{c:,}** min" for rid, c in sorted(t.items(), key=lambda x: x[1])]
    await ctx.send(embed=discord.Embed(title="🎙 VC Role Thresholds",
                                       description="\n".join(lines), color=BOT_COLOR))

@bot.command(name="addpath")
async def prefix_addpath(ctx, trigger_role: discord.Role, promoted_role: discord.Role, count: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    paths = gdata["settings"]["conditional_paths"]
    path  = next((p for p in paths if p["trigger_role_id"]==str(trigger_role.id)), None)
    if not path:
        path = {"trigger_role_id": str(trigger_role.id), "thresholds": {}}
        paths.append(path)
    path["thresholds"][str(promoted_role.id)] = count
    save_data(data)
    await ctx.send(f"✅ Members with **{trigger_role.name}** → **{promoted_role.name}** at **{count:,}** messages.")

@bot.command(name="removepath")
async def prefix_removepath(ctx, trigger_role: discord.Role):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    paths = gdata["settings"]["conditional_paths"]
    new   = [p for p in paths if p["trigger_role_id"]!=str(trigger_role.id)]
    if len(new)==len(paths): return await ctx.send(f"❌ No path for **{trigger_role.name}**.")
    gdata["settings"]["conditional_paths"] = new
    save_data(data)
    await ctx.send(f"✅ Removed path for **{trigger_role.name}**.")

@bot.command(name="listpaths")
async def prefix_listpaths(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    paths = gdata["settings"].get("conditional_paths", [])
    if not paths: return await ctx.send("ℹ️ No conditional paths. Use `!addpath @trigger @role <count>`.")
    embed = discord.Embed(title="🔀 Conditional Role Paths", color=BOT_COLOR)
    for path in paths:
        trigger = ctx.guild.get_role(int(path["trigger_role_id"]))
        lines   = [f"  • <@&{rid}> at {c:,} msgs" for rid,c in sorted(path["thresholds"].items(),key=lambda x:x[1])]
        embed.add_field(name=f"Has role: {trigger.name if trigger else path['trigger_role_id']}",
                        value="\n".join(lines) or "No roles", inline=False)
    await ctx.send(embed=embed)

# ── Settings / anti-spam / toggles ────────────────────────────────────────────
@bot.command(name="setspam")
async def prefix_setspam(ctx, seconds: float):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    if seconds < 1: return await ctx.send("❌ Minimum 1 second.")
    gdata["settings"]["spam_interval"] = seconds
    save_data(data)
    await ctx.send(f"✅ Anti-spam: **{seconds}s** interval.")

@bot.command(name="toggleemoji")
async def prefix_toggleemoji(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    cur = gdata["settings"].get("count_emojis", True)
    gdata["settings"]["count_emojis"] = not cur
    save_data(data)
    await ctx.send(f"✅ Emoji-only messages: **{'counted' if not cur else 'ignored'}**.")

@bot.command(name="togglevc")
async def prefix_togglevc(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    cur = gdata["settings"].get("include_vc_in_roles", False)
    gdata["settings"]["include_vc_in_roles"] = not cur
    save_data(data)
    await ctx.send(f"✅ VC time in msg role thresholds: **{'yes' if not cur else 'no'}**.")

@bot.command(name="whitelist")
async def prefix_whitelist(ctx, channel: discord.TextChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    wl = gdata["settings"]["whitelisted_channels"]
    if channel.id in wl: return await ctx.send(f"ℹ️ {channel.mention} already whitelisted.")
    wl.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ {channel.mention} whitelisted.")

@bot.command(name="blacklist")
async def prefix_blacklist(ctx, channel: discord.TextChannel):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_channels"]
    if channel.id in bl: return await ctx.send(f"ℹ️ {channel.mention} already blacklisted.")
    bl.append(channel.id)
    save_data(data)
    await ctx.send(f"✅ {channel.mention} blacklisted.")

@bot.command(name="clearwhitelist")
async def prefix_clearwhitelist(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["whitelisted_channels"] = []
    save_data(data)
    await ctx.send("✅ Whitelist cleared.")

@bot.command(name="clearblacklist")
async def prefix_clearblacklist(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["settings"]["blacklisted_channels"] = []
    save_data(data)
    await ctx.send("✅ Blacklist cleared.")

@bot.command(name="listchannels")
async def prefix_listchannels(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s     = gdata["settings"]
    wl    = [f"<#{c}>" for c in s.get("whitelisted_channels",[])]
    bl    = [f"<#{c}>" for c in s.get("blacklisted_channels",[])]
    embed = discord.Embed(title="📋 Channel Settings", color=BOT_COLOR)
    embed.add_field(name="✅ Whitelisted", value="\n".join(wl) or "None (all count)", inline=False)
    embed.add_field(name="❌ Blacklisted", value="\n".join(bl) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="blacklistmember")
async def prefix_blacklistmember(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_members"]
    if member.id in bl: return await ctx.send(f"ℹ️ **{member.display_name}** already blacklisted.")
    bl.append(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** blocked from role upgrades.")

@bot.command(name="unblacklistmember")
async def prefix_unblacklistmember(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    bl = gdata["settings"]["blacklisted_members"]
    if member.id not in bl: return await ctx.send(f"ℹ️ **{member.display_name}** is not blacklisted.")
    bl.remove(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** unblacklisted.")

@bot.command(name="addmsgs")
async def prefix_addmsgs(ctx, member: discord.Member, count: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    udata = get_user_data(gdata, member.id)
    increment_counts(udata, count)
    save_data(data)
    await assign_roles(ctx.guild, member, udata, gdata)
    await ctx.send(f"✅ Added **{count:,}** messages to **{member.display_name}** (total: {udata['total']:,}).")

@bot.command(name="removemsgs")
async def prefix_removemsgs(ctx, member: discord.Member, count: int):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    udata = get_user_data(gdata, member.id)
    udata["total"] = max(0, udata.get("total",0)-count)
    save_data(data)
    await ctx.send(f"✅ Removed **{count:,}** from **{member.display_name}** (total: {udata['total']:,}).")

@bot.command(name="resetuser")
async def prefix_resetuser(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    gdata["users"].pop(str(member.id), None)
    save_data(data)
    await ctx.send(f"✅ Reset all stats for **{member.display_name}**.")

@bot.command(name="addowner")
async def prefix_addowner(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    owners = gdata["settings"].setdefault("owner_ids",[])
    if member.id in owners: return await ctx.send(f"ℹ️ Already has bot-owner permissions.")
    owners.append(member.id)
    save_data(data)
    await ctx.send(f"✅ **{member.display_name}** granted bot-owner permissions.")

@bot.command(name="removeowner")
async def prefix_removeowner(ctx, member: discord.Member):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    if not is_owner(ctx.author, ctx.guild, gdata): return await ctx.send("❌ No permission.")
    owners = gdata["settings"].get("owner_ids",[])
    if member.id not in owners: return await ctx.send(f"ℹ️ No bot-owner permissions.")
    owners.remove(member.id)
    save_data(data)
    await ctx.send(f"✅ Removed bot-owner from **{member.display_name}**.")

@bot.command(name="settings")
async def prefix_settings(ctx):
    data  = load_data()
    gdata = get_guild_data(data, ctx.guild.id)
    s = gdata["settings"]
    embed = discord.Embed(title="⚙️ Server Settings", color=BOT_COLOR)
    embed.add_field(name="Spam Interval",    value=f"{s.get('spam_interval',3)}s",                     inline=True)
    embed.add_field(name="Emoji Counting",   value=str(s.get("count_emojis",True)),                    inline=True)
    embed.add_field(name="VC in Msg Roles",  value=str(s.get("include_vc_in_roles",False)),            inline=True)
    embed.add_field(name="AutoMod",          value=str(s.get("automod_enabled",False)),                inline=True)
    embed.add_field(name="Msg Thresholds",   value=str(len(s.get("role_thresholds",{}))),              inline=True)
    embed.add_field(name="VC Thresholds",    value=str(len(s.get("vc_role_thresholds",{}))),           inline=True)
    embed.add_field(name="Cond. Paths",      value=str(len(s.get("conditional_paths",[]))),            inline=True)
    embed.add_field(name="Whitelisted Ch.",  value=str(len(s.get("whitelisted_channels",[]))),        inline=True)
    embed.add_field(name="Blacklisted Ch.",  value=str(len(s.get("blacklisted_channels",[]))),        inline=True)
    embed.add_field(name="Blacklisted Mbrs", value=str(len(s.get("blacklisted_members",[]))),         inline=True)
    embed.add_field(name="Welcome Ch.",      value=f"<#{s['welcome_channel']}>" if s.get("welcome_channel") else "Not set", inline=True)
    embed.add_field(name="Log Ch.",          value=f"<#{s['log_channel']}>"     if s.get("log_channel")     else "Not set", inline=True)
    await ctx.send(embed=embed)

# ══════════════════════════════════════════════════════════════════════════════
#  SLASH COMMANDS  (key ones — mirrors of prefix)
# ══════════════════════════════════════════════════════════════════════════════
@bot.tree.command(name="rank", description="View rank card")
@app_commands.describe(member="User to check")
async def slash_rank(interaction: discord.Interaction, member: discord.Member = None):
    member   = member or interaction.user
    data     = load_data()
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

@bot.tree.command(name="stats", description="View message & VC stats")
@app_commands.describe(member="User to check")
async def slash_stats(interaction: discord.Interaction, member: discord.Member = None):
    member = member or interaction.user
    data   = load_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    udata  = get_user_data(gdata, member.id)
    await interaction.response.send_message(embed=stats_embed(member, udata, gdata, interaction.guild))

@bot.tree.command(name="lb", description="Message leaderboard")
@app_commands.choices(period=[
    app_commands.Choice(name="All Time",   value="total"),
    app_commands.Choice(name="Today",      value="daily"),
    app_commands.Choice(name="This Week",  value="weekly"),
    app_commands.Choice(name="This Month", value="monthly"),
])
async def slash_lb(interaction: discord.Interaction, period: str = "total"):
    data   = load_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    scores = build_leaderboard(interaction.guild, gdata, period)
    await interaction.response.send_message(embed=leaderboard_embed(scores, period, interaction.guild))

@bot.tree.command(name="vclb", description="Voice time leaderboard")
@app_commands.choices(period=[
    app_commands.Choice(name="All Time",   value="total"),
    app_commands.Choice(name="Today",      value="daily"),
    app_commands.Choice(name="This Week",  value="weekly"),
    app_commands.Choice(name="This Month", value="monthly"),
])
async def slash_vclb(interaction: discord.Interaction, period: str = "total"):
    data   = load_data()
    gdata  = get_guild_data(data, interaction.guild.id)
    scores = build_leaderboard(interaction.guild, gdata, period, mode="vc")
    await interaction.response.send_message(embed=leaderboard_embed(scores, period, interaction.guild, mode="vc"))

@bot.tree.command(name="ping", description="Check bot latency")
async def slash_ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"🏓 Pong! **{round(bot.latency*1000)}ms**")

@bot.tree.command(name="help", description="Show the interactive help menu")
async def slash_help(interaction: discord.Interaction):
    banner = generate_banner()
    view   = HelpView()
    embed  = help_home_embed(interaction.guild)
    await interaction.response.defer()
    await interaction.followup.send(file=banner)
    await interaction.followup.send(embed=embed, view=view)

    # Add this function RIGHT AFTER the rank card functions (around line 400-450)
# Make sure it's NOT indented inside any other function

def generate_banner() -> discord.File:
    W, H = 950, 200
    img  = Image.new("RGBA",(W,H),(0,0,0,0))
    draw = ImageDraw.Draw(img)
    for y in range(H):
        t = y/H
        draw.line([(0,y),(W,y)], fill=(int(8+6*t),int(5+4*t),int(20+15*t),255))
    glow = Image.new("RGBA",(W,H),(0,0,0,0))
    gd   = ImageDraw.Draw(glow)
    gd.ellipse((-80,-60,350,220), fill=(80,10,200,30))
    gd.ellipse((600,-40,1050,240),fill=(180,20,100,22))
    img  = Image.alpha_composite(img,glow)
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([3,3,W-3,H-3], radius=18, fill=None, outline=(100,50,200,130), width=2)

    f_title = _get_font(90, True)
    f_sub   = _get_font(18)
    text    = "CHILL PILL"
    bbox    = draw.textbbox((0,0),text,font=f_title)
    tw      = bbox[2]-bbox[0]; th = bbox[3]-bbox[1]
    tx, ty  = (W-tw)//2, (H-th)//2 - 10

    txt_layer = Image.new("RGBA",(W,H),(0,0,0,0))
    td        = ImageDraw.Draw(txt_layer)
    td.text((tx+3,ty+5), text, font=f_title, fill=(0,0,0,120))  # shadow
    cx = tx
    for i, ch in enumerate(text):
        t   = i/max(len(text)-1,1)
        col = _lerp((140,40,255),(230,40,160),t*2) if t<0.5 else _lerp((230,40,160),(255,210,50),(t-0.5)*2)
        td.text((cx,ty), ch, font=f_title, fill=(*col,255))
        cb = td.textbbox((cx,ty),ch,font=f_title)
        cx += cb[2]-cb[0]
    img  = Image.alpha_composite(img,txt_layer)
    draw = ImageDraw.Draw(img)

    sub  = "Your server's rank & activity tracker"
    sb   = draw.textbbox((0,0),sub,font=f_sub)
    draw.text(((W-(sb[2]-sb[0]))//2, ty+th+6), sub, font=f_sub, fill=(180,160,240,180))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="chillpill_banner.png")

# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════════════════
@tasks.loop(hours=24)
async def cleanup_old_data():
    data = load_data()
    now  = datetime.utcnow()
    cd   = (now-timedelta(days=7)).strftime("%Y-%m-%d")
    cw_t = now-timedelta(weeks=8)
    cw   = f"{cw_t.year}-W{cw_t.strftime('%W')}"
    cm   = (now.replace(day=1)-timedelta(days=365)).strftime("%Y-%m")
    for gd in data.values():
        for ud in gd.get("users",{}).values():
            for key, cutoff in [("daily",cd),("weekly",cw),("monthly",cm),
                                 ("vc_daily",cd),("vc_weekly",cw),("vc_monthly",cm)]:
                ud[key] = {k:v for k,v in ud.get(key,{}).items() if k>=cutoff}
    save_data(data)
    log.info("Cleaned up old data.")

@tasks.loop(minutes=5)
async def flush_vc_sessions():
    now  = time.time()
    data = load_data()
    for gid, sessions in vc_sessions.items():
        guild = bot.get_guild(int(gid))
        if not guild: continue
        gdata = get_guild_data(data, int(gid))
        for uid, join_time in list(sessions.items()):
            minutes = (now-join_time)/60
            member  = guild.get_member(int(uid))
            if member:
                udata = get_user_data(gdata, int(uid))
                increment_vc(udata, minutes)
                sessions[uid] = now
    save_data(data)

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

# ── Run ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not TOKEN:
        log.critical("DISCORD_TOKEN not found in .env — aborting.")
    else:
        bot.run(TOKEN)

