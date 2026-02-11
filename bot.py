# bot.py
import discord
from discord.ext import commands
import asyncio
import subprocess
import json
from datetime import datetime
import shlex
import logging
import shutil
import os
from typing import Optional, List, Dict, Any
import threading
import time
import sqlite3
import random

# Load environment variables
DISCORD_TOKEN = ''
BOT_NAME = 'PVMLIX'
PREFIX = '.'
YOUR_SERVER_IP = ''
MAIN_ADMIN_ID = '1372237657207345183'
VPS_USER_ROLE_ID = ''
DEFAULT_STORAGE_POOL = 'default'
THUMBNAIL = ""
BANNER = ""

# Free VPS Plans based on invites/boosts
FREE_VPS_PLANS = {
    'invites': [
        {'name': 'Free Tier I', 'invites': 10, 'ram': 12, 'cpu': 4, 'disk': 100},
        {'name': 'Free Tier II', 'invites': 20, 'ram': 24, 'cpu': 6, 'disk': 250},
        {'name': 'Free Tier III', 'invites': 28, 'ram': 32, 'cpu': 8, 'disk': 300}
    ],
    'boosts': [
        {'name': 'Boost Reward I', 'boosts': 1, 'ram': 24, 'cpu': 6, 'disk': 250},
        {'name': 'Boost Reward II', 'boosts': 2, 'ram': 32, 'cpu': 8, 'disk': 300}
    ]
}

# OS Options for VPS Creation and Reinstall
OS_OPTIONS = [
    {"label": "Ubuntu 20.04 LTS", "value": "ubuntu:20.04"},
    {"label": "Ubuntu 22.04 LTS", "value": "ubuntu:22.04"},
    {"label": "Ubuntu 24.04 LTS", "value": "ubuntu:24.04"},
    {"label": "Debian 10 (Buster)", "value": "images:debian/10"},
    {"label": "Debian 11 (Bullseye)", "value": "images:debian/11"},
    {"label": "Debian 12 (Bookworm)", "value": "images:debian/12"},
    {"label": "Debian 13 (Trixie)", "value": "images:debian/13"},
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f'{BOT_NAME.lower()}_vps_bot')

# Check if lxc command is available
if not shutil.which("lxc"):
    logger.error("LXC command not found. Please ensure LXC is installed.")
    raise SystemExit("LXC command not found. Please ensure LXC is installed.")

# Database setup
def get_db():
    conn = sqlite3.connect('vps.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    
    # Admins table
    cur.execute('''CREATE TABLE IF NOT EXISTS admins (
        user_id TEXT PRIMARY KEY
    )''')
    cur.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (str(MAIN_ADMIN_ID),))
    
    # VPS table
    cur.execute('''CREATE TABLE IF NOT EXISTS vps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        container_name TEXT UNIQUE NOT NULL,
        ram TEXT NOT NULL,
        cpu TEXT NOT NULL,
        storage TEXT NOT NULL,
        config TEXT NOT NULL,
        os_version TEXT DEFAULT 'ubuntu:22.04',
        status TEXT DEFAULT 'stopped',
        suspended INTEGER DEFAULT 0,
        whitelisted INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        shared_with TEXT DEFAULT '[]',
        suspension_history TEXT DEFAULT '[]'
    )''')
    
    # User stats for free VPS
    cur.execute('''CREATE TABLE IF NOT EXISTS user_stats (
        user_id TEXT PRIMARY KEY,
        invites INTEGER DEFAULT 0,
        boosts INTEGER DEFAULT 0,
        claimed_free_vps INTEGER DEFAULT 0,
        last_updated TEXT
    )''')
    
    # Settings table
    cur.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )''')
    
    # Port allocations table
    cur.execute('''CREATE TABLE IF NOT EXISTS port_allocations (
        user_id TEXT PRIMARY KEY,
        allocated_ports INTEGER DEFAULT 0
    )''')
    
    # Port forwards table
    cur.execute('''CREATE TABLE IF NOT EXISTS port_forwards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        vps_container TEXT NOT NULL,
        vps_port INTEGER NOT NULL,
        host_port INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )''')
    
    # Initialize settings
    settings_init = [
        ('cpu_threshold', '90'),
        ('ram_threshold', '90'),
    ]
    for key, value in settings_init:
        cur.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, value))
    
    conn.commit()
    conn.close()

def get_setting(key: str, default: Any = None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default

def set_setting(key: str, value: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

def get_vps_data() -> Dict[str, List[Dict[str, Any]]]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM vps')
    rows = cur.fetchall()
    conn.close()
    data = {}
    for row in rows:
        user_id = row['user_id']
        if user_id not in data:
            data[user_id] = []
        vps = dict(row)
        vps['shared_with'] = json.loads(vps['shared_with'])
        vps['suspension_history'] = json.loads(vps['suspension_history'])
        vps['suspended'] = bool(vps['suspended'])
        vps['whitelisted'] = bool(vps['whitelisted'])
        vps['os_version'] = vps.get('os_version', 'ubuntu:22.04')
        data[user_id].append(vps)
    return data

def get_admins() -> List[str]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT user_id FROM admins')
    rows = cur.fetchall()
    conn.close()
    return [row['user_id'] for row in rows]

def save_vps_data():
    conn = get_db()
    cur = conn.cursor()
    for user_id, vps_list in vps_data.items():
        for vps in vps_list:
            shared_json = json.dumps(vps['shared_with'])
            history_json = json.dumps(vps['suspension_history'])
            suspended_int = 1 if vps['suspended'] else 0
            whitelisted_int = 1 if vps.get('whitelisted', False) else 0
            os_ver = vps.get('os_version', 'ubuntu:22.04')
            created_at = vps.get('created_at', datetime.now().isoformat())
            
            if 'id' not in vps or vps['id'] is None:
                cur.execute('''INSERT INTO vps (user_id, container_name, ram, cpu, storage, config, os_version, status, suspended, whitelisted, created_at, shared_with, suspension_history)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (user_id, vps['container_name'], vps['ram'], vps['cpu'], vps['storage'], vps['config'],
                             os_ver, vps['status'], suspended_int, whitelisted_int,
                             created_at, shared_json, history_json))
                vps['id'] = cur.lastrowid
            else:
                cur.execute('''UPDATE vps SET user_id = ?, ram = ?, cpu = ?, storage = ?, config = ?, os_version = ?, status = ?, suspended = ?, whitelisted = ?, shared_with = ?, suspension_history = ?
                               WHERE id = ?''',
                            (user_id, vps['ram'], vps['cpu'], vps['storage'], vps['config'],
                             os_ver, vps['status'], suspended_int, whitelisted_int, shared_json, history_json, vps['id']))
    conn.commit()
    conn.close()

def save_admin_data():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM admins')
    for admin_id in admin_data['admins']:
        cur.execute('INSERT INTO admins (user_id) VALUES (?)', (admin_id,))
    conn.commit()
    conn.close()

# User stats functions
def get_user_stats(user_id: str) -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return dict(row)
    return {'user_id': user_id, 'invites': 0, 'boosts': 0, 'claimed_free_vps': 0, 'last_updated': None}

def update_user_stats(user_id: str, invites: int = 0, boosts: int = 0, claimed_free_vps: int = 0):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''INSERT OR REPLACE INTO user_stats 
                   (user_id, invites, boosts, claimed_free_vps, last_updated) 
                   VALUES (?, COALESCE((SELECT invites FROM user_stats WHERE user_id = ?), 0) + ?, 
                           COALESCE((SELECT boosts FROM user_stats WHERE user_id = ?), 0) + ?,
                           COALESCE((SELECT claimed_free_vps FROM user_stats WHERE user_id = ?), 0) + ?,
                           ?)''',
                (user_id, user_id, invites, user_id, boosts, user_id, claimed_free_vps, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# Port forwarding functions
def get_user_allocation(user_id: str) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT allocated_ports FROM port_allocations WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0

def get_user_used_ports(user_id: str) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM port_forwards WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0]

def allocate_ports(user_id: str, amount: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('INSERT OR REPLACE INTO port_allocations (user_id, allocated_ports) VALUES (?, COALESCE((SELECT allocated_ports FROM port_allocations WHERE user_id = ?), 0) + ?)', (user_id, user_id, amount))
    conn.commit()
    conn.close()

def deallocate_ports(user_id: str, amount: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE port_allocations SET allocated_ports = GREATEST(0, allocated_ports - ?) WHERE user_id = ?', (amount, user_id))
    conn.commit()
    conn.close()

def get_available_host_port() -> Optional[int]:
    used_ports = set()
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT host_port FROM port_forwards')
    for row in cur.fetchall():
        used_ports.add(row[0])
    conn.close()
    for _ in range(100):
        port = random.randint(20000, 50000)
        if port not in used_ports:
            return port
    return None

async def create_port_forward(user_id: str, container: str, vps_port: int) -> Optional[int]:
    host_port = get_available_host_port()
    if not host_port:
        return None
    try:
        await execute_lxc(f"lxc config device add {container} tcp_proxy_{host_port} proxy listen=tcp:0.0.0.0:{host_port} connect=tcp:127.0.0.1:{vps_port}")
        await execute_lxc(f"lxc config device add {container} udp_proxy_{host_port} proxy listen=udp:0.0.0.0:{host_port} connect=udp:127.0.0.1:{vps_port}")
        conn = get_db()
        cur = conn.cursor()
        cur.execute('INSERT INTO port_forwards (user_id, vps_container, vps_port, host_port, created_at) VALUES (?, ?, ?, ?, ?)',
                    (user_id, container, vps_port, host_port, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        return host_port
    except Exception as e:
        logger.error(f"Failed to create port forward: {e}")
        return None

async def remove_port_forward(forward_id: int, is_admin: bool = False) -> tuple[bool, Optional[str]]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT user_id, vps_container, host_port FROM port_forwards WHERE id = ?', (forward_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, None
    user_id, container, host_port = row
    try:
        await execute_lxc(f"lxc config device remove {container} tcp_proxy_{host_port}")
        await execute_lxc(f"lxc config device remove {container} udp_proxy_{host_port}")
        cur.execute('DELETE FROM port_forwards WHERE id = ?', (forward_id,))
        conn.commit()
        conn.close()
        return True, user_id
    except Exception as e:
        logger.error(f"Failed to remove port forward {forward_id}: {e}")
        conn.close()
        return False, None

def get_user_forwards(user_id: str) -> List[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM port_forwards WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

# Initialize database
init_db()

# Load data at startup
vps_data = get_vps_data()
admin_data = {'admins': get_admins()}

# Global settings from DB
CPU_THRESHOLD = int(get_setting('cpu_threshold', 90))
RAM_THRESHOLD = int(get_setting('ram_threshold', 90))

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# Resource monitoring settings
resource_monitor_active = True

# Helper function to truncate text
def truncate_text(text, max_length=1024):
    if not text:
        return text
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

# Embed creation functions with THUMBNAIL and BANNER integration
def create_embed(title, description="", color=0x1a1a1a):
    embed = discord.Embed(
        title=f"☁️ {title}",
        description=truncate_text(description, 4096),
        color=color
    )
    if THUMBNAIL:
        embed.set_thumbnail(url=THUMBNAIL)
    if BANNER:
        embed.set_image(url=BANNER)
    embed.set_footer(text=f"{BOT_NAME} • Cloud Services • Created by Wanny_Dragon • 6/01/2026")
    return embed

def add_field(embed, name, value, inline=False):
    embed.add_field(
        name=f"⌯⌲ {name}",
        value=truncate_text(value, 1024),
        inline=inline
    )
    return embed

def create_success_embed(title, description=""):
    return create_embed(title, description, color=0x00ff88)

def create_error_embed(title, description=""):
    embed = discord.Embed(
        title=f"☁️ {title}",
        description=f"───────────────\n{description}\n───────────────",
        color=0xff3366
    )
    if THUMBNAIL:
        embed.set_thumbnail(url=THUMBNAIL)
    embed.set_footer(text=f"{BOT_NAME} • Cloud Services • Created by Wanny_Dragon • 6/01/2026")
    return embed

def create_info_embed(title, description=""):
    return create_embed(title, description, color=0x00ccff)

def create_warning_embed(title, description=""):
    return create_embed(title, description, color=0xffaa00)

# Admin checks
def is_admin():
    async def predicate(ctx):
        user_id = str(ctx.author.id)
        if user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", []):
            return True
        raise commands.CheckFailure("You need admin permissions to use this command. Contact support.")
    return commands.check(predicate)

def is_main_admin():
    async def predicate(ctx):
        if str(ctx.author.id) == str(MAIN_ADMIN_ID):
            return True
        raise commands.CheckFailure("Only the main admin can use this command.")
    return commands.check(predicate)

# Clean LXC command execution
async def execute_lxc(command, timeout=120):
    try:
        cmd = shlex.split(command)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise asyncio.TimeoutError(f"Command timed out after {timeout} seconds")
        if proc.returncode != 0:
            error = stderr.decode().strip() if stderr else "Command failed with no error output"
            raise Exception(error)
        return stdout.decode().strip() if stdout else True
    except asyncio.TimeoutError as te:
        logger.error(f"LXC command timed out: {command} - {str(te)}")
        raise
    except Exception as e:
        logger.error(f"LXC Error: {command} - {str(e)}")
        raise

# Function to apply LXC config
async def apply_lxc_config(container_name):
    try:
        await execute_lxc(f"lxc config set {container_name} security.nesting true")
        await execute_lxc(f"lxc config set {container_name} security.privileged true")
        await execute_lxc(f"lxc config set {container_name} security.syscalls.intercept.mknod true")
        await execute_lxc(f"lxc config set {container_name} security.syscalls.intercept.setxattr true")
        
        try:
            await execute_lxc(f"lxc config device add {container_name} fuse unix-char path=/dev/fuse")
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
        
        await execute_lxc(f"lxc config set {container_name} linux.kernel_modules overlay,loop,nf_nat,ip_tables,ip6_tables,netlink_diag,br_netfilter")
        
        raw_lxc_config = """
lxc.apparmor.profile = unconfined
lxc.cgroup.devices.allow = a
lxc.cap.drop =
lxc.mount.auto = proc:rw sys:rw cgroup:rw
"""
        await execute_lxc(f"lxc config set {container_name} raw.lxc '{raw_lxc_config}'")
        
        logger.info(f"Applied LXC config to {container_name}")
    except Exception as e:
        logger.error(f"Failed to apply LXC config to {container_name}: {e}")

async def apply_internal_permissions(container_name):
    try:
        await asyncio.sleep(5)
        
        commands = [
            "mkdir -p /etc/sysctl.d/",
            "echo 'net.ipv4.ip_unprivileged_port_start=0' > /etc/sysctl.d/99-custom.conf",
            "echo 'net.ipv4.ping_group_range=0 2147483647' >> /etc/sysctl.d/99-custom.conf",
            "echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.d/99-custom.conf",
            "sysctl -p /etc/sysctl.d/99-custom.conf || true"
        ]
        
        for cmd in commands:
            try:
                await execute_lxc(f"lxc exec {container_name} -- bash -c \"{cmd}\"")
            except Exception:
                continue
        
        logger.info(f"Applied internal permissions to {container_name}")
    except Exception as e:
        logger.error(f"Failed to apply internal permissions to {container_name}: {e}")

# Get or create VPS user role
async def get_or_create_vps_role(guild):
    global VPS_USER_ROLE_ID
    if VPS_USER_ROLE_ID:
        role = guild.get_role(VPS_USER_ROLE_ID)
        if role:
            return role
    role = discord.utils.get(guild.roles, name=f"{BOT_NAME} VPS User")
    if role:
        VPS_USER_ROLE_ID = role.id
        return role
    try:
        role = await guild.create_role(
            name=f"{BOT_NAME} VPS User",
            color=discord.Color.dark_purple(),
            reason=f"{BOT_NAME} VPS User role for bot management",
            permissions=discord.Permissions.none()
        )
        VPS_USER_ROLE_ID = role.id
        logger.info(f"Created {BOT_NAME} VPS User role: {role.name} (ID: {role.id})")
        return role
    except Exception as e:
        logger.error(f"Failed to create {BOT_NAME} VPS User role: {e}")
        return None

# Helper functions for container stats
async def get_container_status(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "info", container_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        output = stdout.decode()
        for line in output.splitlines():
            if line.startswith("Status: "):
                return line.split(": ", 1)[1].strip().lower()
        return "unknown"
    except Exception:
        return "unknown"

async def get_container_cpu(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "top", "-bn1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        output = stdout.decode()
        for line in output.splitlines():
            if '%Cpu(s):' in line:
                parts = line.split()
                us = float(parts[1])
                sy = float(parts[3])
                ni = float(parts[5])
                id_ = float(parts[7])
                wa = float(parts[9])
                hi = float(parts[11])
                si = float(parts[13])
                st = float(parts[15])
                usage = us + sy + ni + wa + hi + si + st
                return f"{usage:.1f}%"
        return "0.0%"
    except Exception:
        return "N/A"

async def get_container_memory(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "free", "-m",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = stdout.decode().splitlines()
        if len(lines) > 1:
            parts = lines[1].split()
            total = int(parts[1])
            used = int(parts[2])
            usage_pct = (used / total * 100) if total > 0 else 0
            return f"{used}/{total} MB ({usage_pct:.1f}%)"
        return "Unknown"
    except Exception:
        return "N/A"

async def get_container_disk(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "df", "-h", "/",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = stdout.decode().splitlines()
        for line in lines:
            if '/dev/' in line and ' /' in line:
                parts = line.split()
                if len(parts) >= 5:
                    used = parts[2]
                    size = parts[1]
                    perc = parts[4]
                    return f"{used}/{size} ({perc})"
        return "Unknown"
    except Exception:
        return "N/A"

async def get_container_uptime(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "uptime",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        return stdout.decode().strip() if stdout else "Unknown"
    except Exception:
        return "N/A"

def get_uptime():
    try:
        result = subprocess.run(['uptime'], capture_output=True, text=True)
        return result.stdout.strip()
    except Exception:
        return "Unknown"

# Bot events
@bot.event
async def on_ready():
    logger.info(f'{bot.user} has connected to Discord!')
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=f"{BOT_NAME} VPS Manager"))
    logger.info(f"{BOT_NAME} Bot is ready! Created by Wanny_Dragon • 6/01/2026")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=create_error_embed("Missing Argument", "Please check command usage with `.help`."))
    elif isinstance(error, commands.BadArgument):
        await ctx.send(embed=create_error_embed("Invalid Argument", "Please check your input and try again."))
    elif isinstance(error, commands.CheckFailure):
        error_msg = str(error) if str(error) else "You need admin permissions for this command. Contact support."
        await ctx.send(embed=create_error_embed("Access Denied", error_msg))
    else:
        logger.error(f"Command error: {error}")
        await ctx.send(embed=create_error_embed("System Error", "An unexpected error occurred. Support has been notified."))

# ============ BOT COMMANDS ============

@bot.command(name='about')
async def about_command(ctx):
    """Show information about the bot"""
    embed = create_embed(f"🤖 About {BOT_NAME}", f"**{BOT_NAME} VPS Manager**\n\nA powerful Discord bot for managing LXC containers", 0x5865F2)
    
    # Bot Information
    bot_info = f"**Creator:** Wanny_Dragon\n"
    bot_info += f"**Created:** 6/01/2026\n"
    bot_info += f"**Prefix:** `{PREFIX}`\n"
    bot_info += f"**Server IP:** `{YOUR_SERVER_IP if YOUR_SERVER_IP else 'Not Set'}`\n"
    bot_info += f"**Total Users:** {len(vps_data)}\n"
    bot_info += f"**Total VPS:** {sum(len(v) for v in vps_data.values())}"
    
    add_field(embed, "📊 Bot Information", bot_info, False)
    
    # Features
    features = "✅ **Free VPS System** - Earn VPS through invites/boosts\n"
    features += "✅ **Port Forwarding** - Full TCP/UDP port management\n"
    features += "✅ **VPS Management** - Start/Stop/Restart/Reinstall\n"
    features += "✅ **Resource Monitoring** - Auto-suspension for high usage\n"
    features += "✅ **VPS Sharing** - Share access with other users\n"
    features += "✅ **Admin Panel** - Full control for administrators\n"
    features += "✅ **Multi-OS Support** - Ubuntu/Debian distributions\n"
    features += "✅ **Live Statistics** - Real-time resource monitoring"
    
    add_field(embed, "🌟 Features", features, False)
    
    # Watermark with creator info
    embed.set_footer(text=f"🚀 Created by Wanny_Dragon • 6/01/2026")
    
    await ctx.send(embed=embed)

@bot.command(name='userperms')
@is_admin()
async def user_perms(ctx, user: discord.Member = None):
    """Show detailed user permissions and information"""
    target_user = user or ctx.author
    user_id = str(target_user.id)
    
    embed = create_embed(f"👤 User Permissions: {target_user.name}", f"ID: {user_id}")
    
    # Basic Info
    basic_info = f"**User:** {target_user.mention}\n"
    basic_info += f"**Username:** {target_user.name}#{target_user.discriminator}\n"
    basic_info += f"**Account Created:** {target_user.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
    basic_info += f"**Joined Server:** {target_user.joined_at.strftime('%Y-%m-%d %H:%M:%S') if target_user.joined_at else 'N/A'}"
    
    add_field(embed, "📝 Basic Information", basic_info, False)
    
    # Permissions
    is_main_admin_user = user_id == str(MAIN_ADMIN_ID)
    is_admin_user = is_main_admin_user or user_id in admin_data.get("admins", [])
    
    permissions_text = ""
    if is_main_admin_user:
        permissions_text += "👑 **Main Administrator** - Full system access\n"
        permissions_text += "• Can create/delete VPS\n"
        permissions_text += "• Can manage all users\n"
        permissions_text += "• Can add/remove admins\n"
        permissions_text += "• Full system control\n"
    elif is_admin_user:
        permissions_text += "🛡️ **Administrator** - Extended access\n"
        permissions_text += "• Can create/delete VPS\n"
        permissions_text += "• Can manage other users' VPS\n"
        permissions_text += "• Can add invites/boosts\n"
        permissions_text += "• Can manage port allocations\n"
    else:
        permissions_text += "👤 **Regular User** - Basic access\n"
        permissions_text += "• Can manage own VPS\n"
        permissions_text += "• Can share VPS with others\n"
        permissions_text += "• Can use port forwarding\n"
        permissions_text += "• Can claim free VPS\n"
    
    # Check VPS User role
    if ctx.guild:
        vps_role = await get_or_create_vps_role(ctx.guild)
        has_vps_role = vps_role in target_user.roles if vps_role else False
        permissions_text += f"\n**VPS User Role:** {'✅ Yes' if has_vps_role else '❌ No'}"
    
    add_field(embed, "🔐 Permissions", permissions_text, False)
    
    # VPS Information
    vps_list = vps_data.get(user_id, [])
    vps_count = len(vps_list)
    
    vps_info = f"**Total VPS:** {vps_count}\n"
    if vps_count > 0:
        running = sum(1 for v in vps_list if v.get('status') == 'running')
        suspended = sum(1 for v in vps_list if v.get('suspended', False))
        whitelisted = sum(1 for v in vps_list if v.get('whitelisted', False))
        
        vps_info += f"**Running:** {running}\n"
        vps_info += f"**Suspended:** {suspended}\n"
        vps_info += f"**Whitelisted:** {whitelisted}\n"
        
        # Calculate total resources
        total_ram = sum(int(v['ram'].replace('GB', '')) for v in vps_list)
        total_cpu = sum(int(v['cpu']) for v in vps_list)
        total_storage = sum(int(v['storage'].replace('GB', '')) for v in vps_list)
        
        vps_info += f"\n**Total Resources:**\n"
        vps_info += f"• RAM: {total_ram}GB\n"
        vps_info += f"• CPU: {total_cpu} cores\n"
        vps_info += f"• Storage: {total_storage}GB"
    
    add_field(embed, "🖥️ VPS Overview", vps_info, False)
    
    # Stats
    stats = get_user_stats(user_id)
    stats_text = f"**Invites:** {stats['invites']}\n"
    stats_text += f"**Boosts:** {stats['boosts']}\n"
    stats_text += f"**Claimed Free VPS:** {stats['claimed_free_vps']}\n"
    
    # Port allocation
    port_alloc = get_user_allocation(user_id)
    port_used = get_user_used_ports(user_id)
    stats_text += f"\n**Port Forwarding:**\n"
    stats_text += f"• Allocated: {port_alloc} slots\n"
    stats_text += f"• Used: {port_used} slots\n"
    stats_text += f"• Available: {port_alloc - port_used} slots"
    
    add_field(embed, "📊 Statistics", stats_text, False)
    
    # Last VPS if any
    if vps_count > 0:
        latest_vps = max(vps_list, key=lambda x: x.get('created_at', ''))
        created = datetime.fromisoformat(latest_vps['created_at']).strftime('%Y-%m-%d %H:%M') if latest_vps.get('created_at') else 'Unknown'
        add_field(embed, "🆕 Latest VPS", f"**Name:** `{latest_vps['container_name']}`\n**Created:** {created}\n**Config:** {latest_vps.get('config', 'Custom')}", False)
    
    await ctx.send(embed=embed)

# ============ HELPER FUNCTIONS FOR HELP SYSTEM ============

def get_user_permission_level(user_id: str) -> int:
    """Get user permission level: 0=user, 1=admin, 2=main_admin"""
    if user_id == str(MAIN_ADMIN_ID):
        return 2
    elif user_id in admin_data.get("admins", []):
        return 1
    return 0

class HelpView(discord.ui.View):
    def __init__(self, ctx, initial_category="user"):
        super().__init__(timeout=300)
        self.ctx = ctx
        self.current_category = initial_category
        self.permission_level = get_user_permission_level(str(ctx.author.id))
        
        # Define command categories
        self.categories = {
            "user": {
                "name": "👤 User Commands",
                "description": "Basic commands for all users",
                "emoji": "👤",
                "permission": 0
            },
            "vps": {
                "name": "🖥️ VPS Management",
                "description": "Manage your VPS containers",
                "emoji": "🖥️",
                "permission": 0
            },
            "ports": {
                "name": "🔌 Port Forwarding",
                "description": "Manage port forwards",
                "emoji": "🔌",
                "permission": 0
            },
            "free": {
                "name": "🎁 Free VPS System",
                "description": "Earn and claim free VPS",
                "emoji": "🎁",
                "permission": 0
            },
            "admin": {
                "name": "🛡️ Admin Commands",
                "description": "Administrator commands",
                "emoji": "🛡️",
                "permission": 1
            },
            "main_admin": {
                "name": "👑 Main Admin",
                "description": "Main administrator commands",
                "emoji": "👑",
                "permission": 2
            },
            "info": {
                "name": "📊 Information",
                "description": "Bot and system information",
                "emoji": "📊",
                "permission": 0
            }
        }
        
        self.update_select()
        self.update_embed()
    
    def update_select(self):
        """Update dropdown with categories user has access to"""
        self.clear_items()
        
        options = []
        for cat_id, cat_info in self.categories.items():
            if self.permission_level >= cat_info["permission"]:
                options.append(discord.SelectOption(
                    label=cat_info["name"],
                    value=cat_id,
                    emoji=cat_info["emoji"],
                    description=cat_info["description"][:50]
                ))
        
        if options:
            self.select = discord.ui.Select(
                placeholder="Select a category...",
                options=options,
                custom_id="help_category"
            )
            self.select.callback = self.category_callback
            self.add_item(self.select)
    
    async def category_callback(self, interaction: discord.Interaction):
        """Handle category selection"""
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message("This help menu is not for you!", ephemeral=True)
            return
        
        self.current_category = interaction.data["values"][0]
        await interaction.response.defer()
        self.update_embed()
        await interaction.edit_original_response(embed=self.embed, view=self)
    
    def update_embed(self):
        """Update embed with current category commands"""
        category = self.categories[self.current_category]
        
        embed = create_embed(
            f"📚 {BOT_NAME} Help - {category['name']}",
            category["description"],
            0x5865F2
        )
        
        # Add commands based on category
        commands_text = self.get_category_commands(self.current_category)
        add_field(embed, "📋 Commands", commands_text, False)
        
        # Add note about permissions if needed
        if category["permission"] > 0:
            perm_text = "🛡️ **Admin Command** - Requires administrator permissions\n"
            if category["permission"] == 2:
                perm_text = "👑 **Main Admin Command** - Requires main administrator permissions"
            add_field(embed, "⚠️ Permissions", perm_text, False)
        
        # Add navigation help
        nav_text = f"• Use `{PREFIX}help` to show this menu\n"
        nav_text += f"• Use dropdown above to switch categories\n"
        nav_text += f"• Total commands: {self.count_total_commands()}\n"
        nav_text += f"• Prefix: `{PREFIX}`"
        
        add_field(embed, "🔍 Navigation", nav_text, False)
        
        # Add watermark with creator info
        embed.set_footer(text=f"{BOT_NAME} • Created by Wanny_Dragon • 6/01/2026")
        
        self.embed = embed
    
    def get_category_commands(self, category_id: str) -> str:
        """Get formatted commands for a category"""
        commands = {
            "user": [
                (f"{PREFIX}ping", "Check bot latency"),
                (f"{PREFIX}uptime", "Show host uptime"),
                (f"{PREFIX}myvps", "List your VPS briefly"),
                (f"{PREFIX}list", "Detailed VPS information"),
                (f"{PREFIX}manage", "Manage your VPS"),
                (f"{PREFIX}manage @user", "Manage another user's VPS (Admin only)"),
                (f"{PREFIX}share-user @user <vps>", "Share VPS access"),
                (f"{PREFIX}share-ruser @user <vps>", "Revoke VPS access"),
                (f"{PREFIX}manage-shared @owner <vps>", "Manage shared VPS")
            ],
            "vps": [
                (f"{PREFIX}myvps", "List your VPS"),
                (f"{PREFIX}list", "Detailed VPS info"),
                (f"{PREFIX}vpsinfo [container]", "Get VPS details"),
                (f"{PREFIX}manage", "Manage VPS (Start/Stop/SSH/Reinstall)"),
                (f"{PREFIX}restart-vps <container>", "Restart VPS (Admin)"),
                (f"{PREFIX}stop-vps-all", "Stop all VPS (Admin)")
            ],
            "ports": [
                (f"{PREFIX}ports add <vps> <port>", "Add port forward"),
                (f"{PREFIX}ports list", "List your port forwards"),
                (f"{PREFIX}ports remove <id>", "Remove port forward"),
                (f"{PREFIX}ports-add-user <amount> @user", "Allocate ports (Admin)"),
                (f"{PREFIX}ports-remove-user <amount> @user", "Deallocate ports (Admin)"),
                (f"{PREFIX}ports-revoke <id>", "Revoke port forward (Admin)")
            ],
            "free": [
                (f"{PREFIX}plans", "View free VPS plans"),
                (f"{PREFIX}claimfree", "Claim a free VPS"),
                (f"{PREFIX}invadd @user <amount>", "Add invites (Admin)"),
                (f"{PREFIX}boostadd @user <amount>", "Add boosts (Admin)")
            ],
            "admin": [
                (f"{PREFIX}create <ram> <cpu> <disk> @user", "Create VPS for user"),
                (f"{PREFIX}delete-vps @user <vps> [reason]", "Delete user's VPS"),
                (f"{PREFIX}userinfo @user", "Get user information"),
                (f"{PREFIX}userperms @user", "Detailed user permissions"),
                (f"{PREFIX}serverstats", "Server statistics"),
                (f"{PREFIX}list-all", "List all VPS on server"),
                (f"{PREFIX}add-resources <container> [ram] [cpu] [disk]", "Add resources to VPS"),
                (f"{PREFIX}invadd @user <amount>", "Add invites to user"),
                (f"{PREFIX}boostadd @user <amount>", "Add boosts to user")
            ],
            "main_admin": [
                (f"{PREFIX}admin-add @user", "Grant admin privileges"),
                (f"{PREFIX}admin-remove @user", "Revoke admin privileges"),
                (f"{PREFIX}admin-list", "List all admins"),
                (f"{PREFIX}set-threshold <cpu> <ram>", "Set resource thresholds"),
                (f"{PREFIX}set-status <type> <name>", "Set bot status")
            ],
            "info": [
                (f"{PREFIX}about", "Bot information and credits"),
                (f"{PREFIX}help", "Show this help menu"),
                (f"{PREFIX}ping", "Check bot latency"),
                (f"{PREFIX}uptime", "Show host uptime"),
                (f"{PREFIX}serverstats", "Server statistics (Admin)"),
                (f"{PREFIX}userperms @user", "User permissions (Admin)")
            ]
        }
        
        if category_id in commands:
            cmd_list = commands[category_id]
            return "\n".join([f"**`{cmd}`** - {desc}" for cmd, desc in cmd_list])
        return "No commands available for this category."
    
    def count_total_commands(self) -> int:
        """Count total commands user has access to"""
        total = 0
        for cat_id, cat_info in self.categories.items():
            if self.permission_level >= cat_info["permission"]:
                # Count commands in this category
                if cat_id == "user":
                    total += 9
                elif cat_id == "vps":
                    total += 7
                elif cat_id == "ports":
                    total += 6
                elif cat_id == "free":
                    total += 4
                elif cat_id == "admin":
                    total += 9
                elif cat_id == "main_admin":
                    total += 5
                elif cat_id == "info":
                    total += 6
        return total

@bot.command(name='help')
async def help_command(ctx, category: str = None):
    """Show interactive help menu with dropdown"""
    if category and category.lower() in ["user", "vps", "ports", "free", "admin", "main_admin", "info"]:
        view = HelpView(ctx, category.lower())
    else:
        view = HelpView(ctx)
    
    await ctx.send(embed=view.embed, view=view)

# ============ BASIC COMMANDS ============

@bot.command(name='ping')
async def ping(ctx):
    latency = round(bot.latency * 1000)
    embed = create_success_embed("Pong!", f"{BOT_NAME} Bot latency: {latency}ms")
    await ctx.send(embed=embed)

@bot.command(name='uptime')
async def uptime(ctx):
    up = get_uptime()
    embed = create_info_embed("Host Uptime", up)
    await ctx.send(embed=embed)

@bot.command(name='myvps')
async def my_vps(ctx):
    user_id = str(ctx.author.id)
    vps_list = vps_data.get(user_id, [])
    if not vps_list:
        embed = create_error_embed("No VPS Found", f"You don't have any {BOT_NAME} VPS. Use `.plans` to see available free plans.")
        add_field(embed, "Quick Actions", f"• `{PREFIX}manage` - Manage VPS\n• `{PREFIX}plans` - View free plans", False)
        await ctx.send(embed=embed)
        return
    
    embed = create_info_embed("My VPS", f"You have {len(vps_list)} VPS")
    
    for i, vps in enumerate(vps_list, 1):
        status = vps.get('status', 'unknown').upper()
        if vps.get('suspended', False):
            status += " (SUSPENDED)"
        if vps.get('whitelisted', False):
            status += " (WHITELISTED)"
        
        status_emoji = "🟢" if vps.get('status') == 'running' else "🔴" if vps.get('status') == 'stopped' else "🟡"
        
        vps_info = f"{status_emoji} **VPS #{i}:** `{vps['container_name']}`\n"
        vps_info += f"• **Status:** {status}\n"
        vps_info += f"• **Resources:** {vps.get('config', 'Custom')}\n"
        vps_info += f"• **Created:** {vps.get('created_at', 'Unknown')[:10]}\n"
        
        add_field(embed, f"", vps_info, False)
    
    add_field(embed, "🔧 Management", f"Use `{PREFIX}manage` to control your VPS\nUse `{PREFIX}list` for detailed information", False)
    await ctx.send(embed=embed)

@bot.command(name='list')
async def list_user_vps(ctx):
    """Show detailed information about user's VPS"""
    user_id = str(ctx.author.id)
    vps_list = vps_data.get(user_id, [])
    
    if not vps_list:
        embed = create_error_embed("No VPS Found", f"You don't have any {BOT_NAME} VPS. Use `.plans` to see available free plans.")
        await ctx.send(embed=embed)
        return
    
    embed = create_info_embed("📋 Your VPS List", f"Showing {len(vps_list)} VPS for {ctx.author.mention}")
    
    for i, vps in enumerate(vps_list, 1):
        container_name = vps['container_name']
        
        # Get live stats
        status = await get_container_status(container_name)
        cpu_usage = await get_container_cpu(container_name)
        memory_usage = await get_container_memory(container_name)
        disk_usage = await get_container_disk(container_name)
        uptime_info = await get_container_uptime(container_name)
        
        # Status emoji
        status_emoji = "🟢" if status == 'running' else "🔴" if status == 'stopped' else "🟡"
        suspended_text = " (SUSPENDED)" if vps.get('suspended', False) else ""
        whitelisted_text = " (WHITELISTED)" if vps.get('whitelisted', False) else ""
        
        vps_info = f"**#{i} | {status_emoji} {status.upper()}{suspended_text}{whitelisted_text}**\n"
        vps_info += f"**Container:** `{container_name}`\n"
        vps_info += f"**Resources:** {vps['ram']} RAM | {vps['cpu']} CPU | {vps['storage']} Storage\n"
        vps_info += f"**OS:** {vps.get('os_version', 'ubuntu:22.04')}\n"
        vps_info += f"**Uptime:** {uptime_info}\n"
        vps_info += f"**CPU Usage:** {cpu_usage}\n"
        vps_info += f"**Memory:** {memory_usage}\n"
        vps_info += f"**Disk:** {disk_usage}\n"
        vps_info += f"**Created:** {vps.get('created_at', 'Unknown')}\n"
        
        if vps.get('shared_with'):
            shared_count = len(vps['shared_with'])
            vps_info += f"**Shared with:** {shared_count} user(s)\n"
        
        add_field(embed, f"VPS #{i}", vps_info, False)
    
    add_field(embed, "📊 Quick Actions", f"• Use `{PREFIX}manage` to manage your VPS\n• Use `{PREFIX}myvps` for brief overview\n• Contact admin for support", False)
    
    await ctx.send(embed=embed)

@bot.command(name='plans')
async def show_plans(ctx):
    """Show available free VPS plans"""
    embed = create_embed("☁️ Free VPS Plans", "Earn FREE VPS plans by invites or boosts", 0x00ccff)
    
    add_field(embed, "⌯⌲ Free Tier I — 10 Invites", 
              f"**RAM:** 12 GB\n**CPU:** 4 Cores\n**Storage:** 100 GB\n**Network:** Private IPv4", False)
    add_field(embed, "───────────────", "Requirement: 10 Server Invites", False)
    
    add_field(embed, "⌯⌲ Free Tier II — 20 Invites",
              f"**RAM:** 24 GB\n**CPU:** 6 Cores\n**Storage:** 250 GB\n**Network:** Private IPv4", False)
    add_field(embed, "───────────────", "Requirement: 20 Server Invites", False)
    
    add_field(embed, "⌯⌲ Free Tier III — 28 Invites (MAX)",
              f"**RAM:** 32 GB\n**CPU:** 8 Cores\n**Storage:** 300 GB\n**Network:** Private IPv4", False)
    add_field(embed, "───────────────", "Requirement: 28 Server Invites", False)
    
    add_field(embed, "⌯⌲ Boost Reward — 1 Boost",
              f"**RAM:** 24 GB\n**CPU:** 6 Cores\n**Storage:** 250 GB\n**Network:** Private IPv4", False)
    add_field(embed, "───────────────", "Requirement: 1 Server Boost", False)
    
    add_field(embed, "⌯⌲ Boost Reward — 2 Boosts (MAX)",
              f"**RAM:** 32 GB\n**CPU:** 8 Cores\n**Storage:** 300 GB\n**Network:** Private IPv4", False)
    add_field(embed, "───────────────", "Requirement: 2 Server Boosts", False)
    
    add_field(embed, "───────────────", f"⌯⌲ Use `{PREFIX}claimfree` to claim your Free VPS Plan", False)
    add_field(embed, "───────────────", f"Earn credits by inviting users or boosting the server", False)
    
    await ctx.send(embed=embed)

@bot.command(name='claimfree')
async def claim_free_vps(ctx):
    """Claim a free VPS based on invites/boosts"""
    user_id = str(ctx.author.id)
    stats = get_user_stats(user_id)
    
    # Check which plans user qualifies for
    available_plans = []
    
    # Check invite-based plans
    for plan in FREE_VPS_PLANS['invites']:
        if stats['invites'] >= plan['invites']:
            available_plans.append({
                'type': 'invites',
                'plan': plan,
                'required': plan['invites'],
                'current': stats['invites']
            })
    
    # Check boost-based plans
    for plan in FREE_VPS_PLANS['boosts']:
        if stats['boosts'] >= plan['boosts']:
            available_plans.append({
                'type': 'boosts',
                'plan': plan,
                'required': plan['boosts'],
                'current': stats['boosts']
            })
    
    if not available_plans:
        embed = create_error_embed("No Eligible Plans", 
            f"You don't qualify for any free VPS plans yet.\n\n**Your Stats:**\n• Invites: {stats['invites']}\n• Boosts: {stats['boosts']}\n\nUse `{PREFIX}plans` to see requirements.")
        await ctx.send(embed=embed)
        return
    
    # Sort plans by resource size (largest first)
    available_plans.sort(key=lambda x: x['plan']['ram'], reverse=True)
    best_plan = available_plans[0]
    
    embed = create_info_embed("Claim Free VPS", f"You qualify for: **{best_plan['plan']['name']}**")
    add_field(embed, "Requirements Met", 
              f"**{best_plan['type'].title()}:** {best_plan['current']}/{best_plan['required']}", False)
    add_field(embed, "Plan Resources",
              f"**RAM:** {best_plan['plan']['ram']}GB\n**CPU:** {best_plan['plan']['cpu']} cores\n**Storage:** {best_plan['plan']['disk']}GB", False)
    
    await ctx.send(embed=embed, view=ClaimFreeView(ctx, best_plan))

class ClaimFreeView(discord.ui.View):
    def __init__(self, ctx, plan_info):
        super().__init__(timeout=300)
        self.ctx = ctx
        self.plan_info = plan_info
    
    @discord.ui.button(label="Claim Now", style=discord.ButtonStyle.success, emoji="🎁")
    async def claim_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.ctx.author.id):
            await interaction.response.send_message("This claim button is not for you!", ephemeral=True)
            return
        
        # Check if user already claimed a free VPS
        stats = get_user_stats(str(self.ctx.author.id))
        if stats['claimed_free_vps'] > 0:
            await interaction.response.send_message(
                embed=create_error_embed("Already Claimed", "You have already claimed a free VPS!"),
                ephemeral=True
            )
            return
        
        # Send to admin for approval
        admin_embed = create_info_embed("Free VPS Claim Request", 
            f"**User:** {self.ctx.author.mention}\n**Plan:** {self.plan_info['plan']['name']}\n\n**Resources:**\n• RAM: {self.plan_info['plan']['ram']}GB\n• CPU: {self.plan_info['plan']['cpu']} cores\n• Storage: {self.plan_info['plan']['disk']}GB")
        
        try:
            main_admin = await bot.fetch_user(int(MAIN_ADMIN_ID))
            await main_admin.send(embed=admin_embed)
            await interaction.response.send_message(
                embed=create_success_embed("Request Sent", "Your free VPS claim request has been sent to the admin for approval!"),
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                embed=create_error_embed("Request Failed", "Could not send claim request. Please contact admin directly."),
                ephemeral=True
            )

@bot.command(name='invadd')
@is_admin()
async def add_invites(ctx, user: discord.Member, amount: int):
    """Add invites to user (Admin only)"""
    if amount <= 0:
        await ctx.send(embed=create_error_embed("Invalid Amount", "Amount must be positive."))
        return
    
    update_user_stats(str(user.id), invites=amount)
    stats = get_user_stats(str(user.id))
    
    embed = create_success_embed("Invites Added", f"Added {amount} invites to {user.mention}")
    add_field(embed, "Current Stats", f"**Total Invites:** {stats['invites']}\n**Boosts:** {stats['boosts']}", False)
    await ctx.send(embed=embed)

@bot.command(name='boostadd')
@is_admin()
async def add_boosts(ctx, user: discord.Member, amount: int):
    """Add boosts to user (Admin only)"""
    if amount <= 0:
        await ctx.send(embed=create_error_embed("Invalid Amount", "Amount must be positive."))
        return
    
    update_user_stats(str(user.id), boosts=amount)
    stats = get_user_stats(str(user.id))
    
    embed = create_success_embed("Boosts Added", f"Added {amount} boosts to {user.mention}")
    add_field(embed, "Current Stats", f"**Invites:** {stats['invites']}\n**Total Boosts:** {stats['boosts']}", False)
    await ctx.send(embed=embed)

# ============ VPS MANAGEMENT COMMANDS ============

class OSSelectView(discord.ui.View):
    def __init__(self, ram: int, cpu: int, disk: int, user: discord.Member, ctx):
        super().__init__(timeout=300)
        self.ram = ram
        self.cpu = cpu
        self.disk = disk
        self.user = user
        self.ctx = ctx
        self.select = discord.ui.Select(
            placeholder="Select an OS for the VPS",
            options=[discord.SelectOption(label=o["label"], value=o["value"]) for o in OS_OPTIONS]
        )
        self.select.callback = self.select_os
        self.add_item(self.select)
    
    async def select_os(self, interaction: discord.Interaction):
        if str(interaction.user.id) != str(self.ctx.author.id):
            await interaction.response.send_message(embed=create_error_embed("Access Denied", "Only the command author can select."), ephemeral=True)
            return
        
        os_version = self.select.values[0]
        self.select.disabled = True
        creating_embed = create_info_embed("Creating VPS", f"Deploying {os_version} VPS for {self.user.mention}...")
        await interaction.response.edit_message(embed=creating_embed, view=self)
        
        user_id = str(self.user.id)
        if user_id not in vps_data:
            vps_data[user_id] = []
        
        vps_count = len(vps_data[user_id]) + 1
        container_name = f"{BOT_NAME.lower()}-{user_id}-{vps_count}"
        ram_mb = self.ram * 1024
        
        try:
            await execute_lxc(f"lxc init {os_version} {container_name} -s {DEFAULT_STORAGE_POOL}")
            await execute_lxc(f"lxc config set {container_name} limits.memory {ram_mb}MB")
            await execute_lxc(f"lxc config set {container_name} limits.cpu {self.cpu}")
            await execute_lxc(f"lxc config device set {container_name} root size={self.disk}GB")
            await apply_lxc_config(container_name)
            await execute_lxc(f"lxc start {container_name}")
            await apply_internal_permissions(container_name)
            
            config_str = f"{self.ram}GB RAM / {self.cpu} CPU / {self.disk}GB Disk"
            vps_info = {
                "container_name": container_name,
                "ram": f"{self.ram}GB",
                "cpu": str(self.cpu),
                "storage": f"{self.disk}GB",
                "config": config_str,
                "os_version": os_version,
                "status": "running",
                "suspended": False,
                "whitelisted": False,
                "suspension_history": [],
                "created_at": datetime.now().isoformat(),
                "shared_with": [],
                "id": None
            }
            vps_data[user_id].append(vps_info)
            save_vps_data()
            
            if self.ctx.guild:
                vps_role = await get_or_create_vps_role(self.ctx.guild)
                if vps_role:
                    try:
                        await self.user.add_roles(vps_role, reason=f"{BOT_NAME} VPS ownership granted")
                    except discord.Forbidden:
                        logger.warning(f"Failed to assign {BOT_NAME} VPS role to {self.user.name}")
            
            success_embed = create_success_embed("VPS Created Successfully")
            add_field(success_embed, "Owner", self.user.mention, True)
            add_field(success_embed, "VPS ID", f"#{vps_count}", True)
            add_field(success_embed, "Container", f"`{container_name}`", True)
            add_field(success_embed, "Resources", f"**RAM:** {self.ram}GB\n**CPU:** {self.cpu} Cores\n**Storage:** {self.disk}GB", False)
            add_field(success_embed, "OS", os_version, True)
            add_field(success_embed, "Features", "Nesting, Privileged, FUSE, Kernel Modules (Docker Ready), Unprivileged Ports from 0", False)
            add_field(success_embed, "Disk Note", "Run `sudo resize2fs /` inside VPS if needed to expand filesystem.", False)
            
            await interaction.followup.send(embed=success_embed)
            
            # Send DM to user
            try:
                dm_embed = create_success_embed("VPS Created!", f"Your VPS has been successfully deployed by an admin!")
                add_field(dm_embed, "VPS Details", f"**VPS ID:** #{vps_count}\n**Container Name:** `{container_name}`\n**Configuration:** {config_str}\n**Status:** Running\n**OS:** {os_version}\n**Created:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", False)
                add_field(dm_embed, "Management", f"• Use `{PREFIX}manage` to start/stop/reinstall your VPS\n• Use `{PREFIX}manage` → SSH for terminal access\n• Contact admin for upgrades or issues", False)
                await self.user.send(embed=dm_embed)
            except discord.Forbidden:
                await self.ctx.send(embed=create_info_embed("Notification Failed", f"Couldn't send DM to {self.user.mention}. Please ensure DMs are enabled."))
        
        except Exception as e:
            error_embed = create_error_embed("Creation Failed", f"Error: {str(e)}")
            await interaction.followup.send(embed=error_embed)

@bot.command(name='create')
@is_admin()
async def create_vps(ctx, ram: int, cpu: int, disk: int, user: discord.Member):
    """Create a VPS for a user"""
    if ram <= 0 or cpu <= 0 or disk <= 0:
        await ctx.send(embed=create_error_embed("Invalid Specs", "RAM, CPU, and Disk must be positive integers."))
        return
    
    embed = create_info_embed("VPS Creation", f"Creating VPS for {user.mention} with {ram}GB RAM, {cpu} CPU cores, {disk}GB Disk.\nSelect OS below.")
    view = OSSelectView(ram, cpu, disk, user, ctx)
    await ctx.send(embed=embed, view=view)

class ManageView(discord.ui.View):
    def __init__(self, user_id, vps_list, is_shared=False, owner_id=None, is_admin=False, actual_index: Optional[int] = None):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.vps_list = vps_list[:]
        self.selected_index = None
        self.is_shared = is_shared
        self.owner_id = owner_id or user_id
        self.is_admin = is_admin
        self.actual_index = actual_index
        self.indices = list(range(len(vps_list)))
        
        if self.is_shared and self.actual_index is None:
            raise ValueError("actual_index required for shared views")
        
        if len(vps_list) > 1:
            options = [
                discord.SelectOption(
                    label=f"VPS {i+1} ({v.get('config', 'Custom')})",
                    description=f"Status: {v.get('status', 'unknown')}",
                    value=str(i)
                ) for i, v in enumerate(vps_list)
            ]
            self.select = discord.ui.Select(placeholder="Select a VPS to manage", options=options)
            self.select.callback = self.select_vps
            self.add_item(self.select)
            self.initial_embed = create_embed("VPS Management", "Select a VPS from the dropdown menu below.", 0x1a1a1a)
            add_field(self.initial_embed, "Available VPS", "\n".join([f"**VPS {i+1}:** `{v['container_name']}` - Status: `{v.get('status', 'unknown').upper()}`" for i, v in enumerate(vps_list)]), False)
        else:
            self.selected_index = 0
            self.initial_embed = None
            self.add_action_buttons()
    
    async def get_initial_embed(self):
        if self.initial_embed is not None:
            return self.initial_embed
        self.initial_embed = await self.create_vps_embed(self.selected_index)
        return self.initial_embed
    
    async def create_vps_embed(self, index):
        vps = self.vps_list[index]
        status = vps.get('status', 'unknown')
        suspended = vps.get('suspended', False)
        whitelisted = vps.get('whitelisted', False)
        status_color = 0x00ff88 if status == 'running' and not suspended else 0xffaa00 if suspended else 0xff3366
        container_name = vps['container_name']
        
        lxc_status = await get_container_status(container_name)
        cpu_usage = await get_container_cpu(container_name)
        memory_usage = await get_container_memory(container_name)
        disk_usage = await get_container_disk(container_name)
        uptime = await get_container_uptime(container_name)
        
        status_text = f"{lxc_status.upper()}"
        if suspended:
            status_text += " (SUSPENDED)"
        if whitelisted:
            status_text += " (WHITELISTED)"
        
        owner_text = ""
        if self.is_admin and self.owner_id != self.user_id:
            try:
                owner_user = await bot.fetch_user(int(self.owner_id))
                owner_text = f"\n**Owner:** {owner_user.mention}"
            except:
                owner_text = f"\n**Owner ID:** {self.owner_id}"
        
        embed = create_embed(
            f"VPS Management - VPS {index + 1}",
            f"Managing container: `{container_name}`{owner_text}",
            status_color
        )
        
        resource_info = f"**Configuration:** {vps.get('config', 'Custom')}\n"
        resource_info += f"**Status:** `{status_text}`\n"
        resource_info += f"**RAM:** {vps['ram']}\n"
        resource_info += f"**CPU:** {vps['cpu']} Cores\n"
        resource_info += f"**Storage:** {vps['storage']}\n"
        resource_info += f"**OS:** {vps.get('os_version', 'ubuntu:22.04')}\n"
        resource_info += f"**Uptime:** {uptime}"
        
        add_field(embed, "📊 Allocated Resources", resource_info, False)
        
        if suspended:
            add_field(embed, "⚠️ Suspended", "This VPS is suspended. Contact an admin to unsuspend.", False)
        if whitelisted:
            add_field(embed, "✅ Whitelisted", "This VPS is exempt from auto-suspension.", False)
        
        live_stats = f"**CPU Usage:** {cpu_usage}\n**Memory:** {memory_usage}\n**Disk:** {disk_usage}"
        add_field(embed, "📈 Live Usage", live_stats, False)
        add_field(embed, "🎮 Controls", "Use the buttons below to manage your VPS", False)
        
        return embed
    
    def add_action_buttons(self):
        if not self.is_shared and not self.is_admin:
            reinstall_button = discord.ui.Button(label="🔄 Reinstall", style=discord.ButtonStyle.danger)
            reinstall_button.callback = lambda inter: self.action_callback(inter, 'reinstall')
            self.add_item(reinstall_button)
        
        start_button = discord.ui.Button(label="▶ Start", style=discord.ButtonStyle.success)
        start_button.callback = lambda inter: self.action_callback(inter, 'start')
        
        stop_button = discord.ui.Button(label="⏸ Stop", style=discord.ButtonStyle.secondary)
        stop_button.callback = lambda inter: self.action_callback(inter, 'stop')
        
        ssh_button = discord.ui.Button(label="🔑 SSH", style=discord.ButtonStyle.primary)
        ssh_button.callback = lambda inter: self.action_callback(inter, 'tmate')
        
        stats_button = discord.ui.Button(label="📊 Stats", style=discord.ButtonStyle.secondary)
        stats_button.callback = lambda inter: self.action_callback(inter, 'stats')
        
        self.add_item(start_button)
        self.add_item(stop_button)
        self.add_item(ssh_button)
        self.add_item(stats_button)
    
    async def select_vps(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.user_id and not self.is_admin:
            await interaction.response.send_message(embed=create_error_embed("Access Denied", "This is not your VPS!"), ephemeral=True)
            return
        
        self.selected_index = int(self.select.values[0])
        await interaction.response.defer()
        new_embed = await self.create_vps_embed(self.selected_index)
        self.clear_items()
        self.add_action_buttons()
        await interaction.edit_original_response(embed=new_embed, view=self)
    
    async def action_callback(self, interaction: discord.Interaction, action: str):
        if str(interaction.user.id) != self.user_id and not self.is_admin:
            await interaction.response.send_message(embed=create_error_embed("Access Denied", "This is not your VPS!"), ephemeral=True)
            return
        
        if self.selected_index is None:
            await interaction.response.send_message(embed=create_error_embed("No VPS Selected", "Please select a VPS first."), ephemeral=True)
            return
        
        actual_idx = self.actual_index if self.is_shared else self.indices[self.selected_index]
        target_vps = vps_data[self.owner_id][actual_idx]
        suspended = target_vps.get('suspended', False)
        
        if suspended and not self.is_admin and action != 'stats':
            await interaction.response.send_message(embed=create_error_embed("Access Denied", "This VPS is suspended. Contact an admin to unsuspend."), ephemeral=True)
            return
        
        container_name = target_vps["container_name"]
        
        if action == 'stats':
            status = await get_container_status(container_name)
            cpu_usage = await get_container_cpu(container_name)
            memory_usage = await get_container_memory(container_name)
            disk_usage = await get_container_disk(container_name)
            uptime = await get_container_uptime(container_name)
            
            stats_embed = create_info_embed("📈 Live Statistics", f"Real-time stats for `{container_name}`")
            add_field(stats_embed, "Status", f"`{status.upper()}`", True)
            add_field(stats_embed, "CPU", cpu_usage, True)
            add_field(stats_embed, "Memory", memory_usage, True)
            add_field(stats_embed, "Disk", disk_usage, True)
            add_field(stats_embed, "Uptime", uptime, True)
            
            await interaction.response.send_message(embed=stats_embed, ephemeral=True)
            return
        
        if action == 'reinstall':
            if self.is_shared or self.is_admin:
                await interaction.response.send_message(embed=create_error_embed("Access Denied", "Only the VPS owner can reinstall!"), ephemeral=True)
                return
            
            if suspended:
                await interaction.response.send_message(embed=create_error_embed("Cannot Reinstall", "Unsuspend the VPS first."), ephemeral=True)
                return
            
            ram_gb = int(target_vps['ram'].replace('GB', ''))
            cpu = int(target_vps['cpu'])
            storage_gb = int(target_vps['storage'].replace('GB', ''))
            
            confirm_embed = create_warning_embed("Reinstall Warning",
                f"⚠️ **WARNING:** This will erase all data on VPS `{container_name}` and reinstall a fresh OS.\n\n"
                f"This action cannot be undone. Continue?")
            
            class ConfirmView(discord.ui.View):
                def __init__(self, parent_view, container_name, owner_id, actual_idx, ram_gb, cpu, storage_gb):
                    super().__init__(timeout=60)
                    self.parent_view = parent_view
                    self.container_name = container_name
                    self.owner_id = owner_id
                    self.actual_idx = actual_idx
                    self.ram_gb = ram_gb
                    self.cpu = cpu
                    self.storage_gb = storage_gb
                
                @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
                async def confirm(self, inter: discord.Interaction, item: discord.ui.Button):
                    await inter.response.defer(ephemeral=True)
                    try:
                        await inter.followup.send(embed=create_info_embed("Deleting Container", f"Forcefully removing container `{self.container_name}`..."), ephemeral=True)
                        await execute_lxc(f"lxc delete {self.container_name} --force")
                        
                        # Create reinstall OS selection view
                        os_view = ReinstallOSSelectView(self.parent_view, self.container_name, self.owner_id, self.actual_idx, self.ram_gb, self.cpu, self.storage_gb)
                        await inter.followup.send(embed=create_info_embed("Select OS", "Choose the new OS for reinstallation."), view=os_view, ephemeral=True)
                    except Exception as e:
                        await inter.followup.send(embed=create_error_embed("Delete Failed", f"Error: {str(e)}"), ephemeral=True)
                
                @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel(self, inter: discord.Interaction, item: discord.ui.Button):
                    new_embed = await self.parent_view.create_vps_embed(self.parent_view.selected_index)
                    await inter.response.edit_message(embed=new_embed, view=self.parent_view)
            
            await interaction.response.send_message(embed=confirm_embed, view=ConfirmView(self, container_name, self.owner_id, actual_idx, ram_gb, cpu, storage_gb), ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        if suspended:
            target_vps['suspended'] = False
            save_vps_data()
        
        if action == 'start':
            try:
                await execute_lxc(f"lxc start {container_name}")
                target_vps["status"] = "running"
                save_vps_data()
                await apply_internal_permissions(container_name)
                await interaction.followup.send(embed=create_success_embed("VPS Started", f"VPS `{container_name}` is now running!"), ephemeral=True)
            except Exception as e:
                await interaction.followup.send(embed=create_error_embed("Start Failed", str(e)), ephemeral=True)
        
        elif action == 'stop':
            try:
                await execute_lxc(f"lxc stop {container_name}", timeout=120)
                target_vps["status"] = "stopped"
                save_vps_data()
                await interaction.followup.send(embed=create_success_embed("VPS Stopped", f"VPS `{container_name}` has been stopped!"), ephemeral=True)
            except Exception as e:
                await interaction.followup.send(embed=create_error_embed("Stop Failed", str(e)), ephemeral=True)
        
        elif action == 'tmate':
            if suspended:
                await interaction.followup.send(embed=create_error_embed("Access Denied", "Cannot access suspended VPS."), ephemeral=True)
                return
            
            await interaction.followup.send(embed=create_info_embed("SSH Access", "Generating SSH connection..."), ephemeral=True)
            
            try:
                # Check if tmate is installed
                check_proc = await asyncio.create_subprocess_exec(
                    "lxc", "exec", container_name, "--", "which", "tmate",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await check_proc.communicate()
                
                if check_proc.returncode != 0:
                    await interaction.followup.send(embed=create_info_embed("Installing SSH", "Installing tmate..."), ephemeral=True)
                    await execute_lxc(f"lxc exec {container_name} -- apt-get update -y")
                    await execute_lxc(f"lxc exec {container_name} -- apt-get install tmate -y")
                    await interaction.followup.send(embed=create_success_embed("Installed", "SSH service installed!"), ephemeral=True)
                
                # Generate SSH session
                session_name = f"{BOT_NAME.lower()}-session-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                await execute_lxc(f"lxc exec {container_name} -- tmate -S /tmp/{session_name}.sock new-session -d")
                await asyncio.sleep(3)
                
                ssh_proc = await asyncio.create_subprocess_exec(
                    "lxc", "exec", container_name, "--", "tmate", "-S", f"/tmp/{session_name}.sock", "display", "-p", "#{tmate_ssh}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await ssh_proc.communicate()
                ssh_url = stdout.decode().strip() if stdout else None
                
                if ssh_url:
                    try:
                        ssh_embed = create_embed("🔑 SSH Access", f"SSH connection for VPS `{container_name}`:", 0x00ff88)
                        add_field(ssh_embed, "Command", f"```{ssh_url}```", False)
                        add_field(ssh_embed, "⚠️ Security", "This link is temporary. Do not share it.", False)
                        add_field(ssh_embed, "📝 Session", f"Session ID: {session_name}", False)
                        await interaction.user.send(embed=ssh_embed)
                        await interaction.followup.send(embed=create_success_embed("SSH Sent", f"Check your DMs for SSH link! Session: {session_name}"), ephemeral=True)
                    except discord.Forbidden:
                        await interaction.followup.send(embed=create_error_embed("DM Failed", "Enable DMs to receive SSH link!"), ephemeral=True)
                else:
                    error_msg = stderr.decode().strip() if stderr else "Unknown error"
                    await interaction.followup.send(embed=create_error_embed("SSH Failed", error_msg), ephemeral=True)
            
            except Exception as e:
                await interaction.followup.send(embed=create_error_embed("SSH Error", str(e)), ephemeral=True)
        
        # Update embed
        new_embed = await self.create_vps_embed(self.selected_index)
        await interaction.edit_original_response(embed=new_embed, view=self)

class ReinstallOSSelectView(discord.ui.View):
    def __init__(self, parent_view, container_name, owner_id, actual_idx, ram_gb, cpu, storage_gb):
        super().__init__(timeout=300)
        self.parent_view = parent_view
        self.container_name = container_name
        self.owner_id = owner_id
        self.actual_idx = actual_idx
        self.ram_gb = ram_gb
        self.cpu = cpu
        self.storage_gb = storage_gb
        
        self.select = discord.ui.Select(
            placeholder="Select an OS for the reinstall",
            options=[discord.SelectOption(label=o["label"], value=o["value"]) for o in OS_OPTIONS]
        )
        self.select.callback = self.select_os
        self.add_item(self.select)
    
    async def select_os(self, interaction: discord.Interaction):
        os_version = self.select.values[0]
        self.select.disabled = True
        creating_embed = create_info_embed("Reinstalling VPS", f"Deploying {os_version} for `{self.container_name}`...")
        await interaction.response.edit_message(embed=creating_embed, view=self)
        
        ram_mb = self.ram_gb * 1024
        
        try:
            await execute_lxc(f"lxc init {os_version} {self.container_name} -s {DEFAULT_STORAGE_POOL}")
            await execute_lxc(f"lxc config set {self.container_name} limits.memory {ram_mb}MB")
            await execute_lxc(f"lxc config set {self.container_name} limits.cpu {self.cpu}")
            await execute_lxc(f"lxc config device set {self.container_name} root size={self.storage_gb}GB")
            await apply_lxc_config(self.container_name)
            await execute_lxc(f"lxc start {self.container_name}")
            await apply_internal_permissions(self.container_name)
            
            target_vps = vps_data[self.owner_id][self.actual_idx]
            target_vps["os_version"] = os_version
            target_vps["status"] = "running"
            target_vps["suspended"] = False
            target_vps["created_at"] = datetime.now().isoformat()
            config_str = f"{self.ram_gb}GB RAM / {self.cpu} CPU / {self.storage_gb}GB Disk"
            target_vps["config"] = config_str
            save_vps_data()
            
            success_embed = create_success_embed("Reinstall Complete", f"VPS `{self.container_name}` has been successfully reinstalled!")
            add_field(success_embed, "Resources", f"**RAM:** {self.ram_gb}GB\n**CPU:** {self.cpu} Cores\n**Storage:** {self.storage_gb}GB", False)
            add_field(success_embed, "OS", os_version, True)
            add_field(success_embed, "Features", "Nesting, Privileged, FUSE, Kernel Modules (Docker Ready), Unprivileged Ports from 0", False)
            add_field(success_embed, "Disk Note", "Run `sudo resize2fs /` inside VPS if needed to expand filesystem.", False)
            
            await interaction.followup.send(embed=success_embed, ephemeral=True)
            self.stop()
        
        except Exception as e:
            error_embed = create_error_embed("Reinstall Failed", f"Error: {str(e)}")
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            self.stop()

@bot.command(name='manage')
async def manage_vps(ctx, user: discord.Member = None):
    """Manage VPS - user's own VPS or another user's VPS (admin only)"""
    if user:
        user_id_check = str(ctx.author.id)
        if user_id_check != str(MAIN_ADMIN_ID) and user_id_check not in admin_data.get("admins", []):
            await ctx.send(embed=create_error_embed("Access Denied", "Only admins can manage other users' VPS."))
            return
        
        user_id = str(user.id)
        vps_list = vps_data.get(user_id, [])
        if not vps_list:
            await ctx.send(embed=create_error_embed("No VPS Found", f"{user.mention} doesn't have any {BOT_NAME} VPS."))
            return
        
        view = ManageView(str(ctx.author.id), vps_list, is_admin=True, owner_id=user_id)
        await ctx.send(embed=create_info_embed(f"Managing {user.name}'s VPS", f"Managing VPS for {user.mention}"), view=view)
    
    else:
        user_id = str(ctx.author.id)
        vps_list = vps_data.get(user_id, [])
        if not vps_list:
            embed = create_error_embed("No VPS Found", f"You don't have any {BOT_NAME} VPS. Contact an admin to create one.")
            add_field(embed, "Quick Actions", f"• `{PREFIX}manage` - Manage VPS\n• Contact admin for VPS creation", False)
            await ctx.send(embed=embed)
            return
        
        view = ManageView(user_id, vps_list)
        embed = await view.get_initial_embed()
        await ctx.send(embed=embed, view=view)

@bot.command(name='share-user')
async def share_user(ctx, shared_user: discord.Member, vps_number: int):
    """Share VPS access with another user"""
    user_id = str(ctx.author.id)
    shared_user_id = str(shared_user.id)
    
    if user_id not in vps_data or vps_number < 1 or vps_number > len(vps_data[user_id]):
        await ctx.send(embed=create_error_embed("Invalid VPS", "Invalid VPS number or you don't have a VPS."))
        return
    
    vps = vps_data[user_id][vps_number - 1]
    if "shared_with" not in vps:
        vps["shared_with"] = []
    
    if shared_user_id in vps["shared_with"]:
        await ctx.send(embed=create_error_embed("Already Shared", f"{shared_user.mention} already has access to this VPS!"))
        return
    
    vps["shared_with"].append(shared_user_id)
    save_vps_data()
    
    await ctx.send(embed=create_success_embed("VPS Shared", f"VPS #{vps_number} shared with {shared_user.mention}!"))

@bot.command(name='share-ruser')
async def revoke_share(ctx, shared_user: discord.Member, vps_number: int):
    """Revoke VPS access from another user"""
    user_id = str(ctx.author.id)
    shared_user_id = str(shared_user.id)
    
    if user_id not in vps_data or vps_number < 1 or vps_number > len(vps_data[user_id]):
        await ctx.send(embed=create_error_embed("Invalid VPS", "Invalid VPS number or you don't have a VPS."))
        return
    
    vps = vps_data[user_id][vps_number - 1]
    if "shared_with" not in vps:
        vps["shared_with"] = []
    
    if shared_user_id not in vps["shared_with"]:
        await ctx.send(embed=create_error_embed("Not Shared", f"{shared_user.mention} doesn't have access to this VPS!"))
        return
    
    vps["shared_with"].remove(shared_user_id)
    save_vps_data()
    
    await ctx.send(embed=create_success_embed("Access Revoked", f"Access to VPS #{vps_number} revoked from {shared_user.mention}!"))

@bot.command(name='manage-shared')
async def manage_shared_vps(ctx, owner: discord.Member, vps_number: int):
    """Manage a VPS that has been shared with you"""
    owner_id = str(owner.id)
    user_id = str(ctx.author.id)
    
    if owner_id not in vps_data or vps_number < 1 or vps_number > len(vps_data[owner_id]):
        await ctx.send(embed=create_error_embed("Invalid VPS", "Invalid VPS number or owner doesn't have a VPS."))
        return
    
    vps = vps_data[owner_id][vps_number - 1]
    if user_id not in vps.get("shared_with", []):
        await ctx.send(embed=create_error_embed("Access Denied", "You do not have access to this VPS."))
        return
    
    view = ManageView(user_id, [vps], is_shared=True, owner_id=owner_id, actual_index=vps_number - 1)
    embed = await view.get_initial_embed()
    await ctx.send(embed=embed, view=view)

# ============ ADMIN COMMANDS ============

@bot.command(name='admin-add')
@is_main_admin()
async def admin_add(ctx, user: discord.Member):
    """Add an admin (Main Admin only)"""
    user_id = str(user.id)
    
    # Check if already main admin
    if user_id == str(MAIN_ADMIN_ID):
        await ctx.send(embed=create_error_embed("Already Admin", "This user is already the main admin!"))
        return
    
    # Check if already in admin list
    if user_id in admin_data.get("admins", []):
        await ctx.send(embed=create_error_embed("Already Admin", f"{user.mention} is already an admin!"))
        return
    
    try:
        # Add to admin list
        admin_data["admins"].append(user_id)
        
        # Save to database
        conn = get_db()
        cur = conn.cursor()
        cur.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()
        
        save_admin_data()
        
        embed = create_success_embed("Admin Added", f"{user.mention} has been added as an admin!")
        await ctx.send(embed=embed)
        
        try:
            dm_embed = create_info_embed("🎉 Admin Privileges", f"You have been granted admin privileges for {BOT_NAME} VPS Manager by {ctx.author.mention}.")
            await user.send(embed=dm_embed)
        except discord.Forbidden:
            pass
        
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        await ctx.send(embed=create_error_embed("Error", f"Failed to add admin: {str(e)}"))

@bot.command(name='admin-remove')
@is_main_admin()
async def admin_remove(ctx, user: discord.Member):
    """Remove an admin (Main Admin only)"""
    user_id = str(user.id)
    if user_id == str(MAIN_ADMIN_ID):
        await ctx.send(embed=create_error_embed("Cannot Remove", "You cannot remove the main admin!"))
        return
    
    if user_id not in admin_data.get("admins", []):
        await ctx.send(embed=create_error_embed("Not Admin", f"{user.mention} is not an admin!"))
        return
    
    try:
        admin_data["admins"].remove(user_id)
        
        # Remove from database
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
        
        save_admin_data()
        
        embed = create_success_embed("Admin Removed", f"{user.mention} has been removed as an admin!")
        await ctx.send(embed=embed)
        
        try:
            dm_embed = create_info_embed("⚠️ Admin Removed", f"Your admin privileges for {BOT_NAME} VPS Manager have been removed by {ctx.author.mention}.")
            await user.send(embed=dm_embed)
        except discord.Forbidden:
            pass
        
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        await ctx.send(embed=create_error_embed("Error", f"Failed to remove admin: {str(e)}"))

@bot.command(name='admin-list')
async def admin_list(ctx):
    """List all admins"""
    embed = create_info_embed("🛡️ Admin List", "Current administrators of the system")
    
    # Main admin
    try:
        main_admin = await bot.fetch_user(int(MAIN_ADMIN_ID))
        add_field(embed, "👑 Main Admin", f"{main_admin.mention}\n{main_admin.name}#{main_admin.discriminator}", False)
    except:
        add_field(embed, "👑 Main Admin", f"User ID: {MAIN_ADMIN_ID}", False)
    
    # Other admins
    if admin_data['admins']:
        admin_text = []
        for admin_id in admin_data['admins']:
            try:
                admin_user = await bot.fetch_user(int(admin_id))
                admin_text.append(f"• {admin_user.mention} - {admin_user.name}#{admin_user.discriminator}")
            except:
                admin_text.append(f"• User ID: {admin_id}")
        
        add_field(embed, "🛡️ Admins", "\n".join(admin_text), False)
    else:
        add_field(embed, "🛡️ Admins", "No additional admins", False)
    
    await ctx.send(embed=embed)

@bot.command(name='serverstats')
@is_admin()
async def server_stats(ctx):
    """Show server statistics (Admin only)"""
    try:
        # Get container count
        total_containers = sum(len(v) for v in vps_data.values())
        running_containers = 0
        suspended_containers = 0
        
        for vps_list in vps_data.values():
            for vps in vps_list:
                if vps.get('status') == 'running':
                    running_containers += 1
                if vps.get('suspended', False):
                    suspended_containers += 1
        
        # Get disk usage
        disk_result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True)
        disk_info = disk_result.stdout
        
        # Bot latency
        latency = round(bot.latency * 1000)
        
        embed = create_info_embed("📊 Server Statistics", "")
        
        # Host information
        host_info = f"**Bot Latency:** {latency}ms\n"
        host_info += f"**Bot Version:** Created by Wanny_Dragon • 6/01/2026\n"
        host_info += f"**Prefix:** `{PREFIX}`"
        
        # VPS overview
        vps_info = f"**Total VPS:** {total_containers}\n"
        vps_info += f"**Running:** {running_containers}\n"
        vps_info += f"**Stopped:** {total_containers - running_containers}\n"
        vps_info += f"**Suspended:** {suspended_containers}\n"
        vps_info += f"**Total Users:** {len(vps_data)}"
        
        # Check KVM
        kvm_info = "Checking..."
        try:
            result = subprocess.run(['lscpu'], capture_output=True, text=True)
            if 'KVM' in result.stdout.upper():
                kvm_info = "✅ KVM Available (Hardware Virtualization)"
            else:
                kvm_info = "❌ KVM Not Available"
        except:
            kvm_info = "Unknown"
        
        add_field(embed, "🏢 Host Information", host_info, True)
        add_field(embed, "🖥️ VPS Overview", vps_info, True)
        add_field(embed, "⚡ Virtualization", kvm_info, True)
        
        # Disk usage
        if disk_info:
            add_field(embed, "💾 Disk Usage", f"```\n{disk_info}\n```", False)
        
        # Additional system info
        try:
            uptime = get_uptime()
            add_field(embed, "⏱️ System Uptime", uptime, False)
        except:
            pass
        
        # Watermark with creator info
        embed.set_footer(text=f"{BOT_NAME} • Created by Wanny_Dragon • 6/01/2026")
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(embed=create_error_embed("Stats Failed", str(e)))

@bot.command(name='delete-vps')
@is_admin()
async def delete_vps(ctx, user: discord.Member, vps_number: int, *, reason: str = "No reason"):
    """Delete a user's VPS (Admin only) - FIXED VERSION"""
    user_id = str(user.id)
    if user_id not in vps_data or vps_number < 1 or vps_number > len(vps_data[user_id]):
        await ctx.send(embed=create_error_embed("Invalid VPS", "Invalid VPS number or user doesn't have a VPS."))
        return
    
    vps = vps_data[user_id][vps_number - 1]
    container_name = vps["container_name"]
    
    # First, remove all port forwards for this VPS from database
    conn = get_db()
    cur = conn.cursor()
    
    # Get all port forwards for this container
    cur.execute('SELECT id, host_port FROM port_forwards WHERE vps_container = ?', (container_name,))
    port_forwards = cur.fetchall()
    
    # Remove LXC proxy devices first
    for pf in port_forwards:
        forward_id, host_port = pf['id'], pf['host_port']
        try:
            # Try to remove TCP proxy
            await execute_lxc(f"lxc config device remove {container_name} tcp_proxy_{host_port} 2>/dev/null || true")
            # Try to remove UDP proxy
            await execute_lxc(f"lxc config device remove {container_name} udp_proxy_{host_port} 2>/dev/null || true")
        except Exception as e:
            logger.warning(f"Failed to remove port forward device {host_port} for {container_name}: {e}")
            continue
    
    # Delete from database
    cur.execute('DELETE FROM port_forwards WHERE vps_container = ?', (container_name,))
    conn.commit()
    
    # Check container status and stop if running
    try:
        status = await get_container_status(container_name)
        if status == 'running':
            await ctx.send(embed=create_info_embed("Stopping VPS", f"Stopping VPS `{container_name}` before deletion..."))
            try:
                # Try graceful stop first
                await execute_lxc(f"lxc stop {container_name}", timeout=30)
            except Exception as e:
                logger.warning(f"Graceful stop failed, trying force stop: {e}")
                await execute_lxc(f"lxc stop {container_name} --force", timeout=15)
    except Exception as e:
        logger.warning(f"Could not check/stop container {container_name}: {e}")
    
    await ctx.send(embed=create_info_embed("Deleting VPS", f"Removing VPS #{vps_number}..."))
    
    try:
        # Try multiple deletion methods
        delete_success = False
        
        # Method 1: Try normal delete
        try:
            await execute_lxc(f"lxc delete {container_name}")
            delete_success = True
        except Exception as e1:
            logger.warning(f"Normal delete failed: {e1}")
            
            # Method 2: Try force delete
            try:
                await execute_lxc(f"lxc delete {container_name} --force")
                delete_success = True
            except Exception as e2:
                logger.warning(f"Force delete failed: {e2}")
                
                # Method 3: Try with sudo if snap permissions issue
                try:
                    # Use sudo if snap permissions issue
                    proc = await asyncio.create_subprocess_exec(
                        "sudo", "lxc", "delete", container_name, "--force",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await proc.communicate()
                    
                    if proc.returncode == 0:
                        delete_success = True
                        logger.info(f"Successfully deleted {container_name} with sudo")
                    else:
                        error_msg = stderr.decode().strip()
                        logger.error(f"sudo delete failed: {error_msg}")
                        
                        # Method 4: Manual cleanup - at least remove from database
                        await ctx.send(embed=create_warning_embed("Manual Cleanup", 
                            f"Automatic deletion failed. Removing from database anyway..."))
                        
                        # Remove from database anyway
                        del vps_data[user_id][vps_number - 1]
                        if not vps_data[user_id]:
                            del vps_data[user_id]
                        save_vps_data()
                        
                        # Clean up orphaned container manually
                        embed = create_warning_embed("Manual Action Required",
                            f"Container `{container_name}` deletion partially failed.\n\n"
                            f"**Manual cleanup steps:**\n"
                            f"1. Run: `sudo lxc delete {container_name} --force`\n"
                            f"2. Or: `sudo lxc stop {container_name} --force && sudo lxc delete {container_name}`\n\n"
                            f"VPS has been removed from database.")
                        
                        # Notify main admin
                        try:
                            main_admin = await bot.fetch_user(int(MAIN_ADMIN_ID))
                            await main_admin.send(embed=embed)
                        except:
                            pass
                        
                        embed = create_success_embed("VPS Removed from Database",
                            f"VPS #{vps_number} has been removed from the database.\n\n"
                            f"**Container:** `{container_name}`\n"
                            f"**Owner:** {user.mention}\n"
                            f"**Reason:** {reason}\n\n"
                            f"Note: Manual container cleanup may be required.")
                        await ctx.send(embed=embed)
                        conn.close()
                        return
                        
                except Exception as e3:
                    logger.error(f"All deletion methods failed: {e3}")
                    # Still remove from database
                    del vps_data[user_id][vps_number - 1]
                    if not vps_data[user_id]:
                        del vps_data[user_id]
                    save_vps_data()
                    
                    embed = create_warning_embed("Database Cleanup Complete",
                        f"VPS #{vps_number} removed from database but container deletion failed.\n\n"
                        f"**Container:** `{container_name}`\n"
                        f"**Manual cleanup required**")
                    await ctx.send(embed=embed)
                    conn.close()
                    return
        
        # If deletion succeeded
        if delete_success:
            # Remove from data structure
            del vps_data[user_id][vps_number - 1]
            
            if not vps_data[user_id]:
                del vps_data[user_id]
                if ctx.guild:
                    vps_role = await get_or_create_vps_role(ctx.guild)
                    if vps_role and vps_role in user.roles:
                        try:
                            await user.remove_roles(vps_role, reason="No VPS ownership")
                        except discord.Forbidden:
                            logger.warning(f"Failed to remove VPS role from {user.name}")
            
            save_vps_data()
            
            embed = create_success_embed("VPS Deleted Successfully")
            add_field(embed, "Owner", user.mention, True)
            add_field(embed, "VPS ID", f"#{vps_number}", True)
            add_field(embed, "Container", f"`{container_name}`", True)
            add_field(embed, "Reason", reason, False)
            
            await ctx.send(embed=embed)
    
    except Exception as e:
        await ctx.send(embed=create_error_embed("Deletion Failed", f"Error: {str(e)}"))
    finally:
        conn.close()

@bot.command(name='list-all')
@is_admin()
async def list_all_vps(ctx):
    """List all VPS on the server (Admin only)"""
    total_vps = 0
    total_users = len(vps_data)
    running_vps = 0
    stopped_vps = 0
    suspended_vps = 0
    whitelisted_vps = 0
    
    vps_info = []
    user_summary = []
    
    for user_id, vps_list in vps_data.items():
        try:
            user = await bot.fetch_user(int(user_id))
            user_vps_count = len(vps_list)
            user_running = sum(1 for vps in vps_list if vps.get('status') == 'running' and not vps.get('suspended', False))
            user_stopped = sum(1 for vps in vps_list if vps.get('status') == 'stopped')
            user_suspended = sum(1 for vps in vps_list if vps.get('suspended', False))
            user_whitelisted = sum(1 for vps in vps_list if vps.get('whitelisted', False))
            
            total_vps += user_vps_count
            running_vps += user_running
            stopped_vps += user_stopped
            suspended_vps += user_suspended
            whitelisted_vps += user_whitelisted
            
            user_summary.append(f"**{user.name}** ({user.mention}) - {user_vps_count} VPS ({user_running} running, {user_suspended} suspended, {user_whitelisted} whitelisted)")
            
            for i, vps in enumerate(vps_list):
                status_emoji = "🟢" if vps.get('status') == 'running' and not vps.get('suspended', False) else "🟡" if vps.get('suspended', False) else "🔴"
                status_text = vps.get('status', 'unknown').upper()
                if vps.get('suspended', False):
                    status_text += " (SUSPENDED)"
                if vps.get('whitelisted', False):
                    status_text += " (WHITELISTED)"
                
                vps_info.append(f"{status_emoji} **{user.name}** - VPS {i+1}: `{vps['container_name']}` - {vps.get('config', 'Custom')} - {status_text}")
        
        except discord.NotFound:
            vps_info.append(f"❓ Unknown User ({user_id}) - {len(vps_list)} VPS")
    
    embed = create_embed("All VPS Information", "Complete overview of all VPS deployments and user statistics", 0x1a1a1a)
    add_field(embed, "System Overview", f"**Total Users:** {total_users}\n**Total VPS:** {total_vps}\n**Running:** {running_vps}\n**Stopped:** {stopped_vps}\n**Suspended:** {suspended_vps}\n**Whitelisted:** {whitelisted_vps}", False)
    
    await ctx.send(embed=embed)
    
    if user_summary:
        embed = create_embed("User Summary", f"Summary of all users and their VPS", 0x1a1a1a)
        summary_text = "\n".join(user_summary)
        chunks = [summary_text[i:i+1024] for i in range(0, len(summary_text), 1024)]
        for idx, chunk in enumerate(chunks, 1):
            add_field(embed, f"Users (Part {idx})", chunk, False)
        await ctx.send(embed=embed)
    
    if vps_info:
        vps_text = "\n".join(vps_info)
        chunks = [vps_text[i:i+1024] for i in range(0, len(vps_text), 1024)]
        for idx, chunk in enumerate(chunks, 1):
            embed = create_embed(f"VPS Details (Part {idx})", "List of all VPS deployments", 0x1a1a1a)
            add_field(embed, "VPS List", chunk, False)
            await ctx.send(embed=embed)

@bot.command(name='userinfo')
@is_admin()
async def user_info(ctx, user: discord.Member):
    """Get detailed information about a user (Admin only)"""
    user_id = str(user.id)
    vps_list = vps_data.get(user_id, [])
    
    embed = create_embed(f"User Information - {user.name}", f"Detailed information for {user.mention}", 0x1a1a1a)
    
    add_field(embed, "👤 User Details", f"**Name:** {user.name}\n**ID:** {user.id}\n**Joined:** {user.joined_at.strftime('%Y-%m-%d %H:%M:%S') if user.joined_at else 'Unknown'}", False)
    
    if vps_list:
        vps_info = []
        total_ram = 0
        total_cpu = 0
        total_storage = 0
        running_count = 0
        suspended_count = 0
        whitelisted_count = 0
        
        for i, vps in enumerate(vps_list):
            status_emoji = "🟢" if vps.get('status') == 'running' and not vps.get('suspended', False) else "🟡" if vps.get('suspended', False) else "🔴"
            status_text = vps.get('status', 'unknown').upper()
            if vps.get('suspended', False):
                status_text += " (SUSPENDED)"
                suspended_count += 1
            else:
                running_count += 1 if vps.get('status') == 'running' else 0
            
            if vps.get('whitelisted', False):
                whitelisted_count += 1
            
            vps_info.append(f"{status_emoji} VPS {i+1}: `{vps['container_name']}` - {status_text}")
            
            ram_gb = int(vps['ram'].replace('GB', ''))
            storage_gb = int(vps['storage'].replace('GB', ''))
            total_ram += ram_gb
            total_cpu += int(vps['cpu'])
            total_storage += storage_gb
        
        vps_summary = f"**Total VPS:** {len(vps_list)}\n**Running:** {running_count}\n**Suspended:** {suspended_count}\n**Whitelisted:** {whitelisted_count}\n**Total RAM:** {total_ram}GB\n**Total CPU:** {total_cpu} cores\n**Total Storage:** {total_storage}GB"
        add_field(embed, "🖥️ VPS Information", vps_summary, False)
        
        vps_text = "\n".join(vps_info)
        chunks = [vps_text[i:i+1024] for i in range(0, len(vps_text), 1024)]
        for idx, chunk in enumerate(chunks, 1):
            add_field(embed, f"📋 VPS List (Part {idx})", chunk, False)
    else:
        add_field(embed, "🖥️ VPS Information", "**No VPS owned**", False)
    
    port_quota = get_user_allocation(user_id)
    port_used = get_user_used_ports(user_id)
    add_field(embed, "🌐 Port Quota", f"Allocated: {port_quota}, Used: {port_used}", False)
    
    is_admin_user = user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", [])
    add_field(embed, "🛡️ Admin Status", f"**{'Yes' if is_admin_user else 'No'}**", False)
    
    await ctx.send(embed=embed)

@bot.command(name='add-resources')
@is_admin()
async def add_resources(ctx, vps_id: str, ram: int = None, cpu: int = None, disk: int = None):
    """Add resources to a VPS (Admin only)"""
    if ram is None and cpu is None and disk is None:
        await ctx.send(embed=create_error_embed("Missing Parameters", "Please specify at least one resource to add (ram, cpu, or disk)"))
        return
    
    found_vps = None
    user_id = None
    vps_index = None
    
    for uid, vps_list in vps_data.items():
        for i, vps in enumerate(vps_list):
            if vps['container_name'] == vps_id:
                found_vps = vps
                user_id = uid
                vps_index = i
                break
        if found_vps:
            break
    
    if not found_vps:
        await ctx.send(embed=create_error_embed("VPS Not Found", f"No VPS found with ID: `{vps_id}`"))
        return
    
    was_running = found_vps.get('status') == 'running' and not found_vps.get('suspended', False)
    disk_changed = disk is not None
    
    if was_running:
        await ctx.send(embed=create_info_embed("Stopping VPS", f"Stopping VPS `{vps_id}` to apply resource changes..."))
        try:
            await execute_lxc(f"lxc stop {vps_id}")
            found_vps['status'] = 'stopped'
            save_vps_data()
        except Exception as e:
            await ctx.send(embed=create_error_embed("Stop Failed", f"Error stopping VPS: {str(e)}"))
            return
    
    changes = []
    try:
        current_ram_gb = int(found_vps['ram'].replace('GB', ''))
        current_cpu = int(found_vps['cpu'])
        current_disk_gb = int(found_vps['storage'].replace('GB', ''))
        
        new_ram_gb = current_ram_gb
        new_cpu = current_cpu
        new_disk_gb = current_disk_gb
        
        if ram is not None and ram > 0:
            new_ram_gb += ram
            ram_mb = new_ram_gb * 1024
            await execute_lxc(f"lxc config set {vps_id} limits.memory {ram_mb}MB")
            changes.append(f"RAM: +{ram}GB (New total: {new_ram_gb}GB)")
        
        if cpu is not None and cpu > 0:
            new_cpu += cpu
            await execute_lxc(f"lxc config set {vps_id} limits.cpu {new_cpu}")
            changes.append(f"CPU: +{cpu} cores (New total: {new_cpu} cores)")
        
        if disk is not None and disk > 0:
            new_disk_gb += disk
            await execute_lxc(f"lxc config device set {vps_id} root size={new_disk_gb}GB")
            changes.append(f"Disk: +{disk}GB (New total: {new_disk_gb}GB)")
        
        found_vps['ram'] = f"{new_ram_gb}GB"
        found_vps['cpu'] = str(new_cpu)
        found_vps['storage'] = f"{new_disk_gb}GB"
        found_vps['config'] = f"{new_ram_gb}GB RAM / {new_cpu} CPU / {new_disk_gb}GB Disk"
        vps_data[user_id][vps_index] = found_vps
        save_vps_data()
        
        if was_running:
            await execute_lxc(f"lxc start {vps_id}")
            found_vps['status'] = 'running'
            save_vps_data()
            await apply_internal_permissions(vps_id)
        
        embed = create_success_embed("Resources Added", f"Successfully added resources to VPS `{vps_id}`")
        add_field(embed, "Changes Applied", "\n".join(changes), False)
        
        if disk_changed:
            add_field(embed, "Disk Note", "Run `sudo resize2fs /` inside the VPS to expand the filesystem.", False)
        
        await ctx.send(embed=embed)
    
    except Exception as e:
        await ctx.send(embed=create_error_embed("Resource Addition Failed", f"Error: {str(e)}"))

@bot.command(name='set-threshold')
@is_admin()
async def set_threshold(ctx, cpu: int, ram: int):
    """Set resource thresholds for auto-suspension (Admin only)"""
    global CPU_THRESHOLD, RAM_THRESHOLD
    
    if cpu < 0 or ram < 0:
        await ctx.send(embed=create_error_embed("Invalid Thresholds", "Thresholds must be non-negative."))
        return
    
    CPU_THRESHOLD = cpu
    RAM_THRESHOLD = ram
    set_setting('cpu_threshold', str(cpu))
    set_setting('ram_threshold', str(ram))
    
    embed = create_success_embed("Thresholds Updated", f"**CPU:** {cpu}%\n**RAM:** {ram}%")
    await ctx.send(embed=embed)

@bot.command(name='set-status')
@is_admin()
async def set_status(ctx, activity_type: str, *, name: str):
    """Set bot status (Admin only)"""
    types = {
        'playing': discord.ActivityType.playing,
        'watching': discord.ActivityType.watching,
        'listening': discord.ActivityType.listening,
        'streaming': discord.ActivityType.streaming,
    }
    
    if activity_type.lower() not in types:
        await ctx.send(embed=create_error_embed("Invalid Type", "Valid types: playing, watching, listening, streaming"))
        return
    
    await bot.change_presence(activity=discord.Activity(type=types[activity_type.lower()], name=name))
    embed = create_success_embed("Status Updated", f"Set to {activity_type}: {name}")
    await ctx.send(embed=embed)

@bot.command(name='thresholds')
@is_admin()
async def thresholds(ctx):
    """Show current resource thresholds (Admin only)"""
    embed = create_info_embed("Resource Thresholds", f"**CPU:** {CPU_THRESHOLD}%\n**RAM:** {RAM_THRESHOLD}%")
    await ctx.send(embed=embed)

# ============ PORT FORWARDING COMMANDS ============

@bot.command(name='ports')
async def ports_command(ctx, subcmd: str = None, *args):
    """Manage port forwarding"""
    user_id = str(ctx.author.id)
    allocated = get_user_allocation(user_id)
    used = get_user_used_ports(user_id)
    available = allocated - used
    
    if subcmd is None:
        embed = create_info_embed("Port Forwarding Help", f"**Your Quota:** Allocated: {allocated}, Used: {used}, Available: {available}")
        add_field(embed, "Commands", f"{PREFIX}ports add <vps_num> <vps_port>\n{PREFIX}ports list\n{PREFIX}ports remove <id>", False)
        await ctx.send(embed=embed)
        return
    
    if subcmd == 'add':
        if len(args) < 2:
            await ctx.send(embed=create_error_embed("Usage", f"Usage: {PREFIX}ports add <vps_number> <vps_port>"))
            return
        
        try:
            vps_num = int(args[0])
            vps_port = int(args[1])
            if vps_port < 1 or vps_port > 65535:
                raise ValueError
        except ValueError:
            await ctx.send(embed=create_error_embed("Invalid Input", "VPS number and port must be positive integers (port: 1-65535)."))
            return
        
        vps_list = vps_data.get(user_id, [])
        if vps_num < 1 or vps_num > len(vps_list):
            await ctx.send(embed=create_error_embed("Invalid VPS", f"Invalid VPS number (1-{len(vps_list)}). Use {PREFIX}myvps to list."))
            return
        
        vps = vps_list[vps_num - 1]
        container = vps['container_name']
        
        if used >= allocated:
            await ctx.send(embed=create_error_embed("Quota Exceeded", f"No available slots. Allocated: {allocated}, Used: {used}. Contact admin for more."))
            return
        
        host_port = await create_port_forward(user_id, container, vps_port)
        if host_port:
            embed = create_success_embed("Port Forward Created", f"VPS #{vps_num} port {vps_port} (TCP/UDP) forwarded to host port {host_port}.")
            add_field(embed, "Access", f"External: {YOUR_SERVER_IP}:{host_port} → VPS:{vps_port} (TCP & UDP)", False)
            add_field(embed, "Quota Update", f"Used: {used + 1}/{allocated}", False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(embed=create_error_embed("Failed", "Could not assign host port. Try again later."))
    
    elif subcmd == 'list':
        forwards = get_user_forwards(user_id)
        embed = create_info_embed("Your Port Forwards", f"**Quota:** Allocated: {allocated}, Used: {used}, Available: {available}")
        
        if not forwards:
            add_field(embed, "Forwards", "No active port forwards.", False)
        else:
            text = []
            for f in forwards:
                vps_num = next((i+1 for i, v in enumerate(vps_data.get(user_id, [])) if v['container_name'] == f['vps_container']), 'Unknown')
                created = datetime.fromisoformat(f['created_at']).strftime('%Y-%m-%d %H:%M')
                text.append(f"**ID {f['id']}** - VPS #{vps_num}: {f['vps_port']} (TCP/UDP) → {f['host_port']} (Created: {created})")
            
            add_field(embed, "Active Forwards", "\n".join(text[:10]), False)
            if len(forwards) > 10:
                add_field(embed, "Note", f"Showing 10 of {len(forwards)}. Remove unused with {PREFIX}ports remove <id>.")
        
        await ctx.send(embed=embed)
    
    elif subcmd == 'remove':
        if len(args) < 1:
            await ctx.send(embed=create_error_embed("Usage", f"Usage: {PREFIX}ports remove <forward_id>"))
            return
        
        try:
            fid = int(args[0])
        except ValueError:
            await ctx.send(embed=create_error_embed("Invalid ID", "Forward ID must be an integer."))
            return
        
        success, _ = await remove_port_forward(fid)
        if success:
            embed = create_success_embed("Removed", f"Port forward {fid} removed (TCP & UDP).")
            add_field(embed, "Quota Update", f"Used: {used - 1}/{allocated}", False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(embed=create_error_embed("Not Found", "Forward ID not found. Use !ports list."))
    
    else:
        await ctx.send(embed=create_error_embed("Invalid Subcommand", f"Use: add <vps_num> <port>, list, remove <id>"))

@bot.command(name='ports-add-user')
@is_admin()
async def ports_add_user(ctx, amount: int, user: discord.Member):
    """Allocate port slots to a user (Admin only)"""
    if amount <= 0:
        await ctx.send(embed=create_error_embed("Invalid Amount", "Amount must be a positive integer."))
        return
    
    user_id = str(user.id)
    allocate_ports(user_id, amount)
    
    embed = create_success_embed("Ports Allocated", f"Allocated {amount} port slots to {user.mention}.")
    add_field(embed, "Quota", f"Total: {get_user_allocation(user_id)} slots", False)
    await ctx.send(embed=embed)
    
    try:
        dm_embed = create_info_embed("Port Slots Allocated", f"You have been granted {amount} additional port forwarding slots by an admin.\nUse `{PREFIX}ports list` to view your quota and active forwards.")
        await user.send(embed=dm_embed)
    except discord.Forbidden:
        await ctx.send(embed=create_info_embed("DM Failed", f"Could not notify {user.mention} via DM."))

@bot.command(name='ports-remove-user')
@is_admin()
async def ports_remove_user(ctx, amount: int, user: discord.Member):
    """Deallocate port slots from a user (Admin only)"""
    if amount <= 0:
        await ctx.send(embed=create_error_embed("Invalid Amount", "Amount must be a positive integer."))
        return
    
    user_id = str(user.id)
    current = get_user_allocation(user_id)
    if amount > current:
        amount = current
    
    deallocate_ports(user_id, amount)
    remaining = get_user_allocation(user_id)
    
    embed = create_success_embed("Ports Deallocated", f"Removed {amount} port slots from {user.mention}.")
    add_field(embed, "Remaining Quota", f"{remaining} slots", False)
    await ctx.send(embed=embed)
    
    try:
        dm_embed = create_warning_embed("Port Slots Reduced", f"Your port forwarding quota has been reduced by {amount} slots by an admin.\nRemaining: {remaining} slots.")
        await user.send(embed=dm_embed)
    except discord.Forbidden:
        await ctx.send(embed=create_info_embed("DM Failed", f"Could not notify {user.mention} via DM."))

@bot.command(name='ports-revoke')
@is_admin()
async def ports_revoke(ctx, forward_id: int):
    """Revoke a port forward (Admin only)"""
    success, user_id = await remove_port_forward(forward_id, is_admin=True)
    if success and user_id:
        try:
            user = await bot.fetch_user(int(user_id))
            dm_embed = create_warning_embed("Port Forward Revoked", f"One of your port forwards (ID: {forward_id}) has been revoked by an admin.")
            await user.send(embed=dm_embed)
        except:
            pass
        await ctx.send(embed=create_success_embed("Revoked", f"Port forward ID {forward_id} revoked."))
    else:
        await ctx.send(embed=create_error_embed("Failed", "Port forward ID not found or removal failed."))

# ============ ADDITIONAL VPS MANAGEMENT COMMANDS ============

@bot.command(name='restart-vps')
@is_admin()
async def restart_vps(ctx, container_name: str):
    """Restart a VPS (Admin only)"""
    await ctx.send(embed=create_info_embed("Restarting VPS", f"Restarting VPS `{container_name}`..."))
    
    try:
        await execute_lxc(f"lxc restart {container_name}")
        
        for user_id, vps_list in vps_data.items():
            for vps in vps_list:
                if vps['container_name'] == container_name:
                    vps['status'] = 'running'
                    vps['suspended'] = False
                    save_vps_data()
                    break
        
        await apply_internal_permissions(container_name)
        await ctx.send(embed=create_success_embed("VPS Restarted", f"VPS `{container_name}` has been restarted successfully!"))
    
    except Exception as e:
        await ctx.send(embed=create_error_embed("Restart Failed", f"Error: {str(e)}"))

@bot.command(name='stop-vps-all')
@is_admin()
async def stop_all_vps(ctx):
    """Stop all VPS (Admin only)"""
    embed = create_warning_embed("Stopping All VPS", "⚠️ **WARNING:** This will stop ALL running VPS on the server.\n\nThis action cannot be undone. Continue?")
    
    class ConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
        
        @discord.ui.button(label="Stop All VPS", style=discord.ButtonStyle.danger)
        async def confirm(self, interaction: discord.Interaction, item: discord.ui.Button):
            await interaction.response.defer()
            try:
                proc = await asyncio.create_subprocess_exec(
                    "lxc", "stop", "--all", "--force",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if proc.returncode == 0:
                    stopped_count = 0
                    for user_id, vps_list in vps_data.items():
                        for vps in vps_list:
                            if vps.get('status') == 'running':
                                vps['status'] = 'stopped'
                                vps['suspended'] = False
                                stopped_count += 1
                    
                    save_vps_data()
                    embed = create_success_embed("All VPS Stopped", f"Successfully stopped {stopped_count} VPS using `lxc stop --all --force`")
                    output_text = stdout.decode() if stdout else 'No output'
                    add_field(embed, "Command Output", f"```\n{output_text}\n```", False)
                    await interaction.followup.send(embed=embed)
                else:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    embed = create_error_embed("Stop Failed", f"Failed to stop VPS: {error_msg}")
                    await interaction.followup.send(embed=embed)
            
            except Exception as e:
                embed = create_error_embed("Error", f"Error stopping VPS: {str(e)}")
                await interaction.followup.send(embed=embed)
        
        @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
        async def cancel(self, interaction: discord.Interaction, item: discord.ui.Button):
            await interaction.response.edit_message(embed=create_info_embed("Operation Cancelled", "The stop all VPS operation has been cancelled."))
    
    await ctx.send(embed=embed, view=ConfirmView())

@bot.command(name='vpsinfo')
@is_admin()
async def vps_info(ctx, container_name: str = None):
    """Get detailed information about a VPS (Admin only)"""
    if not container_name:
        all_vps = []
        for user_id, vps_list in vps_data.items():
            try:
                user = await bot.fetch_user(int(user_id))
                for i, vps in enumerate(vps_list):
                    status_text = vps.get('status', 'unknown').upper()
                    if vps.get('suspended', False):
                        status_text += " (SUSPENDED)"
                    if vps.get('whitelisted', False):
                        status_text += " (WHITELISTED)"
                    all_vps.append(f"**{user.name}** - VPS {i+1}: `{vps['container_name']}` - {status_text}")
            except:
                pass
        
        vps_text = "\n".join(all_vps)
        chunks = [vps_text[i:i+1024] for i in range(0, len(vps_text), 1024)]
        for idx, chunk in enumerate(chunks, 1):
            embed = create_embed(f"🖥️ All VPS (Part {idx})", f"List of all VPS deployments", 0x1a1a1a)
            add_field(embed, "VPS List", chunk, False)
            await ctx.send(embed=embed)
    
    else:
        found_vps = None
        found_user = None
        
        for user_id, vps_list in vps_data.items():
            for vps in vps_list:
                if vps['container_name'] == container_name:
                    found_vps = vps
                    found_user = await bot.fetch_user(int(user_id))
                    break
            if found_vps:
                break
        
        if not found_vps:
            await ctx.send(embed=create_error_embed("VPS Not Found", f"No VPS found with container name: `{container_name}`"))
            return
        
        suspended_text = " (SUSPENDED)" if found_vps.get('suspended', False) else ""
        whitelisted_text = " (WHITELISTED)" if found_vps.get('whitelisted', False) else ""
        
        embed = create_embed(f"🖥️ VPS Information - {container_name}", f"Details for VPS owned by {found_user.mention}{suspended_text}{whitelisted_text}", 0x1a1a1a)
        
        add_field(embed, "👤 Owner", f"**Name:** {found_user.name}\n**ID:** {found_user.id}", False)
        add_field(embed, "📊 Specifications", f"**RAM:** {found_vps['ram']}\n**CPU:** {found_vps['cpu']} Cores\n**Storage:** {found_vps['storage']}", False)
        add_field(embed, "📈 Status", f"**Current:** {found_vps.get('status', 'unknown').upper()}{suspended_text}{whitelisted_text}\n**Suspended:** {found_vps.get('suspended', False)}\n**Whitelisted:** {found_vps.get('whitelisted', False)}\n**Created:** {found_vps.get('created_at', 'Unknown')}", False)
        
        if 'config' in found_vps:
            add_field(embed, "⚙️ Configuration", f"**Config:** {found_vps['config']}", False)
        
        if found_vps.get('shared_with'):
            shared_users = []
            for shared_id in found_vps['shared_with']:
                try:
                    shared_user = await bot.fetch_user(int(shared_id))
                    shared_users.append(f"• {shared_user.mention}")
                except:
                    shared_users.append(f"• Unknown User ({shared_id})")
            
            shared_text = "\n".join(shared_users)
            add_field(embed, "🔗 Shared With", shared_text, False)
        
        # Port forwards for this VPS
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM port_forwards WHERE vps_container = ?', (container_name,))
        port_count = cur.fetchone()[0]
        conn.close()
        
        add_field(embed, "🌐 Active Ports", f"{port_count} forwarded ports (TCP/UDP)", False)
        await ctx.send(embed=embed)

# ============ COMMAND ALIASES ============

@bot.command(name='commands')
async def commands_alias(ctx):
    """Alias for help command"""
    await help_command(ctx)

@bot.command(name='stats')
async def stats_alias(ctx):
    """Alias for serverstats command (Admin only)"""
    if str(ctx.author.id) == str(MAIN_ADMIN_ID) or str(ctx.author.id) in admin_data.get("admins", []):
        await server_stats(ctx)
    else:
        await ctx.send(embed=create_error_embed("Access Denied", "This command requires admin privileges."))

@bot.command(name='info')
async def info_alias(ctx, user: discord.Member = None):
    """Alias for userinfo command (Admin only)"""
    if str(ctx.author.id) == str(MAIN_ADMIN_ID) or str(ctx.author.id) in admin_data.get("admins", []):
        if user:
            await user_info(ctx, user)
        else:
            await ctx.send(embed=create_error_embed("Usage", f"Please specify a user: `{PREFIX}info @user`"))
    else:
        await ctx.send(embed=create_error_embed("Access Denied", "This command requires admin privileges."))

@bot.command(name='mangage')
async def manage_typo(ctx):
    """Handle typo for manage command"""
    await ctx.send(embed=create_info_embed("Command Correction", f"Did you mean `{PREFIX}manage`? Use the correct command."))

# Run the bot
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN)
    else:
        logger.error("No Discord token found in DISCORD_TOKEN environment variable.")
