# bot.py - Complete Discord VPS Manager with Node System, Purge System, and All Features
import discord
from discord.ext import commands
import asyncio
import subprocess
import json
from datetime import datetime, timedelta
import shlex
import logging
import shutil
import os
from typing import Optional, List, Dict, Any, Tuple
import threading
import time
import sqlite3
import random
import math
from collections import defaultdict

# Load environment variables
DISCORD_TOKEN = ''
BOT_NAME = 'VOLTARISVM'
PREFIX = '!'
YOUR_SERVER_IP = ''
MAIN_ADMIN_ID = '1066956966867517572'
VPS_USER_ROLE_ID = ''
DEFAULT_STORAGE_POOL = 'default'

# OS Options for Node Creation - Updated with Ubuntu 25.04 and Debian 10
OS_OPTIONS = [
    {"label": "Ubuntu 20.04 LTS", "value": "ubuntu:20.04"},
    {"label": "Ubuntu 22.04 LTS", "value": "ubuntu:22.04"},
    {"label": "Ubuntu 24.04 LTS", "value": "ubuntu:24.04"},
    {"label": "Ubuntu 25.04 LTS", "value": "ubuntu:25.04"},
    {"label": "Debian 10 (Buster)", "value": "debian:10"},
    {"label": "Debian 11 (Bullseye)", "value": "debian:11"},
    {"label": "Debian 12 (Bookworm)", "value": "debian:12"},
    {"label": "Debian 13 (Trixie)", "value": "debian:13"},
    {"label": "CentOS 7", "value": "centos:7"},
    {"label": "CentOS 8", "value": "centos:8"},
    {"label": "Alpine Linux", "value": "alpine:latest"},
]

# Node Locations (Storage Pools)
NODE_LOCATIONS = [
    {"label": "USA - East", "value": "us-east-pool"},
    {"label": "USA - West", "value": "us-west-pool"},
    {"label": "Europe - Germany", "value": "eu-de-pool"},
    {"label": "Europe - France", "value": "eu-fr-pool"},
    {"label": "Asia - Singapore", "value": "asia-sg-pool"},
    {"label": "Asia - Japan", "value": "asia-jp-pool"},
    {"label": "Australia", "value": "au-syd-pool"},
    {"label": "Default Pool", "value": "default"},
]

# Node Categories for filtering
NODE_CATEGORIES = {
    "all": "All Nodes",
    "running": "Running Nodes",
    "stopped": "Stopped Nodes",
    "suspended": "Suspended Nodes",
    "whitelisted": "Whitelisted Nodes",
    "high_usage": "High Usage Nodes",
    "recent": "Recently Created Nodes",
    "inactive": "Inactive Nodes",
    "purge_protected": "Purge Protected Nodes",
}

# Purge System Default Settings
PURGE_DEFAULT_SETTINGS = {
    'min_age_days': 30,
    'max_inactive_days': 14,
    'protect_running': True,
    'protect_whitelisted': True,
    'protect_recent': True,
    'recent_days': 7,
    'dry_run': True,
    'notify_users': True,
    'backup_before_purge': False,
    'auto_purge_schedule': 'disabled',
    'max_nodes_per_user': 5,
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f'{BOT_NAME.lower()}_bot')

# Check if lxc is available
if not shutil.which("lxc"):
    logger.error("LXC command not found. Please install LXC/LXD.")
    raise SystemExit("LXC command not found.")

# Database setup
def get_db():
    conn = sqlite3.connect('vps.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    
    # Main VPS/Nodes table
    cur.execute('''CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        container_name TEXT UNIQUE NOT NULL,
        node_name TEXT NOT NULL,
        ram TEXT NOT NULL,
        cpu TEXT NOT NULL,
        storage TEXT NOT NULL,
        location TEXT DEFAULT 'default',
        os_version TEXT DEFAULT 'ubuntu:22.04',
        status TEXT DEFAULT 'stopped',
        suspended INTEGER DEFAULT 0,
        whitelisted INTEGER DEFAULT 0,
        purge_protected INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        last_updated TEXT,
        last_accessed TEXT,
        uptime TEXT DEFAULT '0',
        notes TEXT DEFAULT '',
        backup_path TEXT DEFAULT '',
        tags TEXT DEFAULT '[]'
    )''')
    
    # Node statistics table
    cur.execute('''CREATE TABLE IF NOT EXISTS node_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        container_name TEXT NOT NULL,
        cpu_usage REAL DEFAULT 0,
        memory_usage REAL DEFAULT 0,
        disk_usage TEXT DEFAULT '0/0 (0%)',
        network_rx TEXT DEFAULT '0',
        network_tx TEXT DEFAULT '0',
        processes INTEGER DEFAULT 0,
        recorded_at TEXT NOT NULL,
        FOREIGN KEY (container_name) REFERENCES nodes (container_name) ON DELETE CASCADE
    )''')
    
    # Node ports table
    cur.execute('''CREATE TABLE IF NOT EXISTS node_ports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        container_name TEXT NOT NULL,
        port INTEGER NOT NULL,
        protocol TEXT DEFAULT 'tcp',
        description TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (container_name) REFERENCES nodes (container_name) ON DELETE CASCADE
    )''')
    
    # Purge system tables
    cur.execute('''CREATE TABLE IF NOT EXISTS purge_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )''')
    
    cur.execute('''CREATE TABLE IF NOT EXISTS purge_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        purge_id TEXT NOT NULL,
        container_name TEXT NOT NULL,
        user_id TEXT NOT NULL,
        node_name TEXT NOT NULL,
        action TEXT NOT NULL,
        reason TEXT,
        age_days INTEGER,
        inactive_days INTEGER,
        created_at TEXT NOT NULL,
        FOREIGN KEY (container_name) REFERENCES nodes (container_name) ON DELETE CASCADE
    )''')
    
    cur.execute('''CREATE TABLE IF NOT EXISTS purge_protections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        container_name TEXT NOT NULL,
        protected_by TEXT NOT NULL,
        reason TEXT,
        expires_at TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (container_name) REFERENCES nodes (container_name) ON DELETE CASCADE
    )''')
    
    # Admins table
    cur.execute('''CREATE TABLE IF NOT EXISTS admins (
        user_id TEXT PRIMARY KEY
    )''')
    cur.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (str(MAIN_ADMIN_ID),))
    
    # Settings table
    cur.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )''')
    
    # Initialize default settings
    settings_init = [
        ('cpu_threshold', '90'),
        ('ram_threshold', '90'),
        ('max_nodes_per_user', '5'),
        ('default_location', 'default'),
        ('auto_backup', '1'),
        ('purge_system_active', '0'),
        ('purge_min_age_days', '30'),
        ('purge_max_inactive_days', '14'),
        ('purge_protect_running', '1'),
        ('purge_protect_whitelisted', '1'),
        ('purge_protect_recent', '1'),
        ('purge_recent_days', '7'),
        ('purge_dry_run', '1'),
        ('purge_notify_users', '1'),
        ('purge_backup_before', '0'),
        ('purge_auto_schedule', 'disabled'),
        ('purge_last_triggered', ''),
        ('purge_total_executions', '0'),
        ('purge_total_purged', '0'),
    ]
    
    for key, value in settings_init:
        cur.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, value))
    
    conn.commit()
    conn.close()

# Database helper functions
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

def get_all_nodes() -> List[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM nodes ORDER BY created_at DESC')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_user_nodes(user_id: str) -> List[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM nodes WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_node_by_name(container_name: str) -> Optional[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM nodes WHERE container_name = ?', (container_name,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def get_node_by_id(node_id: int) -> Optional[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM nodes WHERE id = ?', (node_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def add_node(user_id: str, container_name: str, node_name: str, ram: str, cpu: str, 
             storage: str, location: str, os_version: str) -> bool:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''INSERT INTO nodes 
                      (user_id, container_name, node_name, ram, cpu, storage, 
                       location, os_version, status, created_at, last_updated, last_accessed)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (user_id, container_name, node_name, ram, cpu, storage,
                    location, os_version, 'stopped', 
                    datetime.now().isoformat(), datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error adding node: {e}")
        return False

def update_node_status(container_name: str, status: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE nodes SET status = ?, last_updated = ?, last_accessed = ? WHERE container_name = ?',
                (status, datetime.now().isoformat(), datetime.now().isoformat(), container_name))
    conn.commit()
    conn.close()

def update_node_suspended(container_name: str, suspended: bool):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE nodes SET suspended = ?, last_updated = ? WHERE container_name = ?',
                (1 if suspended else 0, datetime.now().isoformat(), container_name))
    conn.commit()
    conn.close()

def update_node_whitelisted(container_name: str, whitelisted: bool):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE nodes SET whitelisted = ?, last_updated = ? WHERE container_name = ?',
                (1 if whitelisted else 0, datetime.now().isoformat(), container_name))
    conn.commit()
    conn.close()

def update_node_purge_protected(container_name: str, protected: bool):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE nodes SET purge_protected = ?, last_updated = ? WHERE container_name = ?',
                (1 if protected else 0, datetime.now().isoformat(), container_name))
    conn.commit()
    conn.close()

def delete_node(container_name: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM nodes WHERE container_name = ?', (container_name,))
    cur.execute('DELETE FROM node_stats WHERE container_name = ?', (container_name,))
    cur.execute('DELETE FROM node_ports WHERE container_name = ?', (container_name,))
    cur.execute('DELETE FROM purge_protections WHERE container_name = ?', (container_name,))
    conn.commit()
    conn.close()

def update_node_stats(container_name: str, cpu_usage: float, memory_usage: float, 
                      disk_usage: str, network_rx: str, network_tx: str, processes: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''INSERT INTO node_stats 
                  (container_name, cpu_usage, memory_usage, disk_usage, 
                   network_rx, network_tx, processes, recorded_at)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
               (container_name, cpu_usage, memory_usage, disk_usage,
                network_rx, network_tx, processes, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# Purge System Database Functions
def add_purge_history(purge_id: str, container_name: str, user_id: str, node_name: str, 
                     action: str, reason: str = None, age_days: int = None, inactive_days: int = None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''INSERT INTO purge_history 
                  (purge_id, container_name, user_id, node_name, action, reason, age_days, inactive_days, created_at)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
               (purge_id, container_name, user_id, node_name, action, reason, age_days, inactive_days, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_purge_history(limit: int = 50) -> List[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM purge_history ORDER BY created_at DESC LIMIT ?', (limit,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_purge_stats() -> Dict[str, Any]:
    conn = get_db()
    cur = conn.cursor()
    
    stats = {}
    
    # Total executions
    cur.execute('SELECT COUNT(DISTINCT purge_id) FROM purge_history')
    stats['total_executions'] = cur.fetchone()[0] or 0
    
    # Total purged
    cur.execute('SELECT COUNT(*) FROM purge_history WHERE action = "purged"')
    stats['total_purged'] = cur.fetchone()[0] or 0
    
    # Total protected
    cur.execute('SELECT COUNT(*) FROM purge_history WHERE action = "protected"')
    stats['total_protected'] = cur.fetchone()[0] or 0
    
    # Total skipped
    cur.execute('SELECT COUNT(*) FROM purge_history WHERE action = "skipped"')
    stats['total_skipped'] = cur.fetchone()[0] or 0
    
    # Last execution
    cur.execute('SELECT MAX(created_at) FROM purge_history')
    stats['last_execution'] = cur.fetchone()[0] or 'Never'
    
    # Node protection stats
    cur.execute('SELECT COUNT(*) FROM nodes WHERE purge_protected = 1')
    stats['protected_nodes'] = cur.fetchone()[0] or 0
    
    # Nodes eligible for purge
    all_nodes = get_all_nodes()
    now = datetime.now()
    eligible = 0
    
    for node in all_nodes:
        if not node['purge_protected']:
            created = datetime.fromisoformat(node['created_at'])
            age_days = (now - created).days
            min_age = int(get_setting('purge_min_age_days', 30))
            
            if age_days >= min_age:
                eligible += 1
    
    stats['eligible_nodes'] = eligible
    stats['total_nodes'] = len(all_nodes)
    
    conn.close()
    return stats

def add_purge_protection(container_name: str, protected_by: str, reason: str = None, expires_at: str = None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''INSERT INTO purge_protections 
                  (container_name, protected_by, reason, expires_at, created_at)
                  VALUES (?, ?, ?, ?, ?)''',
               (container_name, protected_by, reason, expires_at, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    
    # Also update node table
    update_node_purge_protected(container_name, True)

def remove_purge_protection(container_name: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM purge_protections WHERE container_name = ?', (container_name,))
    conn.commit()
    conn.close()
    
    # Also update node table
    update_node_purge_protected(container_name, False)

def get_purge_protection(container_name: str) -> Optional[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM purge_protections WHERE container_name = ? ORDER BY created_at DESC LIMIT 1', (container_name,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_protected_nodes() -> List[Dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT n.*, p.protected_by, p.reason, p.expires_at FROM nodes n JOIN purge_protections p ON n.container_name = p.container_name ORDER BY p.created_at DESC')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_admins() -> List[str]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT user_id FROM admins')
    rows = cur.fetchall()
    conn.close()
    return [row['user_id'] for row in rows]

def add_admin(user_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (user_id,))
    conn.commit()
    conn.close()

def remove_admin(user_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM admins WHERE user_id = ? AND user_id != ?', 
                (user_id, MAIN_ADMIN_ID))
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Load admin data
admin_data = {'admins': get_admins()}

# Global settings
CPU_THRESHOLD = int(get_setting('cpu_threshold', 90))
RAM_THRESHOLD = int(get_setting('ram_threshold', 90))
MAX_NODES_PER_USER = int(get_setting('max_nodes_per_user', 5))
PURGE_SYSTEM_ACTIVE = get_setting('purge_system_active', '0') == '1'

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# Helper functions
def truncate_text(text, max_length=1024):
    if not text:
        return text
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def create_embed(title, description="", color=0x1a1a1a):
    embed = discord.Embed(
        title=truncate_text(f"⭐ {BOT_NAME} - {title}", 256),
        description=truncate_text(description, 4096),
        color=color
    )
    embed.set_footer(text=f"{BOT_NAME} Node Manager • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return embed

def create_success_embed(title, description=""):
    return create_embed(title, description, color=0x00ff88)

def create_error_embed(title, description=""):
    return create_embed(title, description, color=0xff3366)

def create_info_embed(title, description=""):
    return create_embed(title, description, color=0x00ccff)

def create_warning_embed(title, description=""):
    return create_embed(title, description, color=0xffaa00)

def add_field(embed, name, value, inline=False):
    embed.add_field(
        name=truncate_text(f"▸ {name}", 256),
        value=truncate_text(value, 1024),
        inline=inline
    )
    return embed

# Admin checks
def is_admin():
    async def predicate(ctx):
        user_id = str(ctx.author.id)
        if user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", []):
            return True
        raise commands.CheckFailure("You need admin permissions to use this command.")
    return commands.check(predicate)

def is_main_admin():
    async def predicate(ctx):
        if str(ctx.author.id) == str(MAIN_ADMIN_ID):
            return True
        raise commands.CheckFailure("Only the main admin can use this command.")
    return commands.check(predicate)

# LXC execution
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
            error = stderr.decode().strip() if stderr else "Command failed"
            raise Exception(error)
        
        return stdout.decode().strip() if stdout else True
    except Exception as e:
        logger.error(f"LXC Error: {command} - {str(e)}")
        raise

# Container stats functions
async def get_container_status(container_name):
    try:
        result = await execute_lxc(f"lxc info {container_name}")
        for line in result.splitlines():
            if line.startswith("Status: "):
                return line.split(": ", 1)[1].strip().lower()
        return "unknown"
    except:
        return "unknown"

async def get_container_cpu_pct(container_name):
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
                idle = float(parts[7])
                return 100.0 - idle
        return 0.0
    except:
        return 0.0

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
    except:
        return "Unknown"

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
    except:
        return "Unknown"

async def get_container_uptime(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "uptime", "-p",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        return stdout.decode().strip() if stdout else "Unknown"
    except:
        return "Unknown"

async def get_container_network_stats(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "cat", "/proc/net/dev",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = stdout.decode().splitlines()
        rx_total = 0
        tx_total = 0
        
        for line in lines[2:]:  # Skip header lines
            if ':' in line:
                parts = line.split()
                if len(parts) >= 10:
                    rx_total += int(parts[1])
                    tx_total += int(parts[9])
        
        # Convert to human readable format
        def format_bytes(bytes_num):
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes_num < 1024.0:
                    return f"{bytes_num:.1f} {unit}"
                bytes_num /= 1024.0
            return f"{bytes_num:.1f} PB"
        
        return format_bytes(rx_total), format_bytes(tx_total)
    except:
        return "0 B", "0 B"

async def get_container_processes(container_name):
    try:
        proc = await asyncio.create_subprocess_exec(
            "lxc", "exec", container_name, "--", "ps", "aux",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = stdout.decode().strip().split('\n')
        return max(0, len(lines) - 1)  # Subtract header
    except:
        return 0

# Purge System Helper Functions
def is_node_eligible_for_purge(node: Dict) -> Tuple[bool, str]:
    """Check if a node is eligible for purge with reason"""
    now = datetime.now()
    
    # Check if purge protected
    if node['purge_protected']:
        return False, "Purge protected"
    
    # Check if whitelisted (if setting enabled)
    if node['whitelisted'] and get_setting('purge_protect_whitelisted', '1') == '1':
        return False, "Whitelisted"
    
    # Check if running (if setting enabled)
    if node['status'] == 'running' and get_setting('purge_protect_running', '1') == '1':
        return False, "Running"
    
    # Check if suspended
    if node['suspended']:
        return False, "Suspended"
    
    # Check age
    created = datetime.fromisoformat(node['created_at'])
    age_days = (now - created).days
    min_age = int(get_setting('purge_min_age_days', 30))
    
    if age_days < min_age:
        # Check if recent nodes are protected
        if get_setting('purge_protect_recent', '1') == '1':
            recent_days = int(get_setting('purge_recent_days', 7))
            if age_days < recent_days:
                return False, f"Recent ({age_days}d < {recent_days}d)"
        return False, f"Too new ({age_days}d < {min_age}d)"
    
    # Check inactivity
    if 'last_accessed' in node and node['last_accessed']:
        last_accessed = datetime.fromisoformat(node['last_accessed'])
        inactive_days = (now - last_accessed).days
        max_inactive = int(get_setting('purge_max_inactive_days', 14))
        
        if inactive_days < max_inactive:
            return False, f"Active ({inactive_days}d < {max_inactive}d)"
    
    return True, f"Eligible (Age: {age_days}d)"

async def perform_purge_check(dry_run: bool = True) -> Dict[str, Any]:
    """Perform a purge check and return results"""
    all_nodes = get_all_nodes()
    now = datetime.now()
    purge_id = f"purge-{int(time.time())}"
    
    results = {
        'purge_id': purge_id,
        'timestamp': now.isoformat(),
        'dry_run': dry_run,
        'total_nodes': len(all_nodes),
        'eligible': [],
        'protected': [],
        'skipped': [],
        'purged': [],
        'errors': []
    }
    
    # Update purge stats
    set_setting('purge_last_triggered', now.isoformat())
    total_executions = int(get_setting('purge_total_executions', '0'))
    set_setting('purge_total_executions', str(total_executions + 1))
    
    for node in all_nodes:
        try:
            eligible, reason = is_node_eligible_for_purge(node)
            
            if not eligible:
                # Add to skipped
                results['skipped'].append({
                    'container': node['container_name'],
                    'node_name': node['node_name'],
                    'user_id': node['user_id'],
                    'reason': reason
                })
                
                # Add to history
                add_purge_history(purge_id, node['container_name'], node['user_id'], 
                                node['node_name'], 'skipped', reason)
                
                continue
            
            # Node is eligible
            results['eligible'].append({
                'container': node['container_name'],
                'node_name': node['node_name'],
                'user_id': node['user_id'],
                'reason': reason
            })
            
            if dry_run:
                # Dry run - don't actually delete
                results['protected'].append({
                    'container': node['container_name'],
                    'node_name': node['node_name'],
                    'user_id': node['user_id'],
                    'reason': 'Dry run'
                })
                
                add_purge_history(purge_id, node['container_name'], node['user_id'], 
                                node['node_name'], 'protected', 'Dry run mode')
            else:
                # Actually purge the node
                try:
                    # Try to get owner for notification
                    owner = None
                    try:
                        owner = await bot.fetch_user(int(node['user_id']))
                    except:
                        pass
                    
                    # Stop container if running
                    if node['status'] == 'running':
                        await execute_lxc(f"lxc stop {node['container_name']} --force")
                    
                    # Delete container
                    await execute_lxc(f"lxc delete {node['container_name']} --force")
                    
                    # Remove from database
                    delete_node(node['container_name'])
                    
                    # Add to purged list
                    results['purged'].append({
                        'container': node['container_name'],
                        'node_name': node['node_name'],
                        'user_id': node['user_id']
                    })
                    
                    # Add to history
                    add_purge_history(purge_id, node['container_name'], node['user_id'], 
                                    node['node_name'], 'purged', reason)
                    
                    # Update total purged count
                    total_purged = int(get_setting('purge_total_purged', '0'))
                    set_setting('purge_total_purged', str(total_purged + 1))
                    
                    # Notify owner if setting enabled
                    if get_setting('purge_notify_users', '1') == '1' and owner:
                        try:
                            embed = create_warning_embed(
                                "🚨 Node Purged Automatically",
                                f"Your node **{node['node_name']}** has been automatically purged by the system.\n\n"
                                f"**Container:** `{node['container_name']}`\n"
                                f"**Reason:** {reason}\n"
                                f"**Purge ID:** {purge_id}\n\n"
                                f"**Settings:**\n"
                                f"• Minimum Age: {get_setting('purge_min_age_days')} days\n"
                                f"• Max Inactivity: {get_setting('purge_max_inactive_days')} days\n\n"
                                f"To prevent future purges, you can:\n"
                                f"1. Keep your node active\n"
                                f"2. Ask an admin for purge protection\n"
                                f"3. Whitelist your node (admin only)"
                            )
                            await owner.send(embed=embed)
                        except:
                            pass
                    
                except Exception as e:
                    results['errors'].append({
                        'container': node['container_name'],
                        'error': str(e)
                    })
                    logger.error(f"Purge failed for {node['container_name']}: {e}")
        
        except Exception as e:
            results['errors'].append({
                'container': node.get('container_name', 'unknown'),
                'error': str(e)
            })
            logger.error(f"Error processing node for purge: {e}")
    
    return results

# ======================
# NODE SYSTEM COMMANDS
# ======================

@bot.command(name='create-node', aliases=['add-node'])
@is_admin()
async def create_node_cmd(ctx, ram: int, cpu: int, disk: int, location: str, node_name: str, user: discord.Member = None):
    """
    Create a new node/VPS
    
    Usage: !create-node <ram_gb> <cpu_cores> <disk_gb> <location> <node_name> [@user]
    Example: !create-node 4 2 50 usa-east MyServer @user
    
    Locations: usa-east, usa-west, europe, asia, default
    """
    # If no user specified, use command author
    if user is None:
        user = ctx.author
    
    # Validate parameters
    if ram < 1 or ram > 128:
        await ctx.send(embed=create_error_embed(
            "Invalid RAM", 
            "RAM must be between 1GB and 128GB"
        ))
        return
    
    if cpu < 1 or cpu > 32:
        await ctx.send(embed=create_error_embed(
            "Invalid CPU", 
            "CPU cores must be between 1 and 32"
        ))
        return
    
    if disk < 10 or disk > 1000:
        await ctx.send(embed=create_error_embed(
            "Invalid Storage", 
            "Storage must be between 10GB and 1000GB"
        ))
        return
    
    # Validate location
    valid_locations = [loc["value"] for loc in NODE_LOCATIONS]
    if location not in valid_locations:
        location_list = "\n".join([f"• `{loc['value']}` - {loc['label']}" for loc in NODE_LOCATIONS])
        await ctx.send(embed=create_error_embed(
            "Invalid Location", 
            f"Available locations:\n{location_list}"
        ))
        return
    
    # Check if user can create more nodes
    user_nodes = get_user_nodes(str(user.id))
    if len(user_nodes) >= MAX_NODES_PER_USER:
        await ctx.send(embed=create_error_embed(
            "Node Limit Reached", 
            f"{user.mention} already has {len(user_nodes)} nodes (max: {MAX_NODES_PER_USER})"
        ))
        return
    
    # Create embed with node details
    embed = create_info_embed(
        "Create New Node",
        f"**Creating node for:** {user.mention}\n"
        f"**Node Name:** {node_name}\n"
        f"**Specifications:** {ram}GB RAM, {cpu} CPU cores, {disk}GB Storage\n"
        f"**Location:** {location}\n\n"
        f"Please select an operating system:"
    )
    
    # Create OS selection view
    class OSSelectView(discord.ui.View):
        def __init__(self, user, ctx, ram, cpu, disk, location, node_name):
            super().__init__(timeout=300)
            self.user = user
            self.ctx = ctx
            self.ram = ram
            self.cpu = cpu
            self.disk = disk
            self.location = location
            self.node_name = node_name
            
            # OS selection dropdown
            self.select = discord.ui.Select(
                placeholder="Select Operating System",
                options=[discord.SelectOption(label=o["label"], value=o["value"]) for o in OS_OPTIONS]
            )
            self.select.callback = self.select_os
            self.add_item(self.select)
        
        async def select_os(self, interaction: discord.Interaction):
            if str(interaction.user.id) != str(self.ctx.author.id):
                await interaction.response.send_message(
                    embed=create_error_embed("Access Denied", "Only the command author can select."),
                    ephemeral=True
                )
                return
            
            os_version = self.select.values[0]
            await interaction.response.defer()
            
            # Disable the select
            for item in self.children:
                item.disabled = True
            await interaction.edit_original_response(view=self)
            
            # Start node creation
            await self.create_node(interaction, os_version)
        
        async def create_node(self, interaction: discord.Interaction, os_version: str):
            user_id = str(self.user.id)
            
            # Generate container name
            timestamp = int(time.time())
            container_name = f"{BOT_NAME.lower()}-{self.user.name[:8].lower()}-{timestamp}"
            
            # Show creation progress
            progress_embed = create_info_embed(
                "Creating Node",
                f"**Deploying Node:** {self.node_name}\n"
                f"**OS:** {os_version}\n"
                f"**Location:** {self.location}\n\n"
                f"🔄 Initializing container..."
            )
            progress_msg = await interaction.followup.send(embed=progress_embed)
            
            try:
                # Step 1: Create container
                progress_embed.description = f"**Deploying Node:** {self.node_name}\n**Status:** Creating container..."
                await progress_msg.edit(embed=progress_embed)
                
                await execute_lxc(f"lxc init {os_version} {container_name} -s {self.location}")
                
                # Step 2: Configure resources
                progress_embed.description = f"**Deploying Node:** {self.node_name}\n**Status:** Configuring resources..."
                await progress_msg.edit(embed=progress_embed)
                
                ram_mb = self.ram * 1024
                await execute_lxc(f"lxc config set {container_name} limits.memory {ram_mb}MB")
                await execute_lxc(f"lxc config set {container_name} limits.cpu {self.cpu}")
                await execute_lxc(f"lxc config device set {container_name} root size={self.disk}GB")
                
                # Step 3: Apply configurations
                progress_embed.description = f"**Deploying Node:** {self.node_name}\n**Status:** Applying configurations..."
                await progress_msg.edit(embed=progress_embed)
                
                await execute_lxc(f"lxc config set {container_name} security.nesting true")
                await execute_lxc(f"lxc config set {container_name} security.privileged true")
                
                # Step 4: Start container
                progress_embed.description = f"**Deploying Node:** {self.node_name}\n**Status:** Starting node..."
                await progress_msg.edit(embed=progress_embed)
                
                await execute_lxc(f"lxc start {container_name}")
                
                # Step 5: Wait for boot
                progress_embed.description = f"**Deploying Node:** {self.node_name}\n**Status:** Finalizing setup..."
                await progress_msg.edit(embed=progress_embed)
                
                await asyncio.sleep(10)
                
                # Add to database
                add_node(user_id, container_name, self.node_name, f"{self.ram}GB", 
                        str(self.cpu), f"{self.disk}GB", self.location, os_version)
                
                # Success embed
                success_embed = create_success_embed(
                    "✅ Node Created Successfully",
                    f"**Node Name:** {self.node_name}\n"
                    f"**Container:** `{container_name}`\n"
                    f"**Status:** 🟢 Running\n"
                    f"**Location:** {self.location}\n"
                    f"**OS:** {os_version}"
                )
                
                add_field(success_embed, "Specifications", 
                         f"• RAM: {self.ram}GB\n• CPU: {self.cpu} cores\n• Storage: {self.disk}GB", 
                         False)
                
                add_field(success_embed, "Access", 
                         f"• Use `{PREFIX}node-status {container_name}` to check status\n"
                         f"• Use `{PREFIX}manage-node {container_name}` to manage", 
                         False)
                
                add_field(success_embed, "Purge System", 
                         "⚠️ **Note:** This node is NOT purge protected by default.\n"
                         f"Use `{PREFIX}protect-node {container_name}` to protect it.", 
                         False)
                
                await progress_msg.edit(embed=success_embed)
                
                # Send DM to user
                try:
                    dm_embed = create_info_embed(
                        "🚀 New Node Created",
                        f"Your node **{self.node_name}** has been successfully created!"
                    )
                    add_field(dm_embed, "Details", 
                             f"**Container:** `{container_name}`\n"
                             f"**RAM:** {self.ram}GB\n**CPU:** {self.cpu} cores\n"
                             f"**Storage:** {self.disk}GB\n**Location:** {self.location}\n**OS:** {os_version}", 
                             False)
                    add_field(dm_embed, "Management", 
                             f"• Check status: `{PREFIX}node-status {container_name}`\n"
                             f"• Manage: `{PREFIX}manage-node {container_name}`\n"
                             f"• Protect from purge: `{PREFIX}protect-node {container_name}`", 
                             False)
                    await self.user.send(embed=dm_embed)
                except:
                    pass
                
            except Exception as e:
                error_embed = create_error_embed(
                    "❌ Node Creation Failed",
                    f"An error occurred while creating the node:\n```{str(e)}```"
                )
                await progress_msg.edit(embed=error_embed)
                logger.error(f"Node creation failed: {e}")
    
    view = OSSelectView(user, ctx, ram, cpu, disk, location, node_name)
    await ctx.send(embed=embed, view=view)

@bot.command(name='remove-node', aliases=['delete-node'])
@is_admin()
async def remove_node_cmd(ctx, container_name: str, *, reason: str = "No reason provided"):
    """
    Remove a node/VPS
    
    Usage: !remove-node <container_name> [reason]
    Example: !remove-node voltarisvm-user-123456 "Server migration"
    """
    # Find node
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed(
            "Node Not Found", 
            f"No node found with container name: `{container_name}`"
        ))
        return
    
    # Get owner info
    try:
        owner = await bot.fetch_user(int(node['user_id']))
        owner_info = f"{owner.mention} ({owner.name})"
    except:
        owner_info = f"User ID: {node['user_id']}"
    
    # Confirmation embed
    embed = create_warning_embed(
        "⚠️ Confirm Node Removal",
        f"**Node:** {node['node_name']}\n"
        f"**Container:** `{container_name}`\n"
        f"**Owner:** {owner_info}\n"
        f"**Reason:** {reason}\n\n"
        f"**This action will:**\n"
        f"• Delete the container and all data\n"
        f"• Remove all associated configurations\n"
        f"• Free up allocated resources\n\n"
        f"**This action cannot be undone!**"
    )
    
    class ConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
        
        @discord.ui.button(label="✅ Confirm Delete", style=discord.ButtonStyle.danger)
        async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
            if str(interaction.user.id) != str(ctx.author.id):
                await interaction.response.send_message(
                    embed=create_error_embed("Access Denied", "Only the command author can confirm."),
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            
            # Show progress
            progress_embed = create_info_embed("Removing Node", "Deleting container...")
            await interaction.followup.send(embed=progress_embed)
            
            try:
                # Stop container if running
                if node['status'] == 'running':
                    await execute_lxc(f"lxc stop {container_name} --force")
                
                # Delete container
                await execute_lxc(f"lxc delete {container_name} --force")
                
                # Remove from database
                delete_node(container_name)
                
                # Success message
                success_embed = create_success_embed(
                    "✅ Node Removed Successfully",
                    f"**Node:** {node['node_name']}\n"
                    f"**Container:** `{container_name}`\n"
                    f"**Reason:** {reason}"
                )
                
                # Notify owner if possible
                try:
                    owner_user = await bot.fetch_user(int(node['user_id']))
                    notify_embed = create_warning_embed(
                        "🚨 Node Removed",
                        f"Your node **{node['node_name']}** (`{container_name}`) has been removed by an admin.\n"
                        f"**Reason:** {reason}"
                    )
                    await owner_user.send(embed=notify_embed)
                except:
                    pass
                
                await interaction.edit_original_response(embed=success_embed, view=None)
                
            except Exception as e:
                error_embed = create_error_embed(
                    "❌ Removal Failed",
                    f"Failed to remove node:\n```{str(e)}```"
                )
                await interaction.edit_original_response(embed=error_embed, view=None)
        
        @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
        async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
            if str(interaction.user.id) != str(ctx.author.id):
                await interaction.response.send_message(
                    embed=create_error_embed("Access Denied", "Only the command author can cancel."),
                    ephemeral=True
                )
                return
            
            embed = create_info_embed("Cancelled", "Node removal has been cancelled.")
            await interaction.response.edit_message(embed=embed, view=None)
    
    await ctx.send(embed=embed, view=ConfirmView())

@bot.command(name='reedit-node', aliases=['edit-node', 'update-node'])
@is_admin()
async def reedit_node_cmd(ctx, container_name: str, field: str, *, new_value: str):
    """
    Edit node properties
    
    Usage: !reedit-node <container_name> <field> <new_value>
    Fields: name, ram, cpu, storage, location, notes, tags
    Example: !reedit-node voltarisvm-user-123456 name "Production Server"
    """
    # Find node
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed(
            "Node Not Found", 
            f"No node found with container name: `{container_name}`"
        ))
        return
    
    field = field.lower()
    valid_fields = ['name', 'ram', 'cpu', 'storage', 'location', 'notes', 'tags']
    
    if field not in valid_fields:
        await ctx.send(embed=create_error_embed(
            "Invalid Field", 
            f"Valid fields: {', '.join(valid_fields)}"
        ))
        return
    
    # Update database
    conn = get_db()
    cur = conn.cursor()
    
    if field == 'name':
        cur.execute('UPDATE nodes SET node_name = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'ram':
        # Validate RAM format
        if not new_value.endswith('GB'):
            new_value = f"{new_value}GB"
        cur.execute('UPDATE nodes SET ram = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'cpu':
        cur.execute('UPDATE nodes SET cpu = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'storage':
        # Validate storage format
        if not new_value.endswith('GB'):
            new_value = f"{new_value}GB"
        cur.execute('UPDATE nodes SET storage = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'location':
        cur.execute('UPDATE nodes SET location = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'notes':
        cur.execute('UPDATE nodes SET notes = ?, last_updated = ? WHERE container_name = ?',
                   (new_value, datetime.now().isoformat(), container_name))
    elif field == 'tags':
        # Validate tags format
        try:
            tags = json.loads(new_value) if new_value.startswith('[') else new_value.split(',')
            tags_json = json.dumps([tag.strip() for tag in tags])
            cur.execute('UPDATE nodes SET tags = ?, last_updated = ? WHERE container_name = ?',
                       (tags_json, datetime.now().isoformat(), container_name))
        except:
            await ctx.send(embed=create_error_embed("Invalid Tags", "Tags must be JSON array or comma-separated list"))
            conn.close()
            return
    
    conn.commit()
    conn.close()
    
    # Get updated node
    updated_node = get_node_by_name(container_name)
    
    # Success embed
    embed = create_success_embed(
        "✅ Node Updated",
        f"**Node:** {updated_node['node_name']}\n"
        f"**Container:** `{container_name}`\n"
        f"**Updated Field:** {field}\n"
        f"**New Value:** {new_value}"
    )
    
    add_field(embed, "Current Configuration", 
             f"• Name: {updated_node['node_name']}\n"
             f"• RAM: {updated_node['ram']}\n"
             f"• CPU: {updated_node['cpu']} cores\n"
             f"• Storage: {updated_node['storage']}\n"
             f"• Location: {updated_node['location']}", 
             False)
    
    await ctx.send(embed=embed)

@bot.command(name='node-status', aliases=['status-node', 'check-node'])
async def node_status_cmd(ctx, container_name: str = None):
    """
    Check node status and statistics
    
    Usage: !node-status [container_name]
    Example: !node-status voltarisvm-user-123456
    """
    if container_name is None:
        # Show status for all user's nodes
        user_id = str(ctx.author.id)
        user_nodes = get_user_nodes(user_id)
        
        if not user_nodes:
            embed = create_info_embed(
                "Your Nodes",
                "You don't have any nodes yet.\n"
                f"Use `{PREFIX}create-node` to create one (admin only)."
            )
            await ctx.send(embed=embed)
            return
        
        embed = create_info_embed(
            f"Your Nodes ({len(user_nodes)})",
            f"Showing all nodes for {ctx.author.mention}"
        )
        
        for node in user_nodes:
            status_emoji = "🟢" if node['status'] == 'running' else "🔴"
            if node['suspended']:
                status_emoji = "⛔"
            if node['whitelisted']:
                status_emoji = "⭐"
            if node['purge_protected']:
                status_emoji = "🛡️"
            
            node_info = (
                f"{status_emoji} **{node['node_name']}**\n"
                f"`{node['container_name']}`\n"
                f"• Specs: {node['ram']} RAM, {node['cpu']} CPU, {node['storage']} Storage\n"
                f"• Location: {node['location']}\n"
                f"• Status: {node['status'].upper()}"
            )
            
            if node['suspended']:
                node_info += " (SUSPENDED)"
            if node['whitelisted']:
                node_info += " (WHITELISTED)"
            if node['purge_protected']:
                node_info += " (PURGE PROTECTED)"
            
            add_field(embed, "", node_info, False)
        
        add_field(embed, "Actions", 
                 f"• Check specific node: `{PREFIX}node-status <container_name>`\n"
                 f"• Get detailed stats: `{PREFIX}node <container_name>`", 
                 False)
        
        await ctx.send(embed=embed)
        return
    
    # Specific node status
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed(
            "Node Not Found", 
            f"No node found with container name: `{container_name}`"
        ))
        return
    
    # Check if user has access
    user_id = str(ctx.author.id)
    is_admin_user = user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", [])
    
    if node['user_id'] != user_id and not is_admin_user:
        await ctx.send(embed=create_error_embed(
            "Access Denied", 
            "You don't have permission to view this node."
        ))
        return
    
    # Show loading
    loading_embed = create_info_embed(
        "Checking Node Status",
        f"Gathering statistics for `{container_name}`..."
    )
    msg = await ctx.send(embed=loading_embed)
    
    try:
        # Get live stats
        status = await get_container_status(container_name)
        cpu_usage = await get_container_cpu_pct(container_name)
        memory_usage = await get_container_memory(container_name)
        disk_usage = await get_container_disk(container_name)
        uptime = await get_container_uptime(container_name)
        network_rx, network_tx = await get_container_network_stats(container_name)
        processes = await get_container_processes(container_name)
        
        # Update database status
        update_node_status(container_name, status)
        
        # Get owner info
        try:
            owner = await bot.fetch_user(int(node['user_id']))
            owner_info = f"{owner.mention} ({owner.name})"
        except:
            owner_info = f"User ID: {node['user_id']}"
        
        # Create status embed
        status_color = 0x00ff88 if status == 'running' else 0xff3366
        if node['suspended']:
            status_color = 0xffaa00
        
        embed = create_embed(
            f"📊 Node Status: {node['node_name']}",
            f"**Container:** `{container_name}`\n**Owner:** {owner_info}",
            status_color
        )
        
        # Basic info
        add_field(embed, "📋 Basic Information",
                 f"• **Name:** {node['node_name']}\n"
                 f"• **Status:** {status.upper()}\n"
                 f"• **Uptime:** {uptime}\n"
                 f"• **Location:** {node['location']}\n"
                 f"• **OS:** {node['os_version']}\n"
                 f"• **Created:** {node['created_at'][:10]}",
                 False)
        
        # Specifications
        add_field(embed, "⚙️ Specifications",
                 f"• **RAM:** {node['ram']}\n"
                 f"• **CPU:** {node['cpu']} cores\n"
                 f"• **Storage:** {node['storage']}",
                 True)
        
        # Live Statistics
        add_field(embed, "📈 Live Statistics",
                 f"• **CPU Usage:** {cpu_usage:.1f}%\n"
                 f"• **Memory:** {memory_usage}\n"
                 f"• **Disk:** {disk_usage}\n"
                 f"• **Processes:** {processes}",
                 True)
        
        # Network
        add_field(embed, "🌐 Network",
                 f"• **RX:** {network_rx}\n"
                 f"• **TX:** {network_tx}",
                 True)
        
        # Protection flags
        flags = []
        if node['suspended']:
            flags.append("⛔ **SUSPENDED**")
        if node['whitelisted']:
            flags.append("⭐ **WHITELISTED**")
        if node['purge_protected']:
            protection = get_purge_protection(container_name)
            if protection:
                expires = f" (Expires: {protection['expires_at'][:10]})" if protection['expires_at'] else ""
                flags.append(f"🛡️ **PURGE PROTECTED**{expires}")
            else:
                flags.append("🛡️ **PURGE PROTECTED**")
        
        if flags:
            add_field(embed, "🚩 Protection Flags", "\n".join(flags), False)
        
        # Purge eligibility check
        if PURGE_SYSTEM_ACTIVE and not node['purge_protected']:
            eligible, reason = is_node_eligible_for_purge(node)
            if eligible:
                add_field(embed, "⚠️ Purge Warning", 
                         f"This node is **eligible for automatic purge**!\n"
                         f"**Reason:** {reason}\n\n"
                         f"To protect it, use: `{PREFIX}protect-node {container_name}`", 
                         False)
        
        # Notes
        if node['notes']:
            add_field(embed, "📝 Notes", node['notes'], False)
        
        # Actions
        actions = []
        if is_admin_user or node['user_id'] == user_id:
            if status == 'running':
                actions.append(f"`{PREFIX}stop-node {container_name}` - Stop node")
            else:
                actions.append(f"`{PREFIX}start-node {container_name}` - Start node")
            
            if not node['suspended']:
                actions.append(f"`{PREFIX}suspend-node {container_name}` - Suspend node")
            else:
                actions.append(f"`{PREFIX}unsuspend-node {container_name}` - Unsuspend node")
            
            if not node['purge_protected']:
                actions.append(f"`{PREFIX}protect-node {container_name}` - Protect from purge")
            else:
                actions.append(f"`{PREFIX}unprotect-node {container_name}` - Remove protection")
            
            actions.append(f"`{PREFIX}manage-node {container_name}` - Manage node")
        
        if actions:
            add_field(embed, "🛠️ Actions", "\n".join(actions), False)
        
        await msg.edit(embed=embed)
        
    except Exception as e:
        error_embed = create_error_embed(
            "Status Check Failed",
            f"Failed to get node status:\n```{str(e)}```"
        )
        await msg.edit(embed=error_embed)

@bot.command(name='list-nodes', aliases=['nodes-list', 'all-nodes'])
async def list_nodes_cmd(ctx, category: str = "all", page: int = 1):
    """
    List all nodes with filtering options
    
    Usage: !list-nodes [category] [page]
    Categories: all, running, stopped, suspended, whitelisted, high_usage, recent, inactive, purge_protected
    Example: !list-nodes running 2
    """
    # Validate category
    if category.lower() not in NODE_CATEGORIES:
        valid_cats = ", ".join([f"`{cat}`" for cat in NODE_CATEGORIES.keys()])
        embed = create_error_embed(
            "Invalid Category",
            f"Available categories: {valid_cats}"
        )
        await ctx.send(embed=embed)
        return
    
    # Show loading
    loading_embed = create_info_embed(
        "Loading Nodes",
        f"Gathering {NODE_CATEGORIES[category.lower()]}..."
    )
    msg = await ctx.send(embed=loading_embed)
    
    # Get all nodes
    all_nodes = get_all_nodes()
    
    # Filter nodes by category
    filtered_nodes = []
    now = datetime.now()
    
    for node in all_nodes:
        if category.lower() == "all":
            filtered_nodes.append(node)
        elif category.lower() == "running":
            if node['status'] == 'running' and not node['suspended']:
                filtered_nodes.append(node)
        elif category.lower() == "stopped":
            if node['status'] == 'stopped' and not node['suspended']:
                filtered_nodes.append(node)
        elif category.lower() == "suspended":
            if node['suspended']:
                filtered_nodes.append(node)
        elif category.lower() == "whitelisted":
            if node['whitelisted']:
                filtered_nodes.append(node)
        elif category.lower() == "purge_protected":
            if node['purge_protected']:
                filtered_nodes.append(node)
        elif category.lower() == "recent":
            created = datetime.fromisoformat(node['created_at'])
            if (now - created).days <= 7:
                filtered_nodes.append(node)
        elif category.lower() == "inactive":
            if 'last_accessed' in node and node['last_accessed']:
                last_accessed = datetime.fromisoformat(node['last_accessed'])
                inactive_days = (now - last_accessed).days
                max_inactive = int(get_setting('purge_max_inactive_days', 14))
                if inactive_days >= max_inactive:
                    filtered_nodes.append(node)
        elif category.lower() == "high_usage":
            # This would require live stats, simplified version
            if node['status'] == 'running':
                filtered_nodes.append(node)
    
    if not filtered_nodes:
        embed = create_info_embed(
            f"{NODE_CATEGORIES[category.lower()]}",
            "No nodes found in this category."
        )
        await msg.edit(embed=embed)
        return
    
    # Pagination
    nodes_per_page = 8
    total_pages = (len(filtered_nodes) + nodes_per_page - 1) // nodes_per_page
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * nodes_per_page
    end_idx = min(start_idx + nodes_per_page, len(filtered_nodes))
    page_nodes = filtered_nodes[start_idx:end_idx]
    
    # Create embed
    embed = create_embed(
        f"📋 {NODE_CATEGORIES[category.lower()]}",
        f"**Total Nodes:** {len(filtered_nodes)} | **Page {page}/{total_pages}**\n"
        f"*Showing {start_idx + 1}-{end_idx} of {len(filtered_nodes)} nodes*"
    )
    
    # Add node entries
    for i, node in enumerate(page_nodes, start=start_idx + 1):
        # Status emoji
        if node['suspended']:
            status_emoji = "⛔"
        elif node['status'] == 'running':
            status_emoji = "🟢"
        elif node['status'] == 'stopped':
            status_emoji = "🔴"
        else:
            status_emoji = "🟡"
        
        if node['whitelisted']:
            status_emoji += "⭐"
        if node['purge_protected']:
            status_emoji += "🛡️"
        
        # Get owner name
        try:
            owner = await bot.fetch_user(int(node['user_id']))
            owner_name = owner.name[:12]
        except:
            owner_name = f"User:{node['user_id'][:8]}"
        
        # Calculate age
        created = datetime.fromisoformat(node['created_at'])
        age_days = (now - created).days
        age_text = f"{age_days}d" if age_days > 0 else "Today"
        
        # Node info
        node_info = (
            f"{status_emoji} **{node['node_name']}**\n"
            f"`{node['container_name'][:20]}...`\n"
            f"• 👤 {owner_name}\n"
            f"• ⚙️ {node['ram']} | {node['cpu']}CPU | {node['storage']}\n"
            f"• 📍 {node['location']}\n"
            f"• 🕒 {age_text} | {node['status'].upper()}"
        )
        
        add_field(embed, f"Node #{i}", node_info, False)
    
    # Add statistics
    total_nodes = len(all_nodes)
    running_nodes = len([n for n in all_nodes if n['status'] == 'running' and not n['suspended']])
    suspended_nodes = len([n for n in all_nodes if n['suspended']])
    whitelisted_nodes = len([n for n in all_nodes if n['whitelisted']])
    protected_nodes = len([n for n in all_nodes if n['purge_protected']])
    
    stats_text = (
        f"• **Total:** {total_nodes} nodes\n"
        f"• **Running:** {running_nodes} nodes\n"
        f"• **Suspended:** {suspended_nodes} nodes\n"
        f"• **Whitelisted:** {whitelisted_nodes} nodes\n"
        f"• **Purge Protected:** {protected_nodes} nodes"
    )
    
    add_field(embed, "📊 System Statistics", stats_text, False)
    
    # Add purge system status if active
    if PURGE_SYSTEM_ACTIVE:
        purge_stats = get_purge_stats()
        purge_text = (
            f"• **System:** 🟢 Active\n"
            f"• **Last Run:** {purge_stats['last_execution'][:10] if purge_stats['last_execution'] != 'Never' else 'Never'}\n"
            f"• **Total Purged:** {purge_stats['total_purged']} nodes\n"
            f"• **Eligible:** {purge_stats['eligible_nodes']} nodes"
        )
        add_field(embed, "🧹 Purge System", purge_text, False)
    
    # Add navigation info
    if total_pages > 1:
        add_field(embed, "📖 Navigation", 
                 f"Use `{PREFIX}list-nodes {category} <page>` to navigate\n"
                 f"Example: `{PREFIX}list-nodes {category} {min(page + 1, total_pages)}`", 
                 False)
    
    await msg.edit(embed=embed)

# ======================
# PURGE SYSTEM COMMANDS
# ======================

@bot.command(name='purgesystem', aliases=['purge-system'])
@is_admin()
async def purge_system_cmd(ctx, action: str = "status"):
    """
    Enable/disable/configure the purge system
    
    Usage: !purgesystem <status|on|off|settings>
    Example: !purgesystem on
    """
    global PURGE_SYSTEM_ACTIVE
    
    if action.lower() == "status":
        # Show current status
        is_active = get_setting('purge_system_active', '0') == '1'
        last_triggered = get_setting('purge_last_triggered', 'Never')
        total_executions = get_setting('purge_total_executions', '0')
        total_purged = get_setting('purge_total_purged', '0')
        
        embed = create_info_embed(
            "🧹 Purge System Status",
            f"**System:** {'🟢 **ACTIVE**' if is_active else '🔴 **INACTIVE**'}\n"
            f"**Last Triggered:** {last_triggered[:19] if last_triggered != 'Never' else 'Never'}\n"
            f"**Total Executions:** {total_executions}\n"
            f"**Total Purged Nodes:** {total_purged}"
        )
        
        # Add current settings
        settings = [
            f"• **Min Age:** {get_setting('purge_min_age_days')} days",
            f"• **Max Inactivity:** {get_setting('purge_max_inactive_days')} days",
            f"• **Protect Running:** {'✅ Yes' if get_setting('purge_protect_running') == '1' else '❌ No'}",
            f"• **Protect Whitelisted:** {'✅ Yes' if get_setting('purge_protect_whitelisted') == '1' else '❌ No'}",
            f"• **Protect Recent:** {'✅ Yes' if get_setting('purge_protect_recent') == '1' else '❌ No'}",
            f"• **Recent Days:** {get_setting('purge_recent_days')} days",
            f"• **Dry Run Mode:** {'✅ ON' if get_setting('purge_dry_run') == '1' else '❌ OFF'}",
            f"• **Notify Users:** {'✅ Yes' if get_setting('purge_notify_users') == '1' else '❌ No'}",
            f"• **Auto Schedule:** {get_setting('purge_auto_schedule', 'disabled')}",
        ]
        
        add_field(embed, "⚙️ Current Settings", "\n".join(settings), False)
        
        # Add quick actions
        add_field(embed, "⚡ Quick Actions",
                 f"• Enable: `{PREFIX}purgesystem on`\n"
                 f"• Disable: `{PREFIX}purgesystem off`\n"
                 f"• Run purge: `{PREFIX}purge-start`\n"
                 f"• Configure: `{PREFIX}purgesetting <setting> <value>`", 
                 False)
        
        await ctx.send(embed=embed)
    
    elif action.lower() in ["on", "enable"]:
        set_setting('purge_system_active', '1')
        PURGE_SYSTEM_ACTIVE = True
        
        embed = create_success_embed(
            "✅ Purge System Enabled",
            "The purge system is now **ACTIVE**.\n\n"
            "**⚠️ Warning:**\n"
            "• System will automatically purge eligible nodes\n"
            "• Run a dry test first with `!purge-start dry`\n"
            "• Review settings with `!purgesystem status`"
        )
        await ctx.send(embed=embed)
    
    elif action.lower() in ["off", "disable"]:
        set_setting('purge_system_active', '0')
        PURGE_SYSTEM_ACTIVE = False
        
        embed = create_warning_embed(
            "🔴 Purge System Disabled",
            "The purge system is now **INACTIVE**.\n\n"
            "No automatic purges will occur until re-enabled."
        )
        await ctx.send(embed=embed)
    
    elif action.lower() == "settings":
        # Show all settings with descriptions
        embed = create_info_embed("⚙️ Purge System Settings",
                                 "All available purge system settings:")
        
        settings_list = [
            ("purge_min_age_days", "Minimum age before purge eligibility", "30"),
            ("purge_max_inactive_days", "Days of inactivity before purge", "14"),
            ("purge_protect_running", "Protect running nodes (1/0)", "1"),
            ("purge_protect_whitelisted", "Protect whitelisted nodes (1/0)", "1"),
            ("purge_protect_recent", "Protect recent nodes (1/0)", "1"),
            ("purge_recent_days", "Days considered 'recent'", "7"),
            ("purge_dry_run", "Dry run mode (1/0)", "1"),
            ("purge_notify_users", "Notify users when purged (1/0)", "1"),
            ("purge_backup_before", "Backup before purge (1/0)", "0"),
            ("purge_auto_schedule", "Auto schedule (daily/weekly/monthly/disabled)", "disabled"),
        ]
        
        for key, desc, default in settings_list:
            current = get_setting(key, default)
            add_field(embed, f"`{key}`", f"{desc}\n**Current:** {current}\n**Default:** {default}", False)
        
        add_field(embed, "📝 Usage",
                 f"Change a setting: `{PREFIX}purgesetting <key> <value>`\n"
                 f"Example: `{PREFIX}purgesetting purge_min_age_days 45`", 
                 False)
        
        await ctx.send(embed=embed)
    
    else:
        await ctx.send(embed=create_error_embed(
            "Invalid Action",
            f"Use: `{PREFIX}purgesystem <status|on|off|settings>`"
        ))

@bot.command(name='purge-start', aliases=['run-purge', 'start-purge'])
@is_admin()
async def purge_start_cmd(ctx, mode: str = "dry"):
    """
    Start a purge operation
    
    Usage: !purge-start [dry|real]
    Example: !purge-start dry  (for testing)
    Example: !purge-start real (actual deletion)
    """
    if mode.lower() not in ["dry", "real"]:
        await ctx.send(embed=create_error_embed(
            "Invalid Mode",
            "Use: `dry` for testing or `real` for actual deletion"
        ))
        return
    
    dry_run = mode.lower() == "dry"
    
    # Confirmation for real purge
    if not dry_run:
        embed = create_warning_embed(
            "⚠️ **CONFIRM REAL PURGE** ⚠️",
            "**You are about to perform a REAL purge operation!**\n\n"
            "This will **PERMANENTLY DELETE** eligible nodes.\n\n"
            "**Before proceeding:**\n"
            "1. Run a dry test first: `!purge-start dry`\n"
            "2. Check eligible nodes: `!purgestatus`\n"
            "3. Backup important data\n\n"
            "**This action cannot be undone!**"
        )
        
        class ConfirmView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
            
            @discord.ui.button(label="✅ CONFIRM REAL PURGE", style=discord.ButtonStyle.danger)
            async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
                if str(interaction.user.id) != str(ctx.author.id):
                    await interaction.response.send_message(
                        embed=create_error_embed("Access Denied", "Only the command author can confirm."),
                        ephemeral=True
                    )
                    return
                
                await interaction.response.defer()
                await perform_purge_operation(ctx, dry_run=False, interaction=interaction)
            
            @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
                if str(interaction.user.id) != str(ctx.author.id):
                    await interaction.response.send_message(
                        embed=create_error_embed("Access Denied", "Only the command author can cancel."),
                        ephemeral=True
                    )
                    return
                
                embed = create_info_embed("Cancelled", "Purge operation cancelled.")
                await interaction.response.edit_message(embed=embed, view=None)
        
        await ctx.send(embed=embed, view=ConfirmView())
        return
    
    # Dry run - no confirmation needed
    await perform_purge_operation(ctx, dry_run=True)

async def perform_purge_operation(ctx, dry_run: bool, interaction: discord.Interaction = None):
    """Perform the actual purge operation"""
    # Send initial message
    if interaction:
        msg = await interaction.followup.send(embed=create_info_embed(
            f"{'🧪 Testing' if dry_run else '🧹 Executing'} Purge",
            "Scanning all nodes for eligibility..."
        ))
    else:
        msg = await ctx.send(embed=create_info_embed(
            f"{'🧪 Testing' if dry_run else '🧹 Executing'} Purge",
            "Scanning all nodes for eligibility..."
        ))
    
    # Perform purge check
    results = await perform_purge_check(dry_run=dry_run)
    
    # Create results embed
    if dry_run:
        embed = create_info_embed(
            "🧪 Purge Test Results",
            f"**Purge ID:** `{results['purge_id']}`\n"
            f"**Mode:** 🧪 **DRY RUN** (No nodes were deleted)\n"
            f"**Timestamp:** {results['timestamp'][:19]}"
        )
    else:
        embed = create_warning_embed(
            "🧹 Purge Execution Results",
            f"**Purge ID:** `{results['purge_id']}`\n"
            f"**Mode:** ⚠️ **REAL EXECUTION**\n"
            f"**Timestamp:** {results['timestamp'][:19]}"
        )
    
    # Add statistics
    stats_text = (
        f"• **Total Nodes:** {results['total_nodes']}\n"
        f"• **Eligible for Purge:** {len(results['eligible'])}\n"
        f"• **Skipped/Protected:** {len(results['skipped'])}\n"
        f"• **Purged:** {len(results['purged'])}\n"
        f"• **Errors:** {len(results['errors'])}"
    )
    
    add_field(embed, "📊 Statistics", stats_text, False)
    
    # Show eligible nodes (first 5)
    if results['eligible']:
        eligible_list = []
        for i, node in enumerate(results['eligible'][:5], 1):
            eligible_list.append(f"{i}. **{node['node_name']}** (`{node['container']}`) - {node['reason']}")
        
        if len(results['eligible']) > 5:
            eligible_list.append(f"... and {len(results['eligible']) - 5} more")
        
        add_field(embed, "🎯 Eligible Nodes", "\n".join(eligible_list), False)
    
    # Show purged nodes (if any)
    if results['purged']:
        purged_list = []
        for i, node in enumerate(results['purged'][:5], 1):
            purged_list.append(f"{i}. **{node['node_name']}** (`{node['container']}`)")
        
        if len(results['purged']) > 5:
            purged_list.append(f"... and {len(results['purged']) - 5} more")
        
        add_field(embed, "🗑️ Purged Nodes", "\n".join(purged_list), False)
    
    # Show errors (if any)
    if results['errors']:
        error_list = []
        for i, error in enumerate(results['errors'][:3], 1):
            error_list.append(f"{i}. `{error['container']}`: {error['error'][:50]}...")
        
        if len(results['errors']) > 3:
            error_list.append(f"... and {len(results['errors']) - 3} more errors")
        
        add_field(embed, "❌ Errors", "\n".join(error_list), False)
    
    # Add recommendations
    if dry_run and len(results['eligible']) > 0:
        add_field(embed, "💡 Recommendations",
                 f"**{len(results['eligible'])} nodes would be purged in a real run.**\n"
                 f"To protect important nodes:\n"
                 f"1. Use `{PREFIX}protect-node <container>`\n"
                 f"2. Whitelist nodes: `{PREFIX}whitelist-node <container>`\n"
                 f"3. Start inactive nodes to reset timer", 
                 False)
    
    # Add next steps
    if dry_run:
        add_field(embed, "🔄 Next Steps",
                 f"To execute the purge: `{PREFIX}purge-start real`\n"
                 f"To adjust settings: `{PREFIX}purgesystem settings`\n"
                 f"To disable system: `{PREFIX}purgesystem off`", 
                 False)
    
    await msg.edit(embed=embed)

@bot.command(name='purgestatus', aliases=['purge-status', 'check-purge'])
async def purgestatus_cmd(ctx):
    """
    Show detailed purge system status
    
    Usage: !purgestatus
    """
    # Get purge stats
    purge_stats = get_purge_stats()
    is_active = get_setting('purge_system_active', '0') == '1'
    
    # Create embed
    embed = create_embed(
        "🧹 Purge System Status Report",
        f"**System Status:** {'🟢 **ACTIVE**' if is_active else '🔴 **INACTIVE**'}\n"
        f"**Last Triggered:** {purge_stats['last_execution'][:19] if purge_stats['last_execution'] != 'Never' else 'Never'}",
        0x9b59b6 if is_active else 0x95a5a6
    )
    
    # Statistics section
    stats_text = (
        f"• **Total Executions:** {purge_stats['total_executions']}\n"
        f"• **Total Purged Nodes:** {purge_stats['total_purged']}\n"
        f"• **Total Protected Actions:** {purge_stats['total_protected']}\n"
        f"• **Total Skipped:** {purge_stats['total_skipped']}\n"
        f"• **Currently Protected:** {purge_stats['protected_nodes']} nodes\n"
        f"• **Currently Eligible:** {purge_stats['eligible_nodes']} nodes\n"
        f"• **Total Nodes:** {purge_stats['total_nodes']} nodes"
    )
    
    add_field(embed, "📊 Statistics", stats_text, False)
    
    # Current settings
    settings_text = (
        f"• **Min Age:** {get_setting('purge_min_age_days')} days\n"
        f"• **Max Inactivity:** {get_setting('purge_max_inactive_days')} days\n"
        f"• **Dry Run Mode:** {'✅ ON' if get_setting('purge_dry_run') == '1' else '❌ OFF'}\n"
        f"• **Protect Running:** {'✅ Yes' if get_setting('purge_protect_running') == '1' else '❌ No'}\n"
        f"• **Protect Whitelisted:** {'✅ Yes' if get_setting('purge_protect_whitelisted') == '1' else '❌ No'}\n"
        f"• **Auto Schedule:** {get_setting('purge_auto_schedule', 'disabled')}"
    )
    
    add_field(embed, "⚙️ Current Settings", settings_text, False)
    
    # Get recent purge history
    history = get_purge_history(limit=5)
    if history:
        history_text = []
        for entry in history:
            timestamp = datetime.fromisoformat(entry['created_at']).strftime('%m/%d %H:%M')
            action_emoji = "🗑️" if entry['action'] == 'purged' else "🛡️" if entry['action'] == 'protected' else "⏭️"
            history_text.append(f"{action_emoji} {timestamp} - {entry['node_name']} ({entry['action']})")
        
        add_field(embed, "📜 Recent Activity", "\n".join(history_text), False)
    
    # Get top protected nodes
    protected_nodes = get_all_protected_nodes()
    if protected_nodes:
        protected_text = []
        for i, node in enumerate(protected_nodes[:3], 1):
            protected_text.append(f"{i}. **{node['node_name']}** (`{node['container_name'][:15]}...`)")
        
        if len(protected_nodes) > 3:
            protected_text.append(f"... and {len(protected_nodes) - 3} more")
        
        add_field(embed, "🛡️ Protected Nodes", "\n".join(protected_text), False)
    
    # Recommendations
    recommendations = []
    
    if purge_stats['eligible_nodes'] > 0 and is_active:
        recommendations.append(f"⚠️ **{purge_stats['eligible_nodes']} nodes are purge eligible**")
        recommendations.append(f"Run test: `{PREFIX}purge-start dry`")
    
    if get_setting('purge_dry_run') == '1' and is_active:
        recommendations.append("⚠️ **System is in dry run mode**")
        recommendations.append(f"Real purges disabled: `{PREFIX}purgesetting purge_dry_run 0`")
    
    if not is_active and purge_stats['eligible_nodes'] > 0:
        recommendations.append(f"⚠️ **System is disabled**")
        recommendations.append(f"{purge_stats['eligible_nodes']} nodes would be eligible")
        recommendations.append(f"Enable: `{PREFIX}purgesystem on`")
    
    if recommendations:
        add_field(embed, "💡 Recommendations", "\n".join(recommendations), False)
    
    # Actions
    add_field(embed, "⚡ Actions",
             f"• Test purge: `{PREFIX}purge-start dry`\n"
             f"• Run purge: `{PREFIX}purge-start real`\n"
             f"• Toggle system: `{PREFIX}purgesystem {'off' if is_active else 'on'}`\n"
             f"• View settings: `{PREFIX}purgesystem settings`\n"
             f"• List eligible: `{PREFIX}list-nodes inactive`", 
             False)
    
    await ctx.send(embed=embed)

@bot.command(name='purgesetting', aliases=['set-purge', 'configure-purge'])
@is_admin()
async def purgesetting_cmd(ctx, setting: str = None, value: str = None):
    """
    Configure purge system settings
    
    Usage: !purgesetting <setting> <value>
    Example: !purgesetting purge_min_age_days 45
    Example: !purgesetting purge_dry_run 0
    """
    if setting is None or value is None:
        # Show help
        embed = create_info_embed("⚙️ Purge System Settings Help",
                                 f"Configure purge system settings with `{PREFIX}purgesetting <setting> <value>`")
        
        settings_examples = [
            ("purge_min_age_days", "45", "Minimum age in days"),
            ("purge_max_inactive_days", "30", "Max inactivity days"),
            ("purge_dry_run", "0", "Disable dry run (1=on, 0=off)"),
            ("purge_protect_running", "0", "Don't protect running nodes"),
            ("purge_notify_users", "0", "Don't notify users"),
            ("purge_auto_schedule", "weekly", "Auto schedule (daily/weekly/monthly)"),
        ]
        
        for key, example, desc in settings_examples:
            add_field(embed, f"`{key}`", f"{desc}\nExample: `{PREFIX}purgesetting {key} {example}`", False)
        
        add_field(embed, "📝 View All Settings",
                 f"View all current settings: `{PREFIX}purgesystem settings`", 
                 False)
        
        await ctx.send(embed=embed)
        return
    
    # Validate setting
    valid_settings = [
        'purge_min_age_days', 'purge_max_inactive_days', 'purge_protect_running',
        'purge_protect_whitelisted', 'purge_protect_recent', 'purge_recent_days',
        'purge_dry_run', 'purge_notify_users', 'purge_backup_before',
        'purge_auto_schedule'
    ]
    
    if setting not in valid_settings:
        await ctx.send(embed=create_error_embed(
            "Invalid Setting",
            f"Valid settings: {', '.join(valid_settings)}"
        ))
        return
    
    # Validate value based on setting
    old_value = get_setting(setting)
    
    try:
        if setting in ['purge_min_age_days', 'purge_max_inactive_days', 'purge_recent_days']:
            value_int = int(value)
            if value_int < 1:
                raise ValueError("Must be positive")
            value = str(value_int)
        
        elif setting in ['purge_protect_running', 'purge_protect_whitelisted', 
                        'purge_protect_recent', 'purge_dry_run', 
                        'purge_notify_users', 'purge_backup_before']:
            if value not in ['0', '1']:
                raise ValueError("Must be 0 or 1")
        
        elif setting == 'purge_auto_schedule':
            if value not in ['disabled', 'daily', 'weekly', 'monthly']:
                raise ValueError("Must be: disabled, daily, weekly, or monthly")
        
        # Update setting
        set_setting(setting, value)
        
        # Update global variable if needed
        if setting == 'purge_system_active':
            global PURGE_SYSTEM_ACTIVE
            PURGE_SYSTEM_ACTIVE = value == '1'
        
        # Success message
        embed = create_success_embed(
            "✅ Setting Updated",
            f"**Setting:** `{setting}`\n"
            f"**Old Value:** {old_value}\n"
            f"**New Value:** {value}"
        )
        
        # Add setting description
        descriptions = {
            'purge_min_age_days': "Minimum age before a node can be purged",
            'purge_max_inactive_days': "Days of inactivity before purge eligibility",
            'purge_dry_run': "Dry run mode (1=test only, 0=real deletion)",
            'purge_protect_running': "Protect running nodes from purge",
            'purge_protect_whitelisted': "Protect whitelisted nodes from purge",
            'purge_notify_users': "Notify users when their nodes are purged",
            'purge_auto_schedule': "Automatic purge schedule"
        }
        
        if setting in descriptions:
            add_field(embed, "📝 Description", descriptions[setting], False)
        
        await ctx.send(embed=embed)
        
    except ValueError as e:
        await ctx.send(embed=create_error_embed(
            "Invalid Value",
            f"Error: {str(e)}"
        ))

@bot.command(name='protect-node', aliases=['purge-protect', 'node-protect'])
async def protect_node_cmd(ctx, container_name: str, *, reason: str = "User request"):
    """
    Protect a node from automatic purge
    
    Usage: !protect-node <container_name> [reason]
    Example: !protect-node voltarisvm-user-123456 "Production server"
    """
    # Find node
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed(
            "Node Not Found", 
            f"No node found with container name: `{container_name}`"
        ))
        return
    
    # Check permissions
    user_id = str(ctx.author.id)
    is_admin_user = user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", [])
    
    if node['user_id'] != user_id and not is_admin_user:
        await ctx.send(embed=create_error_embed(
            "Access Denied", 
            "You can only protect your own nodes."
        ))
        return
    
    if node['purge_protected']:
        await ctx.send(embed=create_warning_embed(
            "Already Protected",
            f"Node **{node['node_name']}** is already purge protected."
        ))
        return
    
    # Add protection
    add_purge_protection(container_name, user_id, reason)
    
    # Success message
    embed = create_success_embed(
        "🛡️ Node Protected",
        f"**Node:** {node['node_name']}\n"
        f"**Container:** `{container_name}`\n"
        f"**Protected By:** {ctx.author.mention}\n"
        f"**Reason:** {reason}\n\n"
        f"This node is now safe from automatic purge."
    )
    
    add_field(embed, "ℹ️ Information",
             "Purge protection prevents the node from being automatically deleted.\n"
             "You can remove protection at any time with:\n"
             f"`{PREFIX}unprotect-node {container_name}`", 
             False)
    
    await ctx.send(embed=embed)

@bot.command(name='unprotect-node', aliases=['remove-protection', 'node-unprotect'])
async def unprotect_node_cmd(ctx, container_name: str):
    """
    Remove purge protection from a node
    
    Usage: !unprotect-node <container_name>
    Example: !unprotect-node voltarisvm-user-123456
    """
    # Find node
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed(
            "Node Not Found", 
            f"No node found with container name: `{container_name}`"
        ))
        return
    
    # Check permissions
    user_id = str(ctx.author.id)
    is_admin_user = user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", [])
    
    if node['user_id'] != user_id and not is_admin_user:
        await ctx.send(embed=create_error_embed(
            "Access Denied", 
            "You can only unprotect your own nodes."
        ))
        return
    
    if not node['purge_protected']:
        await ctx.send(embed=create_warning_embed(
            "Not Protected",
            f"Node **{node['node_name']}** is not purge protected."
        ))
        return
    
    # Remove protection
    remove_purge_protection(container_name)
    
    # Success message
    embed = create_warning_embed(
        "⚠️ Protection Removed",
        f"**Node:** {node['node_name']}\n"
        f"**Container:** `{container_name}`\n\n"
        f"This node is no longer protected from automatic purge."
    )
    
    # Check if node is now eligible for purge
    if PURGE_SYSTEM_ACTIVE:
        eligible, reason = is_node_eligible_for_purge(node)
        if eligible:
            add_field(embed, "⚠️ Purge Warning",
                     f"This node is now **eligible for automatic purge**!\n"
                     f"**Reason:** {reason}\n\n"
                     f"To protect it again: `{PREFIX}protect-node {container_name}`", 
                     False)
    
    await ctx.send(embed=embed)

@bot.command(name='purge-history', aliases=['purge-logs', 'purge-log'])
@is_admin()
async def purge_history_cmd(ctx, limit: int = 20):
    """
    Show purge system history
    
    Usage: !purge-history [limit]
    Example: !purge-history 50
    """
    if limit < 1 or limit > 100:
        await ctx.send(embed=create_error_embed(
            "Invalid Limit",
            "Limit must be between 1 and 100"
        ))
        return
    
    history = get_purge_history(limit=limit)
    
    if not history:
        embed = create_info_embed("Purge History", "No purge history found.")
        await ctx.send(embed=embed)
        return
    
    # Group by purge ID
    purge_groups = {}
    for entry in history:
        purge_id = entry['purge_id']
        if purge_id not in purge_groups:
            purge_groups[purge_id] = []
        purge_groups[purge_id].append(entry)
    
    # Create embed for each purge group
    for purge_id, entries in list(purge_groups.items())[:3]:  # Show first 3 groups
        # Get purge stats for this group
        purged = [e for e in entries if e['action'] == 'purged']
        protected = [e for e in entries if e['action'] == 'protected']
        skipped = [e for e in entries if e['action'] == 'skipped']
        
        first_entry = entries[0]
        timestamp = datetime.fromisoformat(first_entry['created_at']).strftime('%Y-%m-%d %H:%M')
        
        embed = create_embed(
            f"🧹 Purge History - {purge_id}",
            f"**Time:** {timestamp}\n"
            f"**Total Actions:** {len(entries)}\n"
            f"• 🗑️ Purged: {len(purged)}\n"
            f"• 🛡️ Protected: {len(protected)}\n"
            f"• ⏭️ Skipped: {len(skipped)}"
        )
        
        # Show recent actions
        recent_actions = []
        for i, entry in enumerate(entries[:5], 1):
            action_emoji = "🗑️" if entry['action'] == 'purged' else "🛡️" if entry['action'] == 'protected' else "⏭️"
            time_str = datetime.fromisoformat(entry['created_at']).strftime('%H:%M')
            recent_actions.append(f"{action_emoji} {time_str} - {entry['node_name']} ({entry['action']})")
        
        if recent_actions:
            add_field(embed, "📜 Recent Actions", "\n".join(recent_actions), False)
        
        # Show sample purged nodes
        if purged:
            sample = []
            for i, entry in enumerate(purged[:3], 1):
                sample.append(f"{i}. **{entry['node_name']}** (`{entry['container_name'][:15]}...`)")
            
            if len(purged) > 3:
                sample.append(f"... and {len(purged) - 3} more")
            
            add_field(embed, "🗑️ Purged Nodes", "\n".join(sample), False)
        
        await ctx.send(embed=embed)
    
    # If there are more groups, mention it
    if len(purge_groups) > 3:
        embed = create_info_embed(
            "More History Available",
            f"Showing 3 of {len(purge_groups)} purge operations.\n"
            f"Use `{PREFIX}purge-history {limit}` to see more."
        )
        await ctx.send(embed=embed)

# ======================
# NODE MANAGEMENT COMMANDS
# ======================

@bot.command(name='start-node')
async def start_node_cmd(ctx, container_name: str):
    """Start a stopped node"""
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed("Node Not Found", f"`{container_name}` not found"))
        return
    
    # Check permissions
    user_id = str(ctx.author.id)
    if node['user_id'] != user_id and user_id not in admin_data.get("admins", []) and user_id != MAIN_ADMIN_ID:
        await ctx.send(embed=create_error_embed("Access Denied", "You don't own this node"))
        return
    
    if node['suspended']:
        await ctx.send(embed=create_error_embed("Node Suspended", "Cannot start a suspended node"))
        return
    
    if node['status'] == 'running':
        await ctx.send(embed=create_warning_embed("Already Running", "Node is already running"))
        return
    
    try:
        await execute_lxc(f"lxc start {container_name}")
        update_node_status(container_name, 'running')
        
        embed = create_success_embed(
            "✅ Node Started",
            f"**Node:** {node['node_name']}\n"
            f"**Container:** `{container_name}`\n"
            f"**Status:** 🟢 RUNNING"
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(embed=create_error_embed("Start Failed", str(e)))

@bot.command(name='stop-node')
async def stop_node_cmd(ctx, container_name: str):
    """Stop a running node"""
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed("Node Not Found", f"`{container_name}` not found"))
        return
    
    # Check permissions
    user_id = str(ctx.author.id)
    if node['user_id'] != user_id and user_id not in admin_data.get("admins", []) and user_id != MAIN_ADMIN_ID:
        await ctx.send(embed=create_error_embed("Access Denied", "You don't own this node"))
        return
    
    if node['status'] != 'running':
        await ctx.send(embed=create_warning_embed("Not Running", "Node is not running"))
        return
    
    try:
        await execute_lxc(f"lxc stop {container_name}")
        update_node_status(container_name, 'stopped')
        
        embed = create_success_embed(
            "🛑 Node Stopped",
            f"**Node:** {node['node_name']}\n"
            f"**Container:** `{container_name}`\n"
            f"**Status:** 🔴 STOPPED"
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(embed=create_error_embed("Stop Failed", str(e)))

@bot.command(name='manage-node')
async def manage_node_cmd(ctx, container_name: str):
    """Interactive node management interface"""
    node = get_node_by_name(container_name)
    if not node:
        await ctx.send(embed=create_error_embed("Node Not Found", f"`{container_name}` not found"))
        return
    
    # Check permissions
    user_id = str(ctx.author.id)
    is_admin_user = user_id == str(MAIN_ADMIN_ID) or user_id in admin_data.get("admins", [])
    
    if node['user_id'] != user_id and not is_admin_user:
        await ctx.send(embed=create_error_embed("Access Denied", "You don't own this node"))
        return
    
    # Create management view
    class ManageNodeView(discord.ui.View):
        def __init__(self, node_data):
            super().__init__(timeout=300)
            self.node = node_data
            self.user_id = user_id
            self.is_admin = is_admin_user
        
        @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.primary)
        async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
            await interaction.response.defer()
            await node_status_cmd(ctx, self.node['container_name'])
        
        @discord.ui.button(label="▶ Start", style=discord.ButtonStyle.success)
        async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.node['suspended']:
                await interaction.response.send_message(
                    embed=create_error_embed("Suspended", "Cannot start suspended node"),
                    ephemeral=True
                )
                return
            
            if self.node['status'] == 'running':
                await interaction.response.send_message(
                    embed=create_warning_embed("Already Running", "Node is already running"),
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await start_node_cmd(ctx, self.node['container_name'])
        
        @discord.ui.button(label="⏹️ Stop", style=discord.ButtonStyle.danger)
        async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.node['status'] != 'running':
                await interaction.response.send_message(
                    embed=create_warning_embed("Not Running", "Node is not running"),
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await stop_node_cmd(ctx, self.node['container_name'])
        
        @discord.ui.button(label="🛡️ Protect", style=discord.ButtonStyle.primary)
        async def protect(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.node['purge_protected']:
                await interaction.response.send_message(
                    embed=create_warning_embed("Already Protected", "Node is already purge protected"),
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await protect_node_cmd(ctx, self.node['container_name'])
    
    # Get current status
    status = await get_container_status(container_name)
    update_node_status(container_name, status)
    
    # Create embed
    embed = create_embed(
        f"🔧 Manage Node: {node['node_name']}",
        f"**Container:** `{container_name}`\n"
        f"**Status:** {status.upper()}\n"
        f"**Owner:** <@{node['user_id']}>"
    )
    
    add_field(embed, "Specifications", 
             f"• RAM: {node['ram']}\n• CPU: {node['cpu']} cores\n• Storage: {node['storage']}", 
             True)
    
    add_field(embed, "Configuration", 
             f"• Location: {node['location']}\n• OS: {node['os_version']}\n• Created: {node['created_at'][:10]}", 
             True)
    
    # Add purge system status
    purge_status = []
    if node['purge_protected']:
        purge_status.append("🛡️ **PURGE PROTECTED**")
    elif PURGE_SYSTEM_ACTIVE:
        eligible, reason = is_node_eligible_for_purge(node)
        if eligible:
            purge_status.append(f"⚠️ **PURGE ELIGIBLE** ({reason})")
        else:
            purge_status.append(f"✅ **SAFE FROM PURGE** ({reason})")
    
    if purge_status:
        add_field(embed, "🧹 Purge Status", "\n".join(purge_status), False)
    
    if node['suspended']:
        add_field(embed, "⚠️ Warning", "This node is SUSPENDED", False)
    
    if node['whitelisted']:
        add_field(embed, "⭐ Status", "This node is WHITELISTED", False)
    
    # Add notes if available
    if node['notes']:
        add_field(embed, "📝 Notes", node['notes'][:200], False)
    
    view = ManageNodeView(node)
    await ctx.send(embed=embed, view=view)

# ======================
# SYSTEM COMMANDS
# ======================

@bot.command(name='node-stats')
@is_admin()
async def node_stats_cmd(ctx):
    """Show system-wide node statistics"""
    all_nodes = get_all_nodes()
    
    # Calculate statistics
    total_nodes = len(all_nodes)
    running_nodes = len([n for n in all_nodes if n['status'] == 'running' and not n['suspended']])
    stopped_nodes = len([n for n in all_nodes if n['status'] == 'stopped' and not n['suspended']])
    suspended_nodes = len([n for n in all_nodes if n['suspended']])
    whitelisted_nodes = len([n for n in all_nodes if n['whitelisted']])
    protected_nodes = len([n for n in all_nodes if n['purge_protected']])
    
    # Calculate resource totals
    total_ram = 0
    total_cpu = 0
    total_storage = 0
    
    for node in all_nodes:
        try:
            total_ram += int(node['ram'].replace('GB', ''))
            total_cpu += int(node['cpu'])
            total_storage += int(node['storage'].replace('GB', ''))
        except:
            pass
    
    # Calculate user distribution
    user_counts = {}
    for node in all_nodes:
        user_id = node['user_id']
        user_counts[user_id] = user_counts.get(user_id, 0) + 1
    
    top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Create embed
    embed = create_embed(
        "📊 System Node Statistics",
        f"**Total Nodes:** {total_nodes}\n**Active Users:** {len(user_counts)}",
        0x9b59b6
    )
    
    # Status distribution
    add_field(embed, "📈 Status Distribution",
             f"• 🟢 **Running:** {running_nodes} nodes\n"
             f"• 🔴 **Stopped:** {stopped_nodes} nodes\n"
             f"• ⛔ **Suspended:** {suspended_nodes} nodes\n"
             f"• ⭐ **Whitelisted:** {whitelisted_nodes} nodes\n"
             f"• 🛡️ **Purge Protected:** {protected_nodes} nodes",
             False)
    
    # Resource totals
    add_field(embed, "⚙️ Resource Allocation",
             f"• **Total RAM:** {total_ram} GB\n"
             f"• **Total CPU:** {total_cpu} cores\n"
             f"• **Total Storage:** {total_storage} GB",
             True)
    
    # Top users
    top_users_text = ""
    for i, (user_id, count) in enumerate(top_users, 1):
        try:
            user = await bot.fetch_user(int(user_id))
            top_users_text += f"{i}. {user.name}: {count} nodes\n"
        except:
            top_users_text += f"{i}. User {user_id[:8]}: {count} nodes\n"
    
    if top_users_text:
        add_field(embed, "👥 Top Users", top_users_text, True)
    
    # Recent activity
    recent_nodes = sorted(all_nodes, key=lambda x: x['created_at'], reverse=True)[:3]
    recent_text = ""
    for node in recent_nodes:
        created = datetime.fromisoformat(node['created_at']).strftime('%Y-%m-%d')
        recent_text += f"• {node['node_name']} ({created})\n"
    
    if recent_text:
        add_field(embed, "🆕 Recent Nodes", recent_text, True)
    
    # System health
    health_percentage = (running_nodes / total_nodes * 100) if total_nodes > 0 else 0
    health_emoji = "🟢" if health_percentage > 80 else "🟡" if health_percentage > 50 else "🔴"
    
    add_field(embed, "🏥 System Health",
             f"{health_emoji} **Health:** {health_percentage:.1f}%\n"
             f"• Running: {running_nodes}/{total_nodes}\n"
             f"• Suspension Rate: {(suspended_nodes/total_nodes*100):.1f}%",
             False)
    
    # Purge system stats
    if PURGE_SYSTEM_ACTIVE:
        purge_stats = get_purge_stats()
        purge_text = (
            f"🟢 **System Active**\n"
            f"• Eligible: {purge_stats['eligible_nodes']} nodes\n"
            f"• Protected: {purge_stats['protected_nodes']} nodes\n"
            f"• Total Purged: {purge_stats['total_purged']}"
        )
        add_field(embed, "🧹 Purge System", purge_text, True)
    
    await ctx.send(embed=embed)

@bot.command(name='help-nodes')
async def help_nodes_cmd(ctx):
    """Show help for node commands"""
    embed = create_embed(
        "📚 Node System Help",
        f"**Prefix:** `{PREFIX}`\n"
        f"**Bot Name:** {BOT_NAME}\n\n"
        f"**Complete Node Management System**\n"
        "Create, manage, and monitor VPS nodes with purge protection."
    )
    
    # Node commands
    add_field(embed, "🖥️ Node Commands",
             f"• `{PREFIX}create-node <ram> <cpu> <disk> <loc> <name> [@user]` - Create node\n"
             f"• `{PREFIX}node-status [container]` - Check node status\n"
             f"• `{PREFIX}list-nodes [category]` - List nodes\n"
             f"• `{PREFIX}node <container>` - Detailed node info\n"
             f"• `{PREFIX}manage-node <container>` - Manage node\n"
             f"• `{PREFIX}start-node <container>` - Start node\n"
             f"• `{PREFIX}stop-node <container>` - Stop node\n"
             f"• `{PREFIX}remove-node <container> [reason]` - Remove node (Admin)\n"
             f"• `{PREFIX}reedit-node <container> <field> <value>` - Edit node (Admin)",
             False)
    
    # Purge system commands
    add_field(embed, "🧹 Purge System Commands",
             f"• `{PREFIX}purgesystem <status|on|off|settings>` - Control purge system\n"
             f"• `{PREFIX}purge-start [dry|real]` - Start purge operation\n"
             f"• `{PREFIX}purgestatus` - Show purge system status\n"
             f"• `{PREFIX}purgesetting <setting> <value>` - Configure purge\n"
             f"• `{PREFIX}purge-history [limit]` - View purge history\n"
             f"• `{PREFIX}protect-node <container> [reason]` - Protect node\n"
             f"• `{PREFIX}unprotect-node <container>` - Remove protection",
             False)
    
    # List categories
    categories_text = "\n".join([f"• `{cat}` - {desc}" for cat, desc in NODE_CATEGORIES.items()])
    add_field(embed, "📂 List Categories", categories_text, False)
    
    # OS options
    os_text = "\n".join([f"• {o['label']}" for o in OS_OPTIONS[:5]])
    if len(OS_OPTIONS) > 5:
        os_text += f"\n• ... and {len(OS_OPTIONS) - 5} more"
    add_field(embed, "🐧 Available OS", os_text, False)
    
    # Examples
    add_field(embed, "📝 Examples",
             f"• Create: `{PREFIX}create-node 4 2 50 usa-east MyServer @user`\n"
             f"• Status: `{PREFIX}node-status voltarisvm-user-123456`\n"
             f"• List running: `{PREFIX}list-nodes running`\n"
             f"• Protect: `{PREFIX}protect-node voltarisvm-user-123456 \"Important\"`\n"
             f"• Test purge: `{PREFIX}purge-start dry`",
             False)
    
    embed.set_footer(text=f"Use {PREFIX}help for all commands | {BOT_NAME} Complete System")
    await ctx.send(embed=embed)

@bot.command(name='ping')
async def ping_cmd(ctx):
    """Check bot latency"""
    latency = round(bot.latency * 1000)
    embed = create_success_embed("🏓 Pong!", f"Latency: {latency}ms")
    await ctx.send(embed=embed)

@bot.command(name='uptime')
async def uptime_cmd(ctx):
    """Show bot uptime"""
    try:
        result = subprocess.run(['uptime'], capture_output=True, text=True)
        embed = create_info_embed("⏱️ System Uptime", result.stdout.strip())
        await ctx.send(embed=embed)
    except:
        await ctx.send(embed=create_info_embed("⏱️ Uptime", "Uptime information unavailable"))

# Bot events
@bot.event
async def on_ready():
    logger.info(f'{bot.user} has connected to Discord!')
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.watching, 
        name=f"{BOT_NAME} Node Manager"
    ))
    logger.info(f"{BOT_NAME} Bot is ready!")
    
    # Check if purge system should run on startup
    if PURGE_SYSTEM_ACTIVE and get_setting('purge_auto_schedule') != 'disabled':
        logger.info(f"Purge system is active with schedule: {get_setting('purge_auto_schedule')}")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=create_error_embed(
            "Missing Argument", 
            f"Please check command usage with `{PREFIX}help`."
        ))
    elif isinstance(error, commands.CheckFailure):
        await ctx.send(embed=create_error_embed(
            "Access Denied", 
            "You don't have permission to use this command."
        ))
    else:
        logger.error(f"Command error: {error}")
        await ctx.send(embed=create_error_embed(
            "Error", 
            "An unexpected error occurred."
        ))

# Run the bot
if __name__ == "__main__":
    if DISCORD_TOKEN:
        logger.info(f"Starting {BOT_NAME} Bot with Purge System...")
        bot.run(DISCORD_TOKEN)
    else:
        logger.error("No Discord token found. Please set DISCORD_TOKEN environment variable.")
