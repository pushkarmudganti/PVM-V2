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

# ============ UNCHANGEABLE CONSTANTS ============
__BOT_CREATOR__ = "WANNYGdRAGON"
__CREATION_DATE__ = "6/01/2026"
__BOT_WATERMARK__ = f"Created by {__BOT_CREATOR__} • {__CREATION_DATE__}"
__BOT_NAME__ = "PVMLIX"
__BOT_VERSION__ = "PVM V2"
