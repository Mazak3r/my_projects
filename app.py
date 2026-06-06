#!/usr/bin/env python
"""
PON Outage Monitor - Complete Unified Script
ONE session per OLT with keepalive, simultaneous monitoring, persistent ONU registry
"""

import asyncio
import time
import re
import json
import os
import pandas as pd
import nest_asyncio
import threading
import multiprocessing
import subprocess
import sys
from datetime import datetime, timedelta
import traceback
import signal
import webbrowser
import socket
import atexit
import psutil
import platform

# Apply nest_asyncio to handle async in Streamlit
nest_asyncio.apply()

# ------------------- CONFIG LOADER -------------------

def get_base_dir():
    """Get the base directory (works for both script and EXE)"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

def load_config():
    """Load configuration from config.json file"""
    config_path = CONFIG_FILE
    
    # Default configuration if file doesn't exist
    default_config = {
        "credentials": {
            "username": "admin",
            "password": "admin"
        },
        "olt_devices": {},
        "monitoring": {
            "monitor_interval": 300,
            "session_keepalive_interval": 45,
            "high_loss_threshold": -29
        }
    }
    
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"✅ Loaded configuration from {config_path}")
            
            # Validate required sections
            if "credentials" not in config:
                print("⚠️ 'credentials' section missing in config, using defaults")
                config["credentials"] = default_config["credentials"]
            
            if "olt_devices" not in config:
                print("⚠️ 'olt_devices' section missing in config, using defaults")
                config["olt_devices"] = default_config["olt_devices"]
            
            if "monitoring" not in config:
                print("⚠️ 'monitoring' section missing in config, using defaults")
                config["monitoring"] = default_config["monitoring"]
            
            return config
        else:
            print(f"⚠️ Config file not found at {config_path}")
            print("Creating default config.json file...")
            
            # Create a default config file
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            
            print("✅ Created config.json with default settings. Please edit it with your OLT details.")
            return default_config
            
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing config.json: {e}")
        print("Please fix the JSON format in config.json")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        print("Using default configuration")
        return default_config

# Load configuration
config = load_config()

# Extract configuration values
OLT_DEVICES = config.get("olt_devices", {})
USERNAME = config.get("credentials", {}).get("username", "admin")
PASSWORD = config.get("credentials", {}).get("password", "admin")
MONITOR_INTERVAL = config.get("monitoring", {}).get("monitor_interval", 300)
SESSION_KEEPALIVE_INTERVAL = config.get("monitoring", {}).get("session_keepalive_interval", 45)
HIGH_LOSS_THRESHOLD = config.get("monitoring", {}).get("high_loss_threshold", -29)

# File paths - all in the base directory
DOWN_PORTS_FILE = os.path.join(BASE_DIR, "down_ports.json")
ONU_REGISTRY_FILE = os.path.join(BASE_DIR, "onu_registry.json")
PORT_ONU_COUNTS_FILE = os.path.join(BASE_DIR, "port_onu_counts.json")
LAST_UPDATE_FILE = os.path.join(BASE_DIR, "last_update.txt")
TOTAL_ONU_REGISTERED_FILE = os.path.join(BASE_DIR, "total_onu_registered.txt")
TOTAL_ONU_ONLINE_FILE = os.path.join(BASE_DIR, "total_onu_online.txt")
MONITOR_RUNNING_FILE = os.path.join(BASE_DIR, "monitor.running")
OLT_STATE_FILE = os.path.join(BASE_DIR, "olt_state.json")

# Global variables for data sharing
port_statuses = {}
olt_statuses = {}
last_update_time = None
status_history = {}
down_ports = {}
olt_down_status = {}
onu_registry = {}
port_onu_counts = {}
total_onu_registered = 0
total_onu_online = 0
data_lock = threading.Lock()
shutdown_event = threading.Event()
monitor_thread = None
monitor_thread_lock = threading.Lock()
cleanup_done = False
_ui_initialized = False
olt_reachable_state = {}

# ------------------- LOGGING -------------------

def log_to_console(message):
    """Print message to console"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)

# ------------------- PING FUNCTION -------------------

def ping_node(ip, timeout=2):
    """Simple ping check"""
    try:
        param = "-n" if platform.system().lower() == "windows" else "-c"
        result = subprocess.run(
            ["ping", param, "1", ip],
            capture_output=True,
            timeout=timeout
        )
        return result.returncode == 0
    except:
        return False

def check_olt_reachability(olt_ip, olt_name):
    """Check if OLT is reachable via ping"""
    log_to_console(f"Pinging {olt_name} ({olt_ip})...")
    reachable = ping_node(olt_ip)
    if reachable:
        log_to_console(f"✅ {olt_name} is reachable")
    else:
        log_to_console(f"❌ {olt_name} is NOT reachable")
    return reachable

# ------------------- ONU REGISTRY FUNCTIONS -------------------

def load_onu_registry():
    """Load persistent ONU registry from file"""
    global onu_registry, total_onu_registered
    try:
        if os.path.exists(ONU_REGISTRY_FILE):
            with open(ONU_REGISTRY_FILE, 'r', encoding='utf-8') as f:
                onu_registry = json.load(f)
            total_onu_registered = len(onu_registry)
            log_to_console(f"Loaded ONU registry with {total_onu_registered} total ONUs")
        else:
            onu_registry = {}
            total_onu_registered = 0
    except Exception as e:
        log_to_console(f"Error loading ONU registry: {e}")
        onu_registry = {}
        total_onu_registered = 0

def save_onu_registry():
    """Save persistent ONU registry to file"""
    global onu_registry
    try:
        temp_file = ONU_REGISTRY_FILE + ".tmp"
        with open(temp_file, "w", encoding='utf-8') as f:
            json.dump(onu_registry, f, indent=2)
        os.replace(temp_file, ONU_REGISTRY_FILE)
    except Exception as e:
        log_to_console(f"Error saving ONU registry: {e}")

def update_onu_registry(olt_name, port, onu_ids):
    """Update ONU registry with new ONUs (counts only go up)"""
    global onu_registry, total_onu_registered
    new_onus = 0
    current_time = time.time()
    
    with data_lock:
        for onu_id in onu_ids:
            key = f"{olt_name}_{port}_{onu_id}"
            if key not in onu_registry:
                onu_registry[key] = {
                    "olt_name": olt_name,
                    "port": port,
                    "onu_id": onu_id,
                    "first_seen": current_time,
                    "last_seen": current_time
                }
                new_onus += 1
                log_to_console(f"✨ New ONU discovered: {olt_name} Port {port} ONU {onu_id}")
            else:
                onu_registry[key]["last_seen"] = current_time
        
        if new_onus > 0:
            total_onu_registered = len(onu_registry)
            save_onu_registry()
    
    return new_onus

# ------------------- PORT ONU COUNTS FUNCTIONS -------------------

def load_port_onu_counts():
    """Load persistent port ONU counts from file"""
    global port_onu_counts
    try:
        if os.path.exists(PORT_ONU_COUNTS_FILE):
            with open(PORT_ONU_COUNTS_FILE, 'r', encoding='utf-8') as f:
                port_onu_counts = json.load(f)
            log_to_console(f"Loaded port ONU counts for {len(port_onu_counts)} ports")
        else:
            port_onu_counts = {}
    except Exception as e:
        log_to_console(f"Error loading port ONU counts: {e}")
        port_onu_counts = {}

def save_port_onu_counts():
    """Save persistent port ONU counts to file"""
    global port_onu_counts
    try:
        temp_file = PORT_ONU_COUNTS_FILE + ".tmp"
        with open(temp_file, "w", encoding='utf-8') as f:
            json.dump(port_onu_counts, f, indent=2)
        os.replace(temp_file, PORT_ONU_COUNTS_FILE)
    except Exception as e:
        log_to_console(f"Error saving port ONU counts: {e}")

def update_port_onu_count(olt_name, port, onu_ids):
    """Update the maximum ONU count seen for a specific port"""
    global port_onu_counts
    key = f"{olt_name}_{port}"
    current_count = len(onu_ids)
    
    with data_lock:
        if key not in port_onu_counts:
            port_onu_counts[key] = {
                "olt_name": olt_name,
                "port": port,
                "max_onu_count": current_count,
                "first_seen": time.time(),
                "last_updated": time.time()
            }
            log_to_console(f"📊 Initial ONU count for {olt_name} Port {port}: {current_count}")
        else:
            if current_count > port_onu_counts[key]["max_onu_count"]:
                port_onu_counts[key]["max_onu_count"] = current_count
                port_onu_counts[key]["last_updated"] = time.time()
                log_to_console(f"📈 Updated max ONU count for {olt_name} Port {port}: {current_count}")
        
        save_port_onu_counts()
        return port_onu_counts[key]["max_onu_count"]

# ------------------- FILE-BASED DATA STORAGE -------------------

def load_olt_state():
    """Load OLT reachability state"""
    global olt_reachable_state
    try:
        if os.path.exists(OLT_STATE_FILE):
            with open(OLT_STATE_FILE, 'r', encoding='utf-8') as f:
                olt_reachable_state = json.load(f)
    except Exception as e:
        log_to_console(f"Error loading OLT state: {e}")

def save_olt_state():
    """Save OLT reachability state"""
    try:
        with open(OLT_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(olt_reachable_state, f, indent=2)
    except Exception as e:
        log_to_console(f"Error saving OLT state: {e}")

def save_all_data():
    """Save all current data to files"""
    global down_ports, olt_down_status, last_update_time, total_onu_registered, total_onu_online
    
    temp_files = []
    try:
        temp_down = DOWN_PORTS_FILE + ".tmp"
        with open(temp_down, "w", encoding='utf-8') as f:
            down_data = {
                'ports': down_ports,
                'olts': olt_down_status
            }
            json.dump(down_data, f, indent=2)
        temp_files.append(temp_down)
        
        temp_time = LAST_UPDATE_FILE + ".tmp"
        with open(temp_time, "w", encoding='utf-8') as f:
            f.write(str(last_update_time if last_update_time else int(time.time())))
        temp_files.append(temp_time)
        
        temp_total_reg = TOTAL_ONU_REGISTERED_FILE + ".tmp"
        with open(temp_total_reg, "w", encoding='utf-8') as f:
            f.write(str(total_onu_registered))
        temp_files.append(temp_total_reg)
        
        temp_total_on = TOTAL_ONU_ONLINE_FILE + ".tmp"
        with open(temp_total_on, "w", encoding='utf-8') as f:
            f.write(str(total_onu_online))
        temp_files.append(temp_total_on)
        
        for temp in temp_files:
            actual = temp.replace('.tmp', '')
            if os.path.exists(actual):
                os.remove(actual)
            os.rename(temp, actual)
            
    except Exception as e:
        log_to_console(f"Error saving data: {e}")
        for temp in temp_files:
            try:
                if os.path.exists(temp):
                    os.remove(temp)
            except:
                pass

def load_all_data():
    """Load all data from files"""
    global down_ports, olt_down_status, last_update_time, total_onu_registered, total_onu_online
    
    try:
        if os.path.exists(DOWN_PORTS_FILE):
            with open(DOWN_PORTS_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                down_ports = data.get('ports', {})
                olt_down_status = data.get('olts', {})
        
        if os.path.exists(LAST_UPDATE_FILE):
            with open(LAST_UPDATE_FILE, "r", encoding='utf-8') as f:
                last_update_time = float(f.read().strip())
        
        if os.path.exists(TOTAL_ONU_REGISTERED_FILE):
            with open(TOTAL_ONU_REGISTERED_FILE, "r", encoding='utf-8') as f:
                total_onu_registered = int(f.read().strip())
        
        if os.path.exists(TOTAL_ONU_ONLINE_FILE):
            with open(TOTAL_ONU_ONLINE_FILE, "r", encoding='utf-8') as f:
                total_onu_online = int(f.read().strip())
        
        load_onu_registry()
        load_port_onu_counts()
        load_olt_state()
        
        log_to_console(f"Loaded {len(down_ports)} down ports, {len(olt_down_status)} down OLTs, {total_onu_registered} total ONUs")
    except Exception as e:
        log_to_console(f"Error loading data: {e}")

# ------------------- PERSISTENT TELNET SESSION MANAGEMENT -------------------

telnet_sessions = {}  # {host: (reader, writer, last_used)}
session_lock = threading.Lock()

async def get_telnet_session(host, username, password, force_new=False):
    """Get or create a persistent telnet session - ONE SESSION PER OLT"""
    global telnet_sessions, olt_reachable_state
    
    with session_lock:
        if not force_new and host in telnet_sessions:
            reader, writer, last_used = telnet_sessions[host]
            try:
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.wait_for(reader.read(100), timeout=2)
                log_to_console(f"🔄 Reusing session for {host}")
                telnet_sessions[host] = (reader, writer, time.time())
                return reader, writer, True
            except:
                log_to_console(f"⚠️ Session for {host} is dead, creating new one")
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                del telnet_sessions[host]
    
    try:
        log_to_console(f"🔌 Creating new session for {host}...")
        reader, writer = await asyncio.open_connection(host, 23)
        
        await asyncio.sleep(1)
        writer.write(f"{username}\r\n".encode())
        await writer.drain()
        await asyncio.sleep(1)
        writer.write(f"{password}\r\n".encode())
        await writer.drain()
        await asyncio.sleep(2)
        
        response = await reader.read(1024)
        if b"incorrect" in response.lower() or b"invalid" in response.lower():
            log_to_console(f"❌ Login failed for {host}!")
            writer.close()
            await writer.wait_closed()
            return None, None, False
        
        log_to_console(f"✅ Session established for {host}")
        telnet_sessions[host] = (reader, writer, time.time())
        olt_reachable_state[host] = True
        save_olt_state()
        return reader, writer, True
        
    except Exception as e:
        log_to_console(f"❌ Error creating session for {host}: {e}")
        olt_reachable_state[host] = False
        save_olt_state()
        return None, None, False

async def close_all_sessions():
    """Close all telnet sessions gracefully"""
    global telnet_sessions
    with session_lock:
        for host, (reader, writer, _) in list(telnet_sessions.items()):
            try:
                writer.close()
                await writer.wait_closed()
                log_to_console(f"🔒 Closed session for {host}")
            except Exception as e:
                log_to_console(f"Error closing session for {host}: {e}")
        telnet_sessions = {}

async def send_keepalive():
    """Send keepalive to all sessions to prevent timeout"""
    global telnet_sessions
    with session_lock:
        for host, (reader, writer, last_used) in list(telnet_sessions.items()):
            try:
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.wait_for(reader.read(100), timeout=2)
                telnet_sessions[host] = (reader, writer, time.time())
                log_to_console(f"💓 Keepalive sent to {host}")
            except Exception as e:
                log_to_console(f"Keepalive failed for {host}: {e}")
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                if host in telnet_sessions:
                    del telnet_sessions[host]

# ------------------- TELNET FUNCTIONS -------------------

def parse_onu_data(response_text):
    """Parse the ONU data from the response text"""
    lines = response_text.split('\n')
    onu_ids = []
    onu_details = []
    
    for line in lines:
        line = line.strip()
        
        if re.match(r'^\s*\d+\s+\d+\s+[0-9a-f-]', line):
            parts = line.split()
            if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
                onu_id = parts[1]
                onu_ids.append(onu_id)
                onu_details.append({"onu_id": onu_id})
        
        elif re.match(r'^onu-\d+', line):
            parts = line.split()
            if parts and parts[0].startswith('onu-'):
                onu_id = parts[0].replace('onu-', '').strip()
                if onu_id.isdigit():
                    onu_ids.append(onu_id)
                    onu_details.append({"onu_id": onu_id})
    
    unique_ids = []
    seen = set()
    for onu_id in onu_ids:
        if onu_id not in seen:
            seen.add(onu_id)
            unique_ids.append(onu_id)
    
    return unique_ids, onu_details[:len(unique_ids)]

async def get_full_command_output(reader, writer, command, max_pages=10):
    """Get full command output by handling pagination"""
    full_output = ""
    writer.write((command + "\r\n").encode())
    await writer.drain()
    
    for page in range(max_pages):
        await asyncio.sleep(2)
        response = b''
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=2)
                if not chunk:
                    break
                response += chunk
                
                if b'Press any key to continue' in chunk or b'continue (Q to quit)' in chunk:
                    writer.write(b' ')
                    await writer.drain()
                    break
                elif b'epon#' in chunk or b'#' in chunk or b'>' in chunk:
                    full_output += response.decode('utf-8', errors='ignore')
                    return full_output
                elif b'Total:' in chunk and b'online' in chunk:
                    full_output += response.decode('utf-8', errors='ignore')
                    return full_output
        except asyncio.TimeoutError:
            pass
        
        full_output += response.decode('utf-8', errors='ignore')
        
        if 'epon#' in full_output or '#' in full_output or '>' in full_output:
            break
        if 'Press any key to continue' not in full_output and 'continue (Q to quit)' not in full_output:
            break
    
    return full_output

async def get_onu_signal(reader, writer, port, onu_id):
    """Get ONU signal strength"""
    try:
        command = f"show olt {port} onu {onu_id} ctc optical"
        writer.write((command + "\r\n").encode())
        await writer.drain()
        
        await asyncio.sleep(1)
        response = b''
        try:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=5)
            response += chunk
        except asyncio.TimeoutError:
            pass
        
        output = response.decode('utf-8', errors='ignore')
        
        match = re.search(r'rx power\s+(-?\d+\.\d+)\s*d?Bm?', output, re.IGNORECASE)
        if match:
            return float(match.group(1))
        else:
            return None
    except Exception as e:
        return None

# ------------------- DOWN PORTS MANAGEMENT -------------------

def update_down_port(olt_name, port, description, status, onu_count, avg_signal=None):
    """Update a down port in the tracking dictionary"""
    global down_ports
    key = f"{olt_name}_{port}"
    current_time = time.time()
    
    # Get the maximum ONU count ever seen for this port
    max_onu_count = port_onu_counts.get(key, {}).get("max_onu_count", onu_count)
    
    with data_lock:
        if key not in down_ports:
            down_ports[key] = {
                "olt_name": olt_name,
                "port": port,
                "description": description,
                "status": status,
                "onu_count": onu_count,
                "max_onu_count": max_onu_count,
                "avg_signal": avg_signal,
                "down_since": current_time,
                "last_seen": current_time
            }
            log_to_console(f"🔴 New outage: {olt_name} Port {port} - {status} (Max ONUs: {max_onu_count})")
        else:
            down_ports[key].update({
                "status": status,
                "onu_count": onu_count,
                "max_onu_count": max_onu_count,
                "avg_signal": avg_signal,
                "last_seen": current_time
            })
    
    save_all_data()

def remove_down_port(olt_name, port):
    """Remove a port from down tracking when it recovers"""
    global down_ports
    key = f"{olt_name}_{port}"
    
    with data_lock:
        if key in down_ports:
            down_time = time.time() - down_ports[key]["down_since"]
            log_to_console(f"✅ Port recovered: {olt_name} Port {port} (was down for {int(down_time/60)} min)")
            del down_ports[key]
    
    save_all_data()

def update_olt_down(olt_name, ip, description):
    """Update OLT down status"""
    global olt_down_status
    key = f"{olt_name}"
    current_time = time.time()
    
    # Calculate total max ONUs for all ports on this OLT
    total_max_onus = 0
    for port_key, port_data in port_onu_counts.items():
        if port_data.get("olt_name") == olt_name:
            total_max_onus += port_data.get("max_onu_count", 0)
    
    with data_lock:
        if key not in olt_down_status:
            olt_down_status[key] = {
                "olt_name": olt_name,
                "ip": ip,
                "description": description,
                "down_since": current_time,
                "last_seen": current_time,
                "total_max_onus": total_max_onus
            }
            log_to_console(f"⚫ OLT down: {olt_name} (Affects up to {total_max_onus} ONUs)")
            
            # Remove any port outages for this OLT since OLT down supersedes them
            keys_to_remove = [k for k in down_ports.keys() if k.startswith(f"{olt_name}_")]
            for remove_key in keys_to_remove:
                del down_ports[remove_key]
                log_to_console(f"  ↳ Removed port outage for {remove_key} (OLT down)")
        else:
            olt_down_status[key].update({
                "last_seen": current_time,
                "total_max_onus": total_max_onus
            })
    
    save_all_data()

def remove_olt_down(olt_name):
    """Remove OLT from down tracking when it recovers"""
    global olt_down_status
    key = f"{olt_name}"
    
    with data_lock:
        if key in olt_down_status:
            down_time = time.time() - olt_down_status[key]["down_since"]
            log_to_console(f"✅ OLT recovered: {olt_name} (was down for {int(down_time/60)} min)")
            del olt_down_status[key]
    
    save_all_data()

# ------------------- MONITORING FUNCTIONS -------------------

async def monitor_olt(host, username, password):
    """Monitor a specific OLT device using ONE persistent session for all ports"""
    global total_onu_online
    
    olt_info = None
    for olt_id, info in OLT_DEVICES.items():
        if info["ip"] == host:
            olt_info = info
            break
    
    if not olt_info:
        log_to_console(f"OLT info not found for {host}")
        return None
    
    was_reachable = olt_reachable_state.get(host, True)
    
    if not check_olt_reachability(host, olt_info["name"]):
        log_to_console(f"❌ {olt_info['name']} is not reachable")
        olt_reachable_state[host] = False
        save_olt_state()
        update_olt_down(olt_info["name"], host, olt_info.get("description", ""))
        return None
    
    force_new = not was_reachable or host not in telnet_sessions
    reader, writer, connected = await get_telnet_session(host, username, password, force_new)
    
    if not connected:
        olt_reachable_state[host] = False
        save_olt_state()
        update_olt_down(olt_info["name"], host, olt_info.get("description", ""))
        return None
    
    olt_reachable_state[host] = True
    save_olt_state()
    remove_olt_down(olt_info["name"])
    
    olt_ports = [int(p) for p in olt_info["ports"].keys()]
    total_onus_this_olt = 0
    
    for port in olt_ports:
        port_str = str(port)
        description = olt_info["ports"].get(port_str, f"Port {port}")
        log_to_console(f"\n--- Checking {olt_info['name']} Port {port} - {description} ---")
        
        try:
            command = f"show olt online-onu {port}"
            full_output = await get_full_command_output(reader, writer, command)
            
            onu_ids, onu_details = parse_onu_data(full_output)
            
            # Update ONU registry
            new_onus = update_onu_registry(olt_info["name"], port_str, onu_ids)
            if new_onus > 0:
                log_to_console(f"Added {new_onus} new ONUs to registry")
            
            # Update port-specific max ONU count
            max_onu_count = update_port_onu_count(olt_info["name"], port_str, onu_ids)
            
            signals = []
            for onu in onu_details[:5]:
                signal = await get_onu_signal(reader, writer, port, onu["onu_id"])
                if signal:
                    signals.append(signal)
            
            avg_signal = sum(signals) / len(signals) if signals else None
            total_onus_this_olt += len(onu_ids)
            
            if onu_ids:
                log_to_console(f"Port {port}: {len(onu_ids)} ONUs online (Max seen: {max_onu_count})")
                if avg_signal:
                    log_to_console(f"   Average signal: {avg_signal:.1f} dBm")
            else:
                log_to_console(f"Port {port}: No ONUs online (Max seen: {max_onu_count})")
            
            if len(onu_ids) == 0:
                new_status = "Port Down"
            elif avg_signal and avg_signal < HIGH_LOSS_THRESHOLD:
                new_status = "High Loss"
            else:
                new_status = "Normal"
            
            if new_status in ["Port Down", "High Loss"]:
                update_down_port(olt_info["name"], port_str, description, new_status, len(onu_ids), avg_signal)
            else:
                remove_down_port(olt_info["name"], port_str)
                
        except Exception as e:
            log_to_console(f"Error monitoring port {port}: {e}")
    
    log_to_console(f"\n{'='*60}")
    log_to_console(f"OLT {olt_info['name']}: {total_onus_this_olt} ONUs online")
    log_to_console(f"{'='*60}")
    
    with data_lock:
        total_onu_online += total_onus_this_olt
    
    return True

async def monitor_all_olts_simultaneous():
    """Monitor all OLTs simultaneously using ONE session per OLT"""
    global last_update_time, total_onu_online
    
    log_to_console("\n" + "="*80)
    log_to_console(f"🚀 STARTING MONITORING CYCLE at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_to_console("="*80)
    
    total_onu_online = 0
    
    tasks = []
    for olt_id, olt_info in OLT_DEVICES.items():
        log_to_console(f"Adding monitoring task for {olt_info['name']} ({olt_info['ip']})")
        tasks.append(monitor_olt(olt_info["ip"], USERNAME, PASSWORD))
    
    if not tasks:
        log_to_console("No OLTs to monitor!")
        return
    
    log_to_console(f"Running {len(tasks)} monitoring tasks simultaneously...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success_count = sum(1 for r in results if r is True)
    log_to_console(f"Monitoring cycle complete: {success_count}/{len(tasks)} OLTs successful")
    
    with data_lock:
        last_update_time = time.time()
    
    log_to_console(f"\n{'='*60}")
    log_to_console(f"📊 TOTAL ONUs REGISTERED: {total_onu_registered}")
    log_to_console(f"📶 TOTAL ONUs ONLINE: {total_onu_online}")
    log_to_console(f"{'='*60}")
    
    save_all_data()
    
    log_to_console("\n" + "="*80)
    log_to_console(f"✅ MONITORING CYCLE COMPLETED at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_to_console("="*80 + "\n")

# ------------------- KEEPALIVE LOOP -------------------

async def keepalive_loop():
    """Continuous keepalive loop that runs between monitoring cycles"""
    while True:
        await asyncio.sleep(SESSION_KEEPALIVE_INTERVAL)
        if telnet_sessions:
            log_to_console("💓 Sending keepalive to all sessions...")
            await send_keepalive()

# ------------------- MONITORING THREAD -------------------

def is_monitor_running():
    """Check if monitor thread is running"""
    return os.path.exists(MONITOR_RUNNING_FILE)

def monitoring_thread_func():
    """Main monitoring thread function - runs in background thread"""
    log_to_console("="*80)
    log_to_console("🟢 MONITORING THREAD STARTED")
    log_to_console("="*80)
    
    try:
        with open(MONITOR_RUNNING_FILE, 'w') as f:
            f.write('1')
        
        log_to_console(f"Monitoring interval: {MONITOR_INTERVAL} seconds")
        log_to_console(f"Keepalive interval: {SESSION_KEEPALIVE_INTERVAL} seconds")
        
        # Load OLT state
        load_olt_state()
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Start keepalive task
        keepalive_task = loop.create_task(keepalive_loop())
        
        next_monitor_time = time.time()
        cycle_count = 0
        
        while os.path.exists(MONITOR_RUNNING_FILE):
            try:
                current_time = time.time()
                
                if current_time >= next_monitor_time:
                    cycle_count += 1
                    log_to_console(f"\n{'='*80}")
                    log_to_console(f"STARTING MONITORING CYCLE #{cycle_count}")
                    log_to_console(f"{'='*80}")
                    
                    # Run monitoring cycle
                    loop.run_until_complete(monitor_all_olts_simultaneous())
                    
                    # Schedule next cycle
                    next_monitor_time = time.time() + MONITOR_INTERVAL
                    next_time_str = datetime.fromtimestamp(next_monitor_time).strftime('%H:%M:%S')
                    log_to_console(f"Next cycle #{cycle_count + 1} scheduled at {next_time_str}")
                
                # Sleep for 1 second
                loop.run_until_complete(asyncio.sleep(1))
                
            except Exception as e:
                log_to_console(f"Error in monitoring thread: {e}")
                traceback.print_exc()
                time.sleep(5)
        
        # Cleanup
        keepalive_task.cancel()
        loop.run_until_complete(close_all_sessions())
        loop.close()
        
    except Exception as e:
        log_to_console(f"Fatal error in monitoring thread: {e}")
        traceback.print_exc()
    finally:
        try:
            if os.path.exists(MONITOR_RUNNING_FILE):
                os.remove(MONITOR_RUNNING_FILE)
        except:
            pass
        
        log_to_console("="*80)
        log_to_console("⚫ MONITORING THREAD STOPPED")
        log_to_console("="*80)

def start_monitoring():
    """Start the monitoring thread if not already running"""
    global monitor_thread
    
    with monitor_thread_lock:
        if is_monitor_running():
            log_to_console("Monitoring already running")
            return True
        
        log_to_console("Starting monitoring thread...")
        
        # Clean up any stale files
        try:
            if os.path.exists(MONITOR_RUNNING_FILE):
                os.remove(MONITOR_RUNNING_FILE)
                log_to_console("Removed stale running file")
        except:
            pass
        
        # Clear shutdown event
        shutdown_event.clear()
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=monitoring_thread_func, daemon=True)
        monitor_thread.start()
        log_to_console("Monitoring thread started")
        
        # Wait for thread to initialize and verify it's running
        time.sleep(3)
        
        if is_monitor_running():
            log_to_console("✅ Monitoring verified running")
            return True
        else:
            log_to_console("⚠️ Monitoring started but not verified - will retry")
            time.sleep(2)
            if is_monitor_running():
                log_to_console("✅ Monitoring verified running on second check")
                return True
            else:
                log_to_console("❌ Failed to start monitoring properly")
                return False

def stop_monitoring():
    """Stop the monitoring thread"""
    global cleanup_done, monitor_thread
    
    if cleanup_done:
        return
    
    with monitor_thread_lock:
        if cleanup_done:
            return
            
        log_to_console("Stopping monitoring...")
        
        # Remove running file to signal thread
        if os.path.exists(MONITOR_RUNNING_FILE):
            try:
                os.remove(MONITOR_RUNNING_FILE)
                log_to_console("Removed running file to signal monitoring")
            except:
                pass
        
        # Wait for thread to exit gracefully
        if monitor_thread and monitor_thread.is_alive():
            log_to_console("Waiting for monitoring thread to exit...")
            monitor_thread.join(timeout=10)
        
        # Clean up files
        try:
            if os.path.exists(MONITOR_RUNNING_FILE):
                os.remove(MONITOR_RUNNING_FILE)
                log_to_console("Running file removed")
        except:
            pass
        
        cleanup_done = True
        log_to_console("Monitoring stopped")

# ------------------- STREAMLIT UI FUNCTIONS -------------------

def run_streamlit_ui():
    """This function runs when the script is executed by Streamlit"""
    global _ui_initialized, monitor_thread
    
    import streamlit as st
    import pandas as pd
    
    if not _ui_initialized:
        _ui_initialized = True
        log_to_console("First UI initialization")
        
        # Load existing data
        load_all_data()
        
        # Start monitoring if not running
        if not is_monitor_running():
            log_to_console("Starting monitoring from UI...")
            start_monitoring()
        else:
            log_to_console("Monitoring already active")
        
        # Register cleanup only once
        atexit.register(stop_monitoring)
    
    # Force a data reload on each UI refresh
    load_all_data()
    
    st.set_page_config(
        page_title="PON Outage Monitor",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.markdown("""
        <style>
        .main > div { padding: 0rem 0rem; }
        .stDataFrame { width: 100%; }
        .block-container {
            padding-top: 0.5rem;
            padding-bottom: 0rem;
            padding-left: 0.5rem;
            padding-right: 0.5rem;
            max-width: 100%;
        }
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .badge {
            padding: 10px 20px;
            border-radius: 5px;
            font-size: 1.2em;
            font-weight: bold;
            text-align: center;
            margin: 5px;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("📡 CDATA OLT Port Outage Monitor")
    
    # Top bar with 6 columns
    col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1.5, 1.5, 1.5, 1.5])
    
    with col1:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    with col2:
        if is_monitor_running():
            st.markdown(
                "<div class='badge' style='background-color: #4CAF50; color: white;'>🟢 Active</div>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                "<div class='badge' style='background-color: #f44336; color: white;'>⚫ Stopped</div>",
                unsafe_allow_html=True
            )
            if st.button("🚀 Start Monitoring"):
                with st.spinner("Starting monitoring..."):
                    if start_monitoring():
                        st.success("Monitoring started!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to start monitoring")
    
    # Load data for display
    data = get_ui_data()
    
    with col3:
        if data['last_update_time']:
            time_diff = int(time.time() - data['last_update_time'])
            mins = time_diff // 60
            secs = time_diff % 60
            st.markdown(
                f"<div class='badge' style='background-color: #e0e0e0;'>⏱️ {mins}m {secs}s ago</div>",
                unsafe_allow_html=True
            )
    
    with col4:
        st.markdown(
            f"<div class='badge' style='background-color: #2196F3; color: white;'>📋 Registered: {data['total_onu_registered']}</div>",
            unsafe_allow_html=True
        )
    
    with col5:
        st.markdown(
            f"<div class='badge' style='background-color: #4CAF50; color: white;'>📶 Online: {data['total_onu_online']}</div>",
            unsafe_allow_html=True
        )
    
    with col6:
        if data['last_update_time']:
            next_run = data['last_update_time'] + MONITOR_INTERVAL
            seconds_to_next = int(next_run - time.time())
            if seconds_to_next > 0:
                mins = seconds_to_next // 60
                secs = seconds_to_next % 60
                st.markdown(
                    f"<div class='badge' style='background-color: #FF9800; color: white;'>⏰ Next: {mins}m {secs}s</div>",
                    unsafe_allow_html=True
                )
    
    # Prepare outage data - LATEST FIRST
    outages = []
    total_affected = 0
    
    # First add OLT outages (they take precedence)
    for key, olt_data in data['olt_down_status'].items():
        down_since = olt_data.get('down_since', time.time())
        total_max_onus = olt_data.get('total_max_onus', 0)
        total_affected += total_max_onus
        
        outages.append({
            "OLT": olt_data.get('olt_name', 'Unknown'),
            "Port": "ALL PORTS",
            "Status": "⚫ OLT Down",
            "ONUs Affected": total_max_onus,
            "Since": format_duration(down_since),
            "sort_time": down_since,
            "type": "olt"
        })
    
    # Then add port outages (only if OLT is not down)
    down_olts = set(olt_data.get('olt_name') for olt_data in data['olt_down_status'].values())
    
    for key, port_data in data['down_ports'].items():
        olt_name = port_data.get('olt_name', 'Unknown')
        
        # Skip if this OLT is down
        if olt_name in down_olts:
            continue
            
        down_since = port_data.get('down_since', time.time())
        status = port_data.get('status', 'Unknown')
        avg_signal = port_data.get('avg_signal')
        onu_count = port_data.get('onu_count', 0)
        max_onu_count = port_data.get('max_onu_count', onu_count)
        
        affected_count = max_onu_count
        total_affected += affected_count
        
        if status == "Port Down":
            status_display = "🔴 Port Down"
        elif status == "High Loss":
            signal_str = f"{avg_signal:.1f} dBm" if avg_signal else "N/A"
            status_display = f"🟡 High Loss ({signal_str})"
        else:
            status_display = status
        
        outages.append({
            "OLT": olt_name,
            "Port": f"Port {port_data.get('port')}",
            "Description": port_data.get('description', ''),
            "Status": status_display,
            "ONUs Affected": affected_count,
            "Since": format_duration(down_since),
            "sort_time": down_since,
            "type": "port"
        })
    
    # Sort by newest first (descending timestamp)
    outages.sort(key=lambda x: x['sort_time'], reverse=True)
    
    # Remove sort key and type for display
    for outage in outages:
        if 'sort_time' in outage:
            del outage['sort_time']
        if 'type' in outage:
            del outage['type']
    
    # Display outages
    if outages:
        df = pd.DataFrame(outages)
        
        # Color coding
        def color_rows(row):
            if '⚫' in str(row['Status']):
                return ['background-color: #e0e0e0'] * len(row)
            elif '🔴' in str(row['Status']):
                return ['background-color: #ffcdd2'] * len(row)
            elif '🟡' in str(row['Status']):
                return ['background-color: #fff9c4'] * len(row)
            return [''] * len(row)
        
        styled_df = df.style.apply(color_rows, axis=1)
        st.dataframe(styled_df, use_container_width=True, hide_index=True, height=500)
        
        # Summary
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Outages", len(outages))
        with col2:
            port_outages = sum(1 for d in outages if '🔴' in d['Status'] or '🟡' in d['Status'])
            st.metric("Active Outages", port_outages)
        with col3:
            olt_outages = len(data['olt_down_status'])
            st.metric("OLT Outages", olt_outages)
        with col4:
            st.metric("Total Affected ONUs", total_affected)
        
        if total_affected > 0:
            st.warning(f"📊 Total subscribers affected: {total_affected}")
    else:
        st.success("✅ All systems operational - No outages detected")
        if data['total_onu_online'] > 0:
            st.info(f"📶 {data['total_onu_online']} ONUs currently online")
    
    # Auto-refresh
    time.sleep(30)
    st.rerun()

def get_ui_data():
    """Get data for UI display from files"""
    data = {
        'down_ports': {},
        'olt_down_status': {},
        'last_update_time': None,
        'total_onu_registered': 0,
        'total_onu_online': 0
    }
    
    try:
        if os.path.exists(DOWN_PORTS_FILE):
            with open(DOWN_PORTS_FILE, "r", encoding='utf-8') as f:
                file_data = json.load(f)
                data['down_ports'] = file_data.get('ports', {})
                data['olt_down_status'] = file_data.get('olts', {})
        
        if os.path.exists(LAST_UPDATE_FILE):
            with open(LAST_UPDATE_FILE, "r", encoding='utf-8') as f:
                data['last_update_time'] = float(f.read().strip())
        
        if os.path.exists(TOTAL_ONU_REGISTERED_FILE):
            with open(TOTAL_ONU_REGISTERED_FILE, "r", encoding='utf-8') as f:
                data['total_onu_registered'] = int(f.read().strip())
        
        if os.path.exists(TOTAL_ONU_ONLINE_FILE):
            with open(TOTAL_ONU_ONLINE_FILE, "r", encoding='utf-8') as f:
                data['total_onu_online'] = int(f.read().strip())
    except Exception as e:
        log_to_console(f"Error reading UI data: {e}")
    
    return data

def format_duration(timestamp):
    """Format duration from timestamp"""
    if not timestamp:
        return "Unknown"
    
    seconds = time.time() - timestamp
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}m"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h"
    else:
        days = int(seconds / 86400)
        return f"{days}d"

# ------------------- LAUNCHER FUNCTION -------------------

def is_port_in_use(port):
    """Check if a port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            return False
        except socket.error:
            return True

def run_streamlit_app():
    """Run the Streamlit app as a separate process"""
    script_path = os.path.abspath(__file__)
    
    print("=" * 60)
    print("PON Outage Monitor Launcher")
    print("=" * 60)
    print(f"Base directory: {BASE_DIR}")
    print(f"Script path: {script_path}")
    print("=" * 60)
    
    if is_port_in_use(8501):
        print("Port 8501 is already in use. Streamlit might already be running.")
        response = input("Do you want to try to kill the existing process? (y/n): ")
        if response.lower() == 'y':
            try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    if proc.info['cmdline'] and 'streamlit' in ' '.join(proc.info['cmdline']):
                        print(f"Killing Streamlit process {proc.info['pid']}")
                        proc.kill()
                time.sleep(2)
            except:
                print("Could not kill existing process. Please close it manually.")
                input("Press Enter to exit...")
                sys.exit(1)
        else:
            print("Please close the existing Streamlit instance and try again.")
            input("Press Enter to exit...")
            sys.exit(1)
    
    env = os.environ.copy()
    env['STREAMLIT_SERVER_PORT'] = '8501'
    env['STREAMLIT_SERVER_ADDRESS'] = 'localhost'
    env['STREAMLIT_SERVER_HEADLESS'] = 'true'
    env['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
    
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        script_path,
        "--server.port=8501",
        "--server.address=localhost",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
        "--server.runOnSave=false"
    ]
    
    print("\nStarting PON Outage Monitor...")
    print("Opening browser at http://localhost:8501")
    print("Please wait...\n")
    
    def open_browser():
        time.sleep(3)
        webbrowser.open('http://localhost:8501')
    
    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()
    
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n\nReceived shutdown signal...")
    except Exception as e:
        print(f"\nError launching Streamlit: {e}")
    
    print("\nPON Outage Monitor stopped")
    time.sleep(2)

# ------------------- MAIN ENTRY POINT -------------------

def main():
    multiprocessing.freeze_support()
    
    if 'streamlit' in sys.modules or 'STREAMLIT_RUN' in os.environ:
        if not hasattr(main, "_logged_start"):
            main._logged_start = True
            log_to_console("Starting PON Outage Monitor UI...")
        run_streamlit_ui()
    else:
        run_streamlit_app()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_to_console("Shutting down...")
        stop_monitoring()
    except Exception as e:
        log_to_console(f"Error: {e}")
        traceback.print_exc()
        stop_monitoring()