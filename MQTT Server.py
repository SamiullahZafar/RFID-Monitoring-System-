import sys
import threading
import paho.mqtt.client as mqtt
import cx_Oracle
import logging
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, wait
import time
import json
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from tkinter.font import Font
import traceback
import queue
import psutil
from collections import defaultdict

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rfid_server.log')
    ]
)

# Configuration with optimized values
CONFIG = {
    "mqtt": {
        "broker": "192.168.12.12",
        "port": 1883,
        "keepalive": 60,
        "client_id": "rfid_server_optimized",
        "qos": 1,
        "retain": True,
        "max_inflight_messages": 50,
        "max_queued_messages": 500,
        "reconnect_delay": 5
    },
    "database": {
        "username": "rfid",
        "password": "main",
        "host": "192.168.12.7",
        "port": 1521,
        "service_name": "rfd",
        "pool": {
            "min": 5,
            "max": 20,
            "increment": 2,
            "timeout": 30
        }
    },
    "threading": {
        "max_workers": 50,
        "queue_size": 500
    },
    "tables": {
        "employee": "MV_EMPLOYEES",
        "scan": "MACHINE_OPERATOR_SCANS",
        "bundle_scans": "GARMENT_BUNDLE_SCANS",
        "bundle": "v_cuttingbundled",
        "workstation_status": "WORKSTATION_STATUS",
        "error_logs": "RFID_SYSTEM_ERROR_LOGS"
    },
    "responses": {
        "login_success": "LOGIN_SUCCESS",
        "login_exists": "LOGIN_EXISTS",
        "login_required": "LOGIN_REQUIRED",
        "bundle_started": "BUNDLE_STARTED",
        "bundle_ended": "BUNDLE_ENDED",
        "bundle_active_elsewhere": "BUNDLE_ACTIVE_AT_",
        "bundle_completed": "BUNDLE_COMPLETED",
        "unauthorized": "UNAUTHORIZED_CARD",
        "error_generic": "SYSTEM_ERROR",
        "previous_bundle_active": "PREV_BUNDLE_ACTIVE",
        "status_red": "STATUS_RED",
        "status_yellow": "STATUS_YELLOW",
        "status_green": "STATUS_GREEN",
        "no_operator": "NO_OPERATOR"
    },
    "device": {
        "timeout_minutes": 5,
        "heartbeat_interval": 30,
        "max_message_rate": 100  # messages per second
    }
}

class ResourceMonitor:
    def __init__(self, server):
        self.server = server
        self.running = True
        self.message_rate = 0
        self.message_count = 0
        self.last_check = time.time()
        
    def start(self):
        threading.Thread(target=self.monitor_resources, daemon=True).start()
        
    def stop(self):
        self.running = False
        
    def monitor_resources(self):
        while self.running:
            try:
                # Check memory usage
                process = psutil.Process()
                mem_info = process.memory_info()
                mem_mb = mem_info.rss / (1024 * 1024)
                
                # Check CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                
                # Update GUI with resource usage
                self.server.gui.update_resource_usage(cpu_percent, mem_mb)
                
                # Calculate message rate
                now = time.time()
                elapsed = now - self.last_check
                if elapsed >= 1.0:
                    self.message_rate = self.message_count / elapsed
                    self.message_count = 0
                    self.last_check = now
                    
                    if self.message_rate > CONFIG["device"]["max_message_rate"]:
                        logging.warning(f"High message rate detected: {self.message_rate:.2f} msg/sec")
                        self.server.throttle_messages()
                
                time.sleep(5)
                
            except Exception as e:
                logging.error(f"Resource monitor error: {str(e)}", exc_info=True)
                time.sleep(10)

class DashboardGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MQTT RFID Server - Optimized")
        self.root.geometry("1200x800")
        
        # Configure styles
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('TFrame', background='#f0f0f0')
        self.style.configure('TLabel', background='#f0f0f0', font=('Helvetica', 10))
        self.style.configure('TButton', padding=5, font=('Helvetica', 10))
        self.style.configure('Red.TLabel', foreground='red', font=('Helvetica', 10, 'bold'))
        self.style.configure('Green.TLabel', foreground='green', font=('Helvetica', 10, 'bold'))
        self.style.configure('Yellow.TLabel', foreground='orange', font=('Helvetica', 10, 'bold'))
        self.style.configure('Title.TLabel', font=('Helvetica', 12, 'bold'))
        self.style.map('TButton',
            foreground=[('disabled', 'gray'), ('active', 'white')],
            background=[('disabled', '#f0f0f0'), ('active', '#3c8dbc')]
        )
        
        # Main container
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Connection status bar
        self.connection_frame = ttk.Frame(self.main_frame)
        self.connection_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.connection_status = ttk.Label(
            self.connection_frame,
            text="Initializing...",
            style='Title.TLabel'
        )
        self.connection_status.pack(side=tk.LEFT)
        
        self.resource_status = ttk.Label(
            self.connection_frame,
            text="CPU: 0% | MEM: 0MB",
            style='Title.TLabel'
        )
        self.resource_status.pack(side=tk.RIGHT)
        
        # Stats bar
        self.stats_frame = ttk.Frame(self.main_frame)
        self.stats_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.device_count_label = ttk.Label(self.stats_frame, text="Devices: 0")
        self.device_count_label.pack(side=tk.LEFT)
        
        self.message_stats_label = ttk.Label(self.stats_frame, text="Messages: 0/sec | Total: 0")
        self.message_stats_label.pack(side=tk.RIGHT)
        
        # Notebook (tabs)
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Dashboard Tab
        self.setup_dashboard_tab()
        
        # Devices Tab
        self.setup_devices_tab()
        
        # Messages Tab
        self.setup_messages_tab()
        
        # Errors Tab
        self.setup_errors_tab()
        
        # Status bar
        self.status_bar = ttk.Label(self.main_frame, text="Ready", relief=tk.SUNKEN)
        self.status_bar.pack(fill=tk.X, pady=(5, 0))
        
        # Control buttons
        self.button_frame = ttk.Frame(self.main_frame)
        self.button_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.start_button = ttk.Button(
            self.button_frame,
            text="Start Server",
            command=self.start_server
        )
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        self.stop_button = ttk.Button(
            self.button_frame,
            text="Stop Server",
            state=tk.DISABLED,
            command=self.stop_server
        )
        self.stop_button.pack(side=tk.LEFT, padx=5)
        
        # Initialize variables
        self.message_count = {'received': 0, 'sent': 0}
        self.message_rate = 0
        self.start_time = None
        self.server = None
        
    def setup_dashboard_tab(self):
        self.dashboard_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.dashboard_tab, text="Dashboard")
        
        # Connection status group
        connection_group = ttk.LabelFrame(self.dashboard_tab, text="Connection Status", padding=10)
        connection_group.pack(fill=tk.X, pady=(0, 10))
        
        self.connection_icon = ttk.Label(connection_group, text="🔴", font=('Helvetica', 24))
        self.connection_icon.pack()
        
        self.broker_info = ttk.Label(connection_group, text="Broker: Not connected")
        self.broker_info.pack()
        
        # Stats group
        stats_group = ttk.LabelFrame(self.dashboard_tab, text="Statistics", padding=10)
        stats_group.pack(fill=tk.X, pady=(0, 10))
        
        stats_frame = ttk.Frame(stats_group)
        stats_frame.pack(fill=tk.X)
        
        # Left stats
        left_stats = ttk.Frame(stats_frame)
        left_stats.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.uptime_label = ttk.Label(left_stats, text="Uptime: 00:00:00")
        self.uptime_label.pack(anchor=tk.W)
        
        self.threads_label = ttk.Label(left_stats, text="Active Threads: 0")
        self.threads_label.pack(anchor=tk.W)
        
        # Right stats
        right_stats = ttk.Frame(stats_frame)
        right_stats.pack(side=tk.RIGHT, fill=tk.X, expand=True)
        
        self.last_message_label = ttk.Label(right_stats, text="Last Message: None")
        self.last_message_label.pack(anchor=tk.W)
        
        self.throughput_label = ttk.Label(right_stats, text="Message Rate: 0 msg/sec")
        self.throughput_label.pack(anchor=tk.W)
        
        # Recent messages
        recent_frame = ttk.Frame(self.dashboard_tab)
        recent_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(recent_frame, text="Recent Messages:").pack(anchor=tk.W)
        
        self.recent_messages = scrolledtext.ScrolledText(
            recent_frame,
            height=8,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=('Courier New', 9)
        )
        self.recent_messages.pack(fill=tk.BOTH, expand=True)
        
    def setup_devices_tab(self):
        self.devices_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.devices_tab, text="Devices")
        
        # Create treeview with scrollbars
        tree_frame = ttk.Frame(self.devices_tab)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        self.devices_tree = ttk.Treeview(
            tree_frame,
            columns=('mac_address', 'last_seen', 'status', 'messages', 'ip_address'),
            show='headings',
            selectmode='browse'
        )
        
        # Define headings
        self.devices_tree.heading('mac_address', text='MAC Address')
        self.devices_tree.heading('last_seen', text='Last Seen')
        self.devices_tree.heading('status', text='Status')
        self.devices_tree.heading('messages', text='Messages')
        self.devices_tree.heading('ip_address', text='IP Address')
        
        # Configure column widths
        self.devices_tree.column('mac_address', width=200, anchor=tk.W)
        self.devices_tree.column('last_seen', width=150, anchor=tk.W)
        self.devices_tree.column('status', width=100, anchor=tk.W)
        self.devices_tree.column('messages', width=80, anchor=tk.CENTER)
        self.devices_tree.column('ip_address', width=150, anchor=tk.W)
        
        # Add scrollbars
        yscroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.devices_tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.devices_tree.xview)
        self.devices_tree.configure(yscroll=ysroll.set, xscroll=xscroll.set)
        
        # Grid layout
        self.devices_tree.grid(row=0, column=0, sticky=tk.NSEW)
        yscroll.grid(row=0, column=1, sticky=tk.NS)
        xscroll.grid(row=1, column=0, sticky=tk.EW)
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        # Context menu
        self.devices_menu = tk.Menu(self.devices_tree, tearoff=0)
        self.devices_menu.add_command(label="Refresh", command=self.refresh_devices)
        self.devices_menu.add_command(label="Disconnect", command=self.disconnect_device)
        
        self.devices_tree.bind("<Button-3>", self.show_devices_menu)
        
    def setup_messages_tab(self):
        self.messages_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.messages_tab, text="Messages")
        
        # Create text widget with scrollbars
        self.message_log = scrolledtext.ScrolledText(
            self.messages_tab,
            wrap=tk.WORD,
            font=('Courier New', 9),
            state=tk.DISABLED
        )
        self.message_log.pack(fill=tk.BOTH, expand=True)
        
        # Add control buttons
        button_frame = ttk.Frame(self.messages_tab)
        button_frame.pack(fill=tk.X, pady=(5, 0))
        
        clear_button = ttk.Button(
            button_frame,
            text="Clear Log",
            command=self.clear_message_log
        )
        clear_button.pack(side=tk.LEFT, padx=5)
        
        pause_button = ttk.Button(
            button_frame,
            text="Pause",
            command=self.toggle_pause_log
        )
        pause_button.pack(side=tk.LEFT, padx=5)
        
        self.log_paused = False
        
    def setup_errors_tab(self):
        self.errors_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.errors_tab, text="Errors")
        
        # Create treeview with scrollbars
        tree_frame = ttk.Frame(self.errors_tab)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        self.errors_tree = ttk.Treeview(
            tree_frame,
            columns=('timestamp', 'type', 'message', 'mac', 'rfid'),
            show='headings',
            selectmode='browse'
        )
        
        # Define headings
        self.errors_tree.heading('timestamp', text='Timestamp')
        self.errors_tree.heading('type', text='Error Type')
        self.errors_tree.heading('message', text='Message')
        self.errors_tree.heading('mac', text='MAC Address')
        self.errors_tree.heading('rfid', text='RFID')
        
        # Configure column widths
        self.errors_tree.column('timestamp', width=150, anchor=tk.W)
        self.errors_tree.column('type', width=150, anchor=tk.W)
        self.errors_tree.column('message', width=300, anchor=tk.W)
        self.errors_tree.column('mac', width=150, anchor=tk.W)
        self.errors_tree.column('rfid', width=150, anchor=tk.W)
        
        # Add scrollbars
        yscroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.errors_tree.yview)
        xscroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.errors_tree.xview)
        self.errors_tree.configure(yscroll=ysroll.set, xscroll=xscroll.set)
        
        # Grid layout
        self.errors_tree.grid(row=0, column=0, sticky=tk.NSEW)
        yscroll.grid(row=0, column=1, sticky=tk.NS)
        xscroll.grid(row=1, column=0, sticky=tk.EW)
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        # Add refresh button
        button_frame = ttk.Frame(self.errors_tab)
        button_frame.pack(fill=tk.X, pady=(5, 0))
        
        refresh_button = ttk.Button(
            button_frame,
            text="Refresh Errors",
            command=self.refresh_errors
        )
        refresh_button.pack(side=tk.LEFT, padx=5)
        
        clear_button = ttk.Button(
            button_frame,
            text="Clear Errors",
            command=self.clear_errors
        )
        clear_button.pack(side=tk.LEFT, padx=5)
        
    def show_devices_menu(self, event):
        item = self.devices_tree.identify_row(event.y)
        if item:
            self.devices_tree.selection_set(item)
            self.devices_menu.post(event.x_root, event.y_root)
        
    def refresh_devices(self):
        if self.server:
            self.server.refresh_device_status()
        
    def disconnect_device(self):
        selected_item = self.devices_tree.selection()
        if selected_item:
            mac_address = self.devices_tree.item(selected_item, 'values')[0]
            if self.server:
                self.server.disconnect_device(mac_address)
        
    def clear_message_log(self):
        self.message_log.config(state=tk.NORMAL)
        self.message_log.delete(1.0, tk.END)
        self.message_log.config(state=tk.DISABLED)
        
    def toggle_pause_log(self):
        self.log_paused = not self.log_paused
        self.message_log.config(state=tk.NORMAL if self.log_paused else tk.DISABLED)
        
    def refresh_errors(self):
        if self.server:
            self.server.refresh_error_logs()
        
    def clear_errors(self):
        self.errors_tree.delete(*self.errors_tree.get_children())
        
    def update_connection_status(self, status, is_connected):
        color = "green" if is_connected else "red"
        self.connection_status.config(text=f"Status: {status}", foreground=color)
        
        if is_connected:
            self.connection_icon.config(text="🟢")
            self.broker_info.config(text=f"Broker: {CONFIG['mqtt']['broker']}:{CONFIG['mqtt']['port']}")
            self.start_time = datetime.now()
            self.start_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
        else:
            self.connection_icon.config(text="🔴")
            self.broker_info.config(text="Broker: Not connected")
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            
    def update_resource_usage(self, cpu_percent, mem_usage):
        self.resource_status.config(text=f"CPU: {cpu_percent}% | MEM: {mem_usage:.1f}MB")
            
    def update_device_count(self, count):
        self.device_count_label.config(text=f"Devices: {count}")
        
    def add_message(self, topic, message, direction):
        if self.log_paused:
            return
            
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        direction_icon = "⬇️" if direction == "in" else "⬆️"
        color = "blue" if direction == "in" else "green"
        
        # Add to recent messages (dashboard)
        recent_msg = f"{timestamp} {direction_icon} {topic}: {message[:100]}"
        self.recent_messages.config(state=tk.NORMAL)
        self.recent_messages.insert(tk.END, recent_msg + "\n")
        self.recent_messages.config(state=tk.DISABLED)
        self.recent_messages.see(tk.END)
        
        # Add to full message log
        full_msg = f"{timestamp} {direction_icon} {topic}\n{message}\n{'-'*80}\n"
        self.message_log.config(state=tk.NORMAL)
        self.message_log.tag_config(direction, foreground=color)
        self.message_log.insert(tk.END, full_msg, direction)
        self.message_log.config(state=tk.DISABLED)
        self.message_log.see(tk.END)
        
        # Update last message label
        self.last_message_label.config(text=f"Last Message: {timestamp} ({direction_icon} {topic})")
        
    def update_device_table(self, device_data):
        # Find existing item or add new one
        mac_address = device_data['mac_address']
        children = self.devices_tree.get_children()
        item_id = None
        
        for child in children:
            if self.devices_tree.item(child, 'values')[0] == mac_address:
                item_id = child
                break
                
        values = (
            device_data['mac_address'],
            device_data['last_seen'],
            device_data['status'],
            device_data['message_count'],
            device_data.get('ip_address', 'N/A')
        )
        
        if item_id:
            self.devices_tree.item(item_id, values=values)
        else:
            item_id = self.devices_tree.insert('', tk.END, values=values)
            
        # Color status
        if device_data['status'] == "Active":
            self.devices_tree.tag_configure('active', background='#d4edda')
            self.devices_tree.item(item_id, tags=('active',))
        elif device_data['status'] == "Inactive":
            self.devices_tree.tag_configure('inactive', background='#fff3cd')
            self.devices_tree.item(item_id, tags=('inactive',))
        else:
            self.devices_tree.tag_configure('error', background='#f8d7da')
            self.devices_tree.item(item_id, tags=('error',))
            
    def add_error_to_table(self, error_data):
        """Add a new error to the errors treeview"""
        self.errors_tree.insert('', tk.END, values=(
            error_data.get('timestamp', ''),
            error_data.get('type', ''),
            error_data.get('message', ''),
            error_data.get('mac_address', ''),
            error_data.get('rfid', '')
        ))
            
    def update_message_stats(self, received, sent, rate):
        self.message_stats_label.config(
            text=f"Messages: {rate:.1f}/sec | Total: R:{received} S:{sent}"
        )
        
    def update_uptime(self):
        if self.start_time:
            uptime = datetime.now() - self.start_time
            hours, remainder = divmod(uptime.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            self.uptime_label.config(text=f"Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}")
        self.root.after(1000, self.update_uptime)
            
    def update_thread_count(self, count):
        self.threads_label.config(text=f"Active Threads: {count}")
        
    def start_server(self):
        if not self.server:
            self.server = MQTTServer(self)
            if self.server.start():
                self.update_connection_status("Connected", True)
            else:
                self.server = None
                
    def stop_server(self):
        if self.server:
            self.server.stop()
            self.server = None
            self.update_connection_status("Disconnected", False)

class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.lock = threading.Lock()
        self.initialize_pool()
    
    def initialize_pool(self):
        try:
            dsn = cx_Oracle.makedsn(
                CONFIG["database"]["host"],
                CONFIG["database"]["port"],
                service_name=CONFIG["database"]["service_name"]
            )
            
            self.pool = cx_Oracle.SessionPool(
                CONFIG["database"]["username"],
                CONFIG["database"]["password"],
                dsn,
                min=CONFIG["database"]["pool"]["min"],
                max=CONFIG["database"]["pool"]["max"],
                increment=CONFIG["database"]["pool"]["increment"],
                threaded=True,
                timeout=CONFIG["database"]["pool"]["timeout"],
                encoding="UTF-8"
            )
            
            # Test the connection
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    if cursor.fetchone()[0] != 1:
                        raise Exception("Database test query failed")
            
            logging.info("Database connection pool initialized successfully")
            
        except Exception as e:
            error_msg = f"Failed to initialize database pool: {str(e)}"
            logging.error(error_msg, exc_info=True)
            raise Exception(error_msg)
    
    def get_connection(self):
        """Get a connection from the pool with validation"""
        with self.lock:
            try:
                conn = self.pool.acquire()
                
                # Validate connection
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1 FROM DUAL")
                        if cursor.fetchone()[0] != 1:
                            conn.close()
                            raise Exception("Connection validation failed")
                except:
                    conn.close()
                    raise
                
                return conn
            except Exception as e:
                error_msg = f"Failed to get database connection: {str(e)}"
                logging.error(error_msg, exc_info=True)
                raise Exception(error_msg)
    
    def log_error(self, error_type, error_message, error_details=None, 
                 mac_address=None, rfid=None, topic=None, 
                 message_content=None, stack_trace=None):
        """Log error to RFID_SYSTEM_ERROR_LOGS table"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"INSERT INTO {CONFIG['tables']['error_logs']} "
                        "(ERROR_TYPE, ERROR_MESSAGE, ERROR_DETAILS, MAC_ADDRESS, "
                        "RFID, TOPIC, MESSAGE_CONTENT, STACK_TRACE, TIMESTAMP) "
                        "VALUES (:error_type, :error_message, :error_details, :mac_address, "
                        ":rfid, :topic, :message_content, :stack_trace, SYSTIMESTAMP)",
                        {
                            'error_type': error_type[:100],
                            'error_message': error_message[:4000],
                            'error_details': str(error_details)[:4000] if error_details else None,
                            'mac_address': mac_address[:20] if mac_address else None,
                            'rfid': rfid[:50] if rfid else None,
                            'topic': topic[:100] if topic else None,
                            'message_content': str(message_content)[:4000] if message_content else None,
                            'stack_trace': str(stack_trace)[:4000] if stack_trace else None
                        }
                    )
                    conn.commit()
                    return True
        except Exception as e:
            logging.error(f"Failed to log error to database: {e}", exc_info=True)
            return False

    def close(self):
        """Close the connection pool"""
        if self.pool:
            try:
                self.pool.close()
                logging.info("Database connection pool closed")
            except Exception as e:
                logging.error(f"Error closing database pool: {e}")

class MQTTServer:
    def __init__(self, gui):
        self.gui = gui
        self.db_manager = DatabaseManager()
        self.thread_pool = ThreadPoolExecutor(
            max_workers=CONFIG["threading"]["max_workers"],
            thread_name_prefix="mqtt_worker",
            maxsize=CONFIG["threading"]["queue_size"]
        )
        self.connected_devices = set()
        self.device_last_seen = {}
        self.device_message_count = defaultdict(int)
        self.device_ip_address = {}
        self.message_count = {'received': 0, 'sent': 0}
        self.message_rate = 0
        self.client = None
        self.connection_status = "Initializing"
        self.running = False
        self.lock = threading.Lock()
        self.resource_monitor = ResourceMonitor(self)
        
        # Initialize GUI
        self.gui.update_connection_status(self.connection_status, False)
        
    def device_heartbeat(self, mac_address, ip_address=None):
        """Update device last seen timestamp and notify GUI"""
        with self.lock:
            self.device_last_seen[mac_address] = datetime.now()
            
            if ip_address:
                self.device_ip_address[mac_address] = ip_address
                
            if mac_address not in self.connected_devices:
                self.connected_devices.add(mac_address)
                logging.info(f"New device connected: {mac_address}")
                self.gui.update_device_count(len(self.connected_devices))
            
            # Update device table
            device_data = {
                'mac_address': mac_address,
                'last_seen': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'status': "Active",
                'message_count': self.device_message_count[mac_address],
                'ip_address': self.device_ip_address.get(mac_address, 'N/A')
            }
            
            self.gui.update_device_table(device_data)
    
    def check_device_timeouts(self):
        """Check for inactive devices and update GUI"""
        if not self.running:
            return
            
        try:
            timeout = datetime.now() - timedelta(minutes=CONFIG["device"]["timeout_minutes"])
            inactive_devices = []
            
            with self.lock:
                for mac, last_seen in list(self.device_last_seen.items()):
                    if last_seen < timeout:
                        inactive_devices.append(mac)
                        self.connected_devices.discard(mac)
                        del self.device_last_seen[mac]
                        if mac in self.device_ip_address:
                            del self.device_ip_address[mac]
                        
                        # Update device table with inactive status
                        device_data = {
                            'mac_address': mac,
                            'last_seen': last_seen.strftime("%Y-%m-%d %H:%M:%S"),
                            'status': "Inactive",
                            'message_count': self.device_message_count[mac],
                            'ip_address': self.device_ip_address.get(mac, 'N/A')
                        }
                        self.gui.update_device_table(device_data)
            
            if inactive_devices:
                logging.info(f"Devices timed out: {', '.join(inactive_devices)}")
                self.gui.update_device_count(len(self.connected_devices))
                
                # Log the timeout events
                for mac in inactive_devices:
                    self.db_manager.log_error(
                        error_type="Device Timeout",
                        error_message=f"Device {mac} timed out",
                        mac_address=mac,
                        error_details=f"Last seen at {last_seen}"
                    )
        
        except Exception as e:
            logging.error(f"Error checking device timeouts: {e}", exc_info=True)
            self.db_manager.log_error(
                error_type="Device Timeout Check",
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
        
        finally:
            # Schedule next check if still running
            if self.running:
                threading.Timer(60.0, self.check_device_timeouts).start()
    
    def increment_message_count(self, direction='received'):
        """Increment message count and calculate rate"""
        with self.lock:
            self.message_count[direction] += 1
            self.resource_monitor.message_count += 1
            
        # Update GUI stats periodically
        now = time.time()
        if hasattr(self, 'last_stats_update'):
            if now - self.last_stats_update >= 1.0:
                with self.lock:
                    rate = self.resource_monitor.message_rate
                    self.gui.update_message_stats(
                        self.message_count['received'],
                        self.message_count['sent'],
                        rate
                    )
                    self.last_stats_update = now
        else:
            self.last_stats_update = now
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connection_status = "Connected to broker"
            logging.info("Connected to MQTT broker")
            self.gui.update_connection_status(self.connection_status, True)
            
            # Subscribe to topics with proper QoS
            subscribe_topics = [
                ("nodemcu/rfid", 1),
                ("nodemcu/+/heartbeat", 1),
                ("nodemcu/+/status", 1)
            ]
            
            client.subscribe(subscribe_topics)
            
            # Publish server status with retain flag
            client.publish("nodemcu/server/status", "online", qos=2, retain=True)
            self.increment_message_count('sent')
            
            # Start resource monitoring
            self.resource_monitor.start()
            
        else:
            self.connection_status = f"Connection failed (code {rc})"
            error_message = f"MQTT connection failed with code {rc}"
            logging.error(error_message)
            self.db_manager.log_error(
                error_type="MQTT Connection",
                error_message=error_message,
                error_details=f"Connection return code: {rc}"
            )
            self.gui.update_connection_status(self.connection_status, False)
            
            # Attempt reconnect
            if self.running:
                threading.Timer(CONFIG["mqtt"]["reconnect_delay"], self.reconnect_client).start()
    
    def on_disconnect(self, client, userdata, rc):
        self.connection_status = "Disconnected from broker"
        error_message = f"Disconnected with code {rc}"
        logging.warning(error_message)
        self.db_manager.log_error(
            error_type="MQTT Disconnection",
            error_message=error_message,
            error_details=f"Disconnection return code: {rc}"
        )
        self.gui.update_connection_status(self.connection_status, False)
        
        # Attempt reconnect if we didn't initiate the disconnect
        if self.running and rc != 0:
            threading.Timer(CONFIG["mqtt"]["reconnect_delay"], self.reconnect_client).start()
    
    def reconnect_client(self):
        """Attempt to reconnect the MQTT client"""
        if not self.running or not self.client:
            return
            
        try:
            logging.info("Attempting to reconnect to MQTT broker...")
            self.client.reconnect()
        except Exception as e:
            logging.error(f"Reconnect failed: {e}")
            self.db_manager.log_error(
                error_type="MQTT Reconnect",
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            
            # Schedule another reconnect attempt
            if self.running:
                threading.Timer(CONFIG["mqtt"]["reconnect_delay"], self.reconnect_client).start()
    
    def on_message(self, client, userdata, msg):
        try:
            self.increment_message_count('received')
            
            payload = msg.payload.decode().strip()
            topic = msg.topic
            
            # Extract IP address from message if available
            ip_address = None
            if hasattr(msg, 'ip_address'):
                ip_address = msg.ip_address
            
            # Notify GUI of incoming message
            self.gui.add_message(topic, payload, "in")
            
            # Handle heartbeat messages first
            if "heartbeat" in topic.lower():
                mac_address = topic.split('/')[1]
                self.device_heartbeat(mac_address, ip_address)
                
                try:
                    # Try to parse as JSON
                    json_msg = json.loads(payload)
                    if "timestamp" in json_msg:
                        logging.debug(f"JSON heartbeat from {mac_address}, timestamp: {json_msg['timestamp']}")
                        return
                except json.JSONDecodeError:
                    # Not JSON, treat as text
                    logging.debug(f"Text heartbeat from {mac_address}")
                    self.db_manager.log_error(
                        error_type="Heartbeat Format",
                        error_message="Non-JSON heartbeat received",
                        error_details=payload,
                        mac_address=mac_address
                    )
                return
                
            # Process the message in a thread
            future = self.thread_pool.submit(self.process_message, msg)
            future.add_done_callback(self.handle_process_result)
            
        except Exception as e:
            error_message = f"Error in on_message handler: {str(e)}"
            logging.error(error_message, exc_info=True)
            self.db_manager.log_error(
                error_type="Message Handling",
                error_message=error_message,
                error_details=f"Topic: {topic}, Payload: {payload}",
                stack_trace=traceback.format_exc()
            )

    def handle_process_result(self, future):
        """Handle the result of message processing"""
        try:
            future.result()  # This will re-raise any exceptions from the process
        except Exception as e:
            logging.error(f"Message processing failed: {e}", exc_info=True)

    def process_message(self, msg):
        """Process an MQTT message in a thread"""
        try:
            payload = msg.payload.decode().strip()
            topic = msg.topic
            client = msg._client  # Access the client from the message
            
            # Handle login status
            if payload.startswith("loginstatus"):
                mac_address = payload.split()[1]
                response_topic = f"nodemcu/{mac_address}/response"
                status = self.check_mac_login_status(mac_address)
                response = "LOW" if status else "HIGH"
                client.publish(response_topic, response, qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, response, "out")
                logging.info(f"Login status for {mac_address}: {'Logged in' if status else 'Not logged in'}")
                return
                
            # Handle workstation status
            if payload.startswith("workstationstatus"):
                mac_address = payload.split()[1]
                response_topic = f"nodemcu/{mac_address}/response"
                
                # First check if there's an operator logged in
                operator_logged_in = self.check_mac_login_status(mac_address)
                if not operator_logged_in:
                    response = CONFIG["responses"]["no_operator"]
                    client.publish(response_topic, response, qos=1)
                    self.increment_message_count('sent')
                    self.gui.add_message(response_topic, response, "out")
                    logging.info(f"No operator logged in at {mac_address}, skipping status check")
                    
                    # Log the no operator event
                    self.db_manager.log_error(
                        error_type="Workstation Status",
                        error_message="No operator logged in",
                        mac_address=mac_address,
                        error_details="Workstation status requested but no operator logged in"
                    )
                    return
                    
                # If operator is logged in, get the status
                response = self.get_workstation_status(mac_address)
                client.publish(response_topic, response, qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, response, "out")
                logging.info(f"Workstation status for {mac_address}: {response}")
                return

            # Process RFID scans
            match = re.search(r'ID:\s*([0-9A-Fa-f]+)\s*Mac ID:\s*([0-9A-Fa-f:]+)', payload)
            if not match:
                error_message = f"Unrecognized message format: {payload}"
                logging.warning(error_message)
                self.db_manager.log_error(
                    error_type="Message Format",
                    error_message=error_message,
                    error_details=payload,
                    topic=topic
                )
                return

            rfid, mac_address = match.groups()
            self.device_heartbeat(mac_address)
            self.device_message_count[mac_address] += 1
            response_topic = f"nodemcu/{mac_address}/response"
            logging.info(f"RFID Scan - Card: {rfid}, Device: {mac_address}")

            if self.is_employee_card(rfid):
                self.process_employee_scan(rfid, mac_address, client, response_topic)
            elif self.is_bundle_card(rfid):
                self.process_bundle_scan(rfid, mac_address, client, response_topic)
            else:
                response = CONFIG["responses"]["unauthorized"]
                client.publish(response_topic, response, qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, response, "out")
                logging.warning(f"Unauthorized card: {rfid}")
                self.db_manager.log_error(
                    error_type="Authorization",
                    error_message="Unauthorized RFID card scanned",
                    mac_address=mac_address,
                    rfid=rfid,
                    error_details="Card not found in employee or bundle systems"
                )
            
        except Exception as e:
            error_message = f"Message processing error: {str(e)}"
            logging.error(error_message, exc_info=True)
            self.db_manager.log_error(
                error_type="Message Processing",
                error_message=error_message,
                error_details=f"Topic: {topic}, Payload: {payload}",
                stack_trace=traceback.format_exc()
            )
            
            # Send error response if we have the client and topic
            if 'response_topic' in locals() and client:
                response = CONFIG["responses"]["error_generic"]
                client.publish(response_topic, response, qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, response, "out")

    def process_employee_scan(self, rfid, mac_address, client, response_topic):
        """Process an employee RFID scan"""
        if self.is_rfid_already_logged_in(rfid, mac_address):
            response = CONFIG["responses"]["login_exists"]
            logging.info(f"Employee {rfid} already logged in")
            
            # Log the duplicate login attempt
            self.db_manager.log_error(
                error_type="Duplicate Login",
                error_message=f"Employee {rfid} already logged in",
                mac_address=mac_address,
                rfid=rfid,
                error_details=f"Duplicate login attempt at {mac_address}"
            )
        else:
            if self.insert_employee_login(rfid, mac_address):
                response = CONFIG["responses"]["login_success"]
                # First send LOW signal (matches NodeMCU code)
                client.publish(response_topic, "LOW", qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, "LOW", "out")
                # Then send the success message
                client.publish(response_topic, response, qos=1)
                self.increment_message_count('sent')
                self.gui.add_message(response_topic, response, "out")
                logging.info(f"Employee {rfid} login successful")
            else:
                response = CONFIG["responses"]["error_generic"]
                error_message = f"Failed to log in employee {rfid}"
                logging.error(error_message)
                self.db_manager.log_error(
                    error_type="Login Failure",
                    error_message=error_message,
                    mac_address=mac_address,
                    rfid=rfid,
                    stack_trace=traceback.format_exc()
                )
        
        client.publish(response_topic, response, qos=1)
        self.increment_message_count('sent')
        self.gui.add_message(response_topic, response, "out")

    def process_bundle_scan(self, rfid, mac_address, client, response_topic):
        """Process a bundle RFID scan"""
        if not self.check_mac_login_status(mac_address):
            response = CONFIG["responses"]["login_required"]
            logging.warning("Operator login required")
            self.db_manager.log_error(
                error_type="Authorization",
                error_message="Operator login required for bundle scan",
                mac_address=mac_address,
                rfid=rfid
            )
            client.publish(response_topic, response, qos=1)
            self.increment_message_count('sent')
            self.gui.add_message(response_topic, response, "out")
            return

        # Check if bundle is active on another MAC address
        other_mac = self.is_bundle_active_on_other_mac(rfid, mac_address)
        if other_mac:
            response = f"{CONFIG['responses']['bundle_active_elsewhere']}{other_mac}"
            error_message = f"Bundle {rfid} is already active on terminal {other_mac}"
            logging.warning(error_message)
            self.db_manager.log_error(
                error_type="Bundle Conflict",
                error_message=error_message,
                mac_address=mac_address,
                rfid=rfid,
                error_details=f"Bundle already active on {other_mac}"
            )
            client.publish(response_topic, response, qos=1)
            self.increment_message_count('sent')
            self.gui.add_message(response_topic, response, "out")
            return

        # Check if another bundle is active on this MAC
        if self.is_other_bundle_active(mac_address, rfid):
            response = CONFIG["responses"]["previous_bundle_active"]
            logging.warning("Previous bundle not completed")
            self.db_manager.log_error(
                error_type="Bundle Conflict",
                error_message="Previous bundle not completed",
                mac_address=mac_address,
                rfid=rfid,
                error_details="Attempted to start new bundle while previous one active"
            )
            client.publish(response_topic, response, qos=1)
            self.increment_message_count('sent')
            self.gui.add_message(response_topic, response, "out")
            return

        current_bundle_id = self.get_bundle_id(rfid)
        if current_bundle_id is None:
            response = CONFIG["responses"]["error_generic"]
            error_message = f"No bundle found for RFID: {rfid}"
            logging.error(error_message)
            self.db_manager.log_error(
                error_type="Bundle Error",
                error_message=error_message,
                mac_address=mac_address,
                rfid=rfid
            )
            client.publish(response_topic, response, qos=1)
            self.increment_message_count('sent')
            self.gui.add_message(response_topic, response, "out")
            return

        # Check if this bundle_id has been scanned before
        if self.is_bundle_already_scanned(current_bundle_id, mac_address):
            if self.is_bundle_active(current_bundle_id, mac_address):
                if self.update_bundle_end_time(current_bundle_id, mac_address):
                    response = CONFIG["responses"]["bundle_ended"]
                    logging.info(f"Bundle {rfid} ended")
                else:
                    response = CONFIG["responses"]["error_generic"]
                    error_message = "Error ending bundle"
                    logging.error(error_message)
                    self.db_manager.log_error(
                        error_type="Bundle Error",
                        error_message=error_message,
                        mac_address=mac_address,
                        rfid=rfid,
                        stack_trace=traceback.format_exc()
                    )
            else:
                response = CONFIG["responses"]["bundle_completed"]
                logging.info("Bundle already completed")
        else:
            if self.insert_bundle_scan(rfid, mac_address, current_bundle_id):
                response = CONFIG["responses"]["bundle_started"]
                logging.info(f"Bundle {rfid} started")
            else:
                response = CONFIG["responses"]["error_generic"]
                error_message = "Error starting bundle"
                logging.error(error_message)
                self.db_manager.log_error(
                    error_type="Bundle Error",
                    error_message=error_message,
                    mac_address=mac_address,
                    rfid=rfid,
                    stack_trace=traceback.format_exc()
                )

        client.publish(response_topic, response, qos=1)
        self.increment_message_count('sent')
        self.gui.add_message(response_topic, response, "out")
        logging.info(f"Response sent: {response}")

    # Database operation methods remain largely the same as in your original code
    # but with additional error handling and logging
    
    def setup_mqtt_client(self):
        """Create and configure the MQTT client"""
        self.client = mqtt.Client(
            client_id=CONFIG["mqtt"]["client_id"],
            clean_session=False,
            protocol=mqtt.MQTTv311,
            transport="tcp"
        )
        
        # Configure client settings
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # Set will message
        self.client.will_set(
            "nodemcu/server/status",
            payload="offline",
            qos=2,
            retain=True
        )
        
        # Configure message handling
        self.client.max_inflight_messages_set(CONFIG["mqtt"]["max_inflight_messages"])
        self.client.max_queued_messages_set(CONFIG["mqtt"]["max_queued_messages"])
        
        # Configure reconnect behavior
        self.client.reconnect_delay_set(
            min_delay=1,
            max_delay=CONFIG["mqtt"]["reconnect_delay"]
        )
        
        return self.client

    def start(self):
        """Start the MQTT server"""
        try:
            logging.info("Starting MQTT RFID Server")
            self.running = True
            self.client = self.setup_mqtt_client()
            
            self.client.connect(
                CONFIG["mqtt"]["broker"],
                port=CONFIG["mqtt"]["port"],
                keepalive=CONFIG["mqtt"]["keepalive"]
            )
            
            # Start network loop
            self.client.loop_start()
            
            # Start device timeout checker
            self.check_device_timeouts()
            
            return True
            
        except Exception as e:
            error_message = f"Failed to start server: {e}"
            logging.error(error_message, exc_info=True)
            self.db_manager.log_error(
                error_type="Server Startup",
                error_message=error_message,
                stack_trace=traceback.format_exc()
            )
            self.running = False
            return False
    
    def stop(self):
        """Stop the MQTT server"""
        logging.info("Shutting down server...")
        self.running = False
        
        try:
            if self.client:
                # Publish offline status before disconnecting
                self.client.publish("nodemcu/server/status", "offline", qos=2, retain=True)
                self.increment_message_count('sent')
                
                # Disconnect cleanly
                self.client.disconnect()
                self.client.loop_stop()
                
            # Stop resource monitoring
            self.resource_monitor.stop()
            
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Close database pool
            self.db_manager.close()
            
            logging.info("Server shutdown complete")
            
        except Exception as e:
            error_message = f"Error during shutdown: {e}"
            logging.error(error_message, exc_info=True)
            self.db_manager.log_error(
                error_type="Server Shutdown",
                error_message=error_message,
                stack_trace=traceback.format_exc()
            )
    
    def throttle_messages(self):
        """Reduce message processing rate when threshold exceeded"""
        logging.warning("Message rate threshold exceeded, throttling messages")
        # Reduce thread pool size temporarily
        self.thread_pool._max_workers = max(10, self.thread_pool._max_workers // 2)
        
    def refresh_device_status(self):
        """Refresh all device statuses in the GUI"""
        with self.lock:
            for mac in list(self.connected_devices):
                device_data = {
                    'mac_address': mac,
                    'last_seen': self.device_last_seen.get(mac, datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                    'status': "Active",
                    'message_count': self.device_message_count.get(mac, 0),
                    'ip_address': self.device_ip_address.get(mac, 'N/A')
                }
                self.gui.update_device_table(device_data)
                
    def refresh_error_logs(self):
        """Refresh error logs from database"""
        try:
            self.gui.errors_tree.delete(*self.gui.errors_tree.get_children())
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'), ERROR_TYPE, "
                        "ERROR_MESSAGE, MAC_ADDRESS, RFID "
                        f"FROM {CONFIG['tables']['error_logs']} "
                        "ORDER BY TIMESTAMP DESC LIMIT 1000"
                    )
                    
                    for row in cursor:
                        self.gui.errors_tree.insert('', tk.END, values=row)
                        
        except Exception as e:
            logging.error(f"Error refreshing error logs: {e}", exc_info=True)
            
    def disconnect_device(self, mac_address):
        """Force disconnect a device"""
        with self.lock:
            if mac_address in self.connected_devices:
                self.connected_devices.remove(mac_address)
                if mac_address in self.device_last_seen:
                    del self.device_last_seen[mac_address]
                if mac_address in self.device_ip_address:
                    del self.device_ip_address[mac_address]
                    
                logging.info(f"Forcefully disconnected device: {mac_address}")
                self.gui.update_device_count(len(self.connected_devices))
                
                # Update device table
                device_data = {
                    'mac_address': mac_address,
                    'last_seen': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'status': "Disconnected",
                    'message_count': self.device_message_count.get(mac_address, 0),
                    'ip_address': 'N/A'
                }
                self.gui.update_device_table(device_data)
                
                return True
        return False

def main():
    try:
        root = tk.Tk()
        
        # Create the GUI
        gui = DashboardGUI(root)
        
        # Start the application
        root.mainloop()
        
    except Exception as e:
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
