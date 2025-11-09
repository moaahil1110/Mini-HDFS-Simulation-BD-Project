# config.py - Mini HDFS Configuration with ZeroTier IPs

# --- Namenode (controller) ---
# ZeroTier IP for the Namenode (Member 1)
NAMENODE_HOST = '172.29.233.31'
NAMENODE_PORT = 9000
NAMENODE_HEARTBEAT_PORT = 9001

# --- Datanode 0 ---
# ZeroTier IP for datanode0 (Member 4)
DATANODE0_HOST = '172.29.212.152'
DATANODE0_PORT = 9002
DATANODE0_STORAGE_DIR = './storage/datanode0_chunks'

# --- Datanode 1 ---
# ZeroTier IP for datanode1 (Member 3)
DATANODE1_HOST = '172.29.141.246'
DATANODE1_PORT = 9003
DATANODE1_STORAGE_DIR = './storage/datanode1_chunks'

# --- Client (Flask app) ---
# Client runs on Member 2 (ZeroTier IP shown below). Use 0.0.0.0 to bind all interfaces
# and access the UI from another machine at http://172.29.162.70:5000
CLIENT_HOST = '0.0.0.0'
CLIENT_PORT = 5000

# --- System knobs ---
CHUNK_SIZE = 2 * 1024 * 1024         # 2 MB chunks
REPLICATION_FACTOR = 2               # replicate to both DNs
HEARTBEAT_INTERVAL = 5               # seconds
HEARTBEAT_TIMEOUT = 15               # seconds

# Replication queue / retry knobs
REPL_QUEUE_FILE = './storage/repl_queue.json'
REPL_MAX_RETRIES = 5
REPL_BACKOFF_BASE = 2.0  # seconds (exponential backoff multiplier)

# When a datanode recovers, wait this many seconds before enqueuing rebuild tasks
# to allow any straggling report_chunk messages to arrive at the Namenode.
REBUILD_DELAY = 3

# --- Storage paths ---
NAMENODE_METADATA_DIR = './storage/namenode_metadata'
UPLOAD_DIR = './uploads'
