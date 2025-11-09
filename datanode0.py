import os
import socket
import threading
import time
import json
import hashlib
import logging
import signal

import config
from protocol import send_json, recv_json

# logging
logger = logging.getLogger('datanode0')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('datanode0.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())

class Datanode0:
    """
    Datanode0 stores actual chunk bytes, replies heartbeats to NN,
    verifies checksums, and serves retrieve requests.
    """

    def __init__(self):
        self.host = config.DATANODE0_HOST
        self.port = config.DATANODE0_PORT
        self.storagedir = config.DATANODE0_STORAGE_DIR
        self.namenodehost = config.NAMENODE_HOST
        self.namenodeheartbeatport = config.NAMENODE_HEARTBEAT_PORT
        self.heartbeatinterval = config.HEARTBEAT_INTERVAL

        os.makedirs(self.storagedir, exist_ok=True)
        self.chunks = {}
        self._load_chunks()
        # graceful shutdown
        self.stop_event = threading.Event()
        self.server = None

    def _chunks_path(self):
        return os.path.join(self.storagedir, 'chunks.json')

    def _load_chunks(self):
        p = self._chunks_path()
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    self.chunks = json.load(f)
            except:
                self.chunks = {}

    def _save_chunks(self):
        p = self._chunks_path()
        tmp = p + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.chunks, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

    def calculate_checksum(self, data):
        return hashlib.md5(data).hexdigest()

    # ---------- heartbeat ----------
    def send_heartbeat(self):
        # Add small jitter to avoid thundering herd during test startup
        time.sleep(0.1)
        consecutive_failures = 0
        while not getattr(self, 'stop_event', threading.Event()).is_set():
            try:
                # Log heartbeat being sent
                logger.debug(f"Sending heartbeat to {self.namenodehost}:{self.namenodeheartbeatport} (chunks={len(self.chunks)})")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                sock.connect((self.namenodehost, self.namenodeheartbeatport))
                hb = {'datanode_id': 'datanode0', 'timestamp': time.time(), 'chunks_stored': len(self.chunks)}
                send_json(sock, hb)
                sock.close()
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                # Reduce log level for transient connectivity issues; log details at debug.
                logger.debug(f"Heartbeat send failed (attempts={consecutive_failures}): {e}")
                # If we've failed many times, emit a warning to surface persistent issues
                if consecutive_failures and consecutive_failures % 12 == 0:
                    logger.warning(f"Repeated heartbeat failures ({consecutive_failures}) to {self.namenodehost}:{self.namenodeheartbeatport}")
            # sleep with simple backoff on failures
            sleep_time = min(self.heartbeatinterval * (1 + consecutive_failures * 0.2), 10)
            time.sleep(sleep_time)

    def start_heartbeat(self):
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    # ---------- actions ----------
    def handle_store_chunk(self, request, clientsocket):
        chunkid = request['chunk_id']
        chunksize = int(request['chunk_size'])

        logger.info(f"STORE request received for {chunkid} size={chunksize}")
        # handshake:
        clientsocket.sendall(b'READY')

        data = b''
        while len(data) < chunksize:
            packet = clientsocket.recv(min(8192, chunksize - len(data)))
            if not packet:
                break
            data += packet

        checksum = self.calculate_checksum(data)
        with open(os.path.join(self.storagedir, chunkid), 'wb') as f:
            f.write(data)

        self.chunks[chunkid] = {'size': len(data), 'checksum': checksum}
        self._save_chunks()

        logger.info(f"Stored chunk {chunkid} size={len(data)} checksum={checksum}")
        send_json(clientsocket, {
            'status': 'success',
            'chunk_id': chunkid,
            'checksum': checksum,
            'chunk_size': len(data)
        })
        # report to Namenode so it can persist checksum and 'stored_on' (retry a few times)
        fn = chunkid.rsplit('chunk', 1)[0]
        report_msg = {'command': 'report_chunk', 'filename': fn, 'chunk_id': chunkid, 'datanode_id': 'datanode0', 'checksum': checksum, 'chunk_size': len(data)}
        # increase retries and timeout to improve reliability in noisy test runs
        for attempt in range(1, 7):
            try:
                logger.debug(f"Reporting chunk {chunkid} to Namenode (attempt {attempt})")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(8)
                s.connect((config.NAMENODE_HOST, config.NAMENODE_PORT))
                send_json(s, report_msg)
                resp = recv_json(s)
                s.close()
                if resp.get('status') == 'success':
                    logger.info(f"Reported chunk {chunkid} to Namenode (attempt {attempt})")
                    break
                else:
                    logger.warning(f"Namenode rejected report for {chunkid} (attempt {attempt}): {resp}")
            except Exception:
                logger.exception(f'Failed to report chunk {chunkid} to Namenode (attempt {attempt})')
            # exponential backoff
            time.sleep(min(5, 0.5 * (2 ** (attempt - 1))))

    def handle_retrieve_chunk(self, request, clientsocket):
        chunkid = request['chunk_id']
        meta = self.chunks.get(chunkid)
        if not meta:
            send_json(clientsocket, {'status': 'error', 'message': 'Chunk not found'})
            return

        path = os.path.join(self.storagedir, chunkid)
        with open(path, 'rb') as f:
            chunkdata = f.read()

        checksum = self.calculate_checksum(chunkdata)
        if checksum != meta['checksum']:
            send_json(clientsocket, {'status': 'error', 'message': 'Checksum failed'})
            return

        # send header
        logger.info(f"RETRIEVE request for {chunkid}, sending header chunk_size={len(chunkdata)}")
        send_json(clientsocket, {'status': 'success', 'chunk_size': len(chunkdata), 'checksum': checksum})

        # wait ack
        _ = clientsocket.recv(3)
        logger.info(f"ACK received for {chunkid}, streaming bytes...")

        # stream bytes
        clientsocket.sendall(chunkdata)
        logger.info(f"Finished streaming {len(chunkdata)} bytes for {chunkid}")

    def handle_client(self, clientsocket, _):
        try:
            request = recv_json(clientsocket)
            action = request.get('action')

            if action == 'store':
                self.handle_store_chunk(request, clientsocket)
            elif action == 'retrieve':
                self.handle_retrieve_chunk(request, clientsocket)
            else:
                send_json(clientsocket, {'status': 'error', 'message': 'Unknown action'})
        except Exception as e:
            try:
                send_json(clientsocket, {'status': 'error', 'message': str(e)})
            except:
                pass
        finally:
            clientsocket.close()

    def run(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(50)

        self.start_heartbeat()

        logger.info(f"Listening on {self.host}:{self.port}")
        try:
            while not self.stop_event.is_set():
                try:
                    s, addr = self.server.accept()
                except OSError:
                    break
                threading.Thread(target=self.handle_client, args=(s, addr), daemon=True).start()
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info('Shutting down datanode0')
        try:
            self.stop_event.set()
            if self.server:
                try:
                    self.server.close()
                except Exception:
                    pass
            try:
                self._save_chunks()
            except Exception:
                logger.exception('Error saving chunks during shutdown')
        except Exception:
            logger.exception('Exception during datanode0 shutdown')

if __name__ == '__main__':
    dn = Datanode0()
    def _sig(signum, frame):
        dn.shutdown()
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    dn.run()
