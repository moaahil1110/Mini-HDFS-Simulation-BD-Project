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

# -------------------- Logging --------------------
logger = logging.getLogger('datanode1')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('datanode1.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())


class Datanode1:
    """
    Datanode1 stores actual chunk bytes, replies heartbeats to NN,
    verifies checksums, and serves retrieve requests.
    """

    def __init__(self):
        # --- endpoints / config ---
        self.host = config.DATANODE1_HOST
        self.port = int(config.DATANODE1_PORT)
        self.storagedir = config.DATANODE1_STORAGE_DIR

        self.namenode_host = config.NAMENODE_HOST
        self.namenode_hb_port = int(config.NAMENODE_HEARTBEAT_PORT)

        self.heartbeat_interval = int(getattr(config, 'HEARTBEAT_INTERVAL', 5))

        # --- state ---
        os.makedirs(self.storagedir, exist_ok=True)
        self.chunks = {}  # {chunk_id: {"size": int, "checksum": str}}
        self._load_chunks()

        # graceful shutdown
        self.stop_event = threading.Event()
        self.server = None

    # ---------- persistence ----------
    def _chunks_path(self):
        return os.path.join(self.storagedir, 'chunks.json')

    def _load_chunks(self):
        p = self._chunks_path()
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    self.chunks = json.load(f)
            except Exception:
                logger.exception("Failed to load chunks.json; starting empty")
                self.chunks = {}
        else:
            self.chunks = {}

    def _save_chunks(self):
        p = self._chunks_path()
        tmp = p + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.chunks, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

    # ---------- utils ----------
    @staticmethod
    def calculate_checksum(data: bytes) -> str:
        return hashlib.md5(data).hexdigest()

    # ---------- heartbeat ----------
    def _send_heartbeat_once(self):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.namenode_host, self.namenode_hb_port))
            hb = {
                'datanode_id': 'datanode1',
                'timestamp': time.time(),
                'chunks_stored': len(self.chunks),
                # help the NameNode dynamic registry:
                'host': self.host,
                'port': self.port,
            }
            send_json(sock, hb)
        except Exception:
            logger.debug("Heartbeat send failed", exc_info=True)
        finally:
            try:
                if sock:
                    sock.close()
            except Exception:
                pass

    def send_heartbeat(self):
        while not self.stop_event.is_set():
            self._send_heartbeat_once()
            time.sleep(self.heartbeat_interval)

    def start_heartbeat(self):
        threading.Thread(target=self.send_heartbeat, daemon=True, name="Heartbeat").start()

    # ---------- actions ----------
    def handle_store_chunk(self, request, clientsocket):
        """
        Robust store:
        - handshake READY
        - exact-size receive with timeout
        - atomic write via .tmp + os.replace
        - strict short-read rejection with clean JSON error
        - correct filename extraction (<filename>_chunk<i>)
        """
        chunkid = request['chunk_id']
        chunksize = int(request['chunk_size'])
        logger.info(f"STORE request received for {chunkid} size={chunksize}")

        # I/O timeout
        clientsocket.settimeout(30)

        # handshake to sender
        try:
            clientsocket.sendall(b'READY')
        except Exception as e:
            logger.error(f"Failed to send READY for {chunkid}: {e}")
            try:
                send_json(clientsocket, {'status': 'error', 'message': str(e)})
            except Exception:
                pass
            return

        final_path = os.path.join(self.storagedir, chunkid)
        tmp_path = final_path + ".tmp"

        remaining = chunksize
        hasher = hashlib.md5()

        try:
            with open(tmp_path, 'wb') as f:
                while remaining > 0:
                    packet = clientsocket.recv(min(65536, remaining))
                    if not packet:
                        # Peer closed early -> short read (reject)
                        raise RuntimeError(f"short read: expected {remaining} more bytes")
                    f.write(packet)
                    hasher.update(packet)
                    remaining -= len(packet)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, final_path)
        except Exception as e:
            # cleanup temp on any failure
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            # tell the sender cleanly, then return
            logger.error(f"STORE failed for {chunkid}: {e}")
            try:
                send_json(clientsocket, {'status': 'error', 'message': str(e)})
            except Exception:
                pass
            return

        checksum = hasher.hexdigest()

        # record meta
        self.chunks[chunkid] = {'size': chunksize, 'checksum': checksum}
        self._save_chunks()

        logger.info(f"Stored chunk {chunkid} size={chunksize} checksum={checksum}")

        # ACK to sender
        try:
            send_json(clientsocket, {
                'status': 'success',
                'chunk_id': chunkid,
                'checksum': checksum,
                'chunk_size': chunksize
            })
        except Exception:
            # if ACK fails, we still persist the chunk; sender may retry report
            logger.debug(f"Failed sending final ACK for {chunkid}", exc_info=True)

        # ---- Report to NameNode (correct filename extraction) ----
        # chunk_id is "<filename>_chunk<i>" → split EXACTLY on "_chunk"
        fn = chunkid.split('_chunk', 1)[0]

        report_msg = {
            'command': 'report_chunk',
            'filename': fn,  # correct (no trailing underscore)
            'chunk_id': chunkid,
            'datanode_id': 'datanode1',
            'checksum': checksum,
            'chunk_size': chunksize,
        }

        for attempt in range(1, 7):
            s = None
            try:
                logger.debug(f"Reporting chunk {chunkid} to NameNode (attempt {attempt})")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(8)
                s.connect((config.NAMENODE_HOST, config.NAMENODE_PORT))
                send_json(s, report_msg)
                resp = recv_json(s)
                if resp.get('status') == 'success':
                    logger.info(f"Reported chunk {chunkid} to NameNode (attempt {attempt})")
                    break
                else:
                    logger.warning(f"NameNode rejected report for {chunkid} (attempt {attempt}): {resp}")
            except Exception:
                logger.exception(f"Failed to report chunk {chunkid} to NameNode (attempt {attempt})")
            finally:
                try:
                    if s:
                        s.close()
                except Exception:
                    pass
            time.sleep(min(5, 0.5 * (2 ** (attempt - 1))))

    def handle_retrieve_chunk(self, request, clientsocket):
        chunkid = request['chunk_id']
        meta = self.chunks.get(chunkid)
        if not meta:
            send_json(clientsocket, {'status': 'error', 'message': 'Chunk not found'})
            return

        path = os.path.join(self.storagedir, chunkid)
        if not os.path.exists(path):
            send_json(clientsocket, {'status': 'error', 'message': 'Chunk file missing'})
            return

        try:
            with open(path, 'rb') as f:
                chunkdata = f.read()
        except Exception as e:
            send_json(clientsocket, {'status': 'error', 'message': f'IO error: {e}'})
            return

        checksum = self.calculate_checksum(chunkdata)
        if checksum != meta.get('checksum'):
            send_json(clientsocket, {'status': 'error', 'message': 'Checksum failed'})
            return

        # send header
        logger.info(f"RETRIEVE request for {chunkid}, sending header chunk_size={len(chunkdata)}")
        try:
            send_json(clientsocket, {'status': 'success', 'chunk_size': len(chunkdata), 'checksum': checksum})
            # wait ack
            clientsocket.settimeout(15)
            _ = clientsocket.recv(3)  # 'ACK'
        except Exception as e:
            logger.error(f"retrieve header/ack failed for {chunkid}: {e}")
            return

        # stream bytes
        try:
            clientsocket.sendall(chunkdata)
            logger.info(f"Finished streaming {len(chunkdata)} bytes for {chunkid}")
        except Exception as e:
            logger.error(f"stream failed for {chunkid}: {e}")

    # ---------- server plumbing ----------
    def handle_client(self, clientsocket, _addr):
        try:
            clientsocket.settimeout(30)
            request = recv_json(clientsocket)
            action = request.get('action')

            if action == 'store':
                self.handle_store_chunk(request, clientsocket)
            elif action == 'retrieve':
                self.handle_retrieve_chunk(request, clientsocket)
            else:
                send_json(clientsocket, {'status': 'error', 'message': 'Unknown action'})
        except Exception as e:
            # Clean, no stacktrace dump to stdout
            try:
                send_json(clientsocket, {'status': 'error', 'message': str(e)})
            except Exception:
                pass
            logger.error(f"Client handler error: {e}")
        finally:
            try:
                clientsocket.close()
            except Exception:
                pass

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
        logger.info('Shutting down datanode1')
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
            logger.exception('Exception during datanode1 shutdown')


if __name__ == '__main__':
    dn = Datanode1()

    def _sig(signum, frame):
        dn.shutdown()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    dn.run()
