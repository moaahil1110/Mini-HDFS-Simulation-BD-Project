import socket
import threading
import json
import os
import time
import logging
import queue
import hashlib
from datetime import datetime
import signal

import config
from protocol import send_json, recv_json

# -------------------- Logging --------------------
logger = logging.getLogger('namenode')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('namenode.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())

class Namenode:
    """
    NameNode (robust version)
    - Dynamic datanode registry (persisted)
    - Proactive + periodic re-replication
    - End-to-end integrity: source checksum + target checksum ACK
    - Avoid re-replicating back to corrupted node
    - Thread-safety for in-progress set & candidate set
    - PriorityQueue tie-breaker (seq) to avoid dict-compare TypeError
    - Efficient checker: candidate-first + infrequent full scans
    """

    def __init__(self):
        # -------------------- State --------------------
        self.metadata = {}        # {filename: {file_size, upload_time, replication_factor, chunks:[...]}}
        self.chunk_index = {}     # {chunk_id: (filename, chunk_info)}
        self.lock = threading.RLock()

        # datanode registry (dynamic)
        # {id: {host, port, heartbeat_port, last_heartbeat, status, was_dead, chunks_stored, storage_used}}
        self.datanodes = {}
        self.datanodes_lock = threading.RLock()

        # backwards-compat health view
        self.datanode_health = {}

        # replication queue with tie-breaker
        self.repl_queue = queue.PriorityQueue()
        self.repl_queue_lock = threading.Lock()
        self._repl_inprog_lock = threading.Lock()
        self.repl_tasks_in_progress = set()

        # sequence counter for PriorityQueue tie-breaker
        self._repl_seq = 0
        self._repl_seq_lock = threading.Lock()

        # under-rep candidates (small working set)
        self._ur_lock = threading.Lock()
        self.under_rep_candidates = set()

        # -------------------- Config --------------------
        self.replication_factor = getattr(config, 'REPLICATION_FACTOR', 2)
        self.heartbeat_timeout = getattr(config, 'HEARTBEAT_TIMEOUT', 15)
        self.heartbeat_interval = getattr(config, 'HEARTBEAT_INTERVAL', 5)
        self.repl_check_interval = getattr(config, 'REPL_CHECK_INTERVAL', 30)
        self.repl_max_retries = getattr(config, 'REPL_MAX_RETRIES', 5)
        self.repl_backoff_base = getattr(config, 'REPL_BACKOFF_BASE', 2.0)
        self.rebuild_delay = getattr(config, 'REBUILD_DELAY', 3)
        self.max_alerts = getattr(config, 'MAX_ALERTS', 100)
        self.repl_fullscan_every = getattr(config, 'REPL_FULLSCAN_EVERY', 10)
        self._checker_cycle = 0

        # alerts
        self.alerts = []
        self.alerts_lock = threading.Lock()

        # -------------------- Persistence --------------------
        os.makedirs(config.NAMENODE_METADATA_DIR, exist_ok=True)
        self.metadata_path = os.path.join(config.NAMENODE_METADATA_DIR, 'metadata.json')
        self.repl_queue_path = os.path.join(config.NAMENODE_METADATA_DIR, 'repl_queue.json')
        self.datanodes_path = os.path.join(config.NAMENODE_METADATA_DIR, 'datanodes.json')

        self._load_metadata()
        self._load_repl_queue()
        self._load_datanodes()

        # -------------------- Shutdown --------------------
        self.stop_event = threading.Event()
        self.server = None
        self.heartbeat_server = None

    # ==================== Helpers ====================

    def _next_repl_seq(self):
        with self._repl_seq_lock:
            self._repl_seq += 1
            return self._repl_seq

    # ==================== Persistence ====================

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            try:
                with open(self.metadata_path, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
                self._rebuild_chunk_index()
                logger.info(f"[METADATA] Loaded {len(self.metadata)} files, {len(self.chunk_index)} chunks")
            except Exception as e:
                logger.exception(f"Failed to load metadata, starting fresh: {e}")
                self.metadata = {}
                self.chunk_index = {}
        else:
            logger.info("[METADATA] No existing metadata, starting fresh")

    def _rebuild_chunk_index(self):
        self.chunk_index = {}
        for fname, info in self.metadata.items():
            for ci in info.get('chunks', []):
                self.chunk_index[ci['chunk_id']] = (fname, ci)

    def save_metadata(self):
        tmp = self.metadata_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.metadata_path)

    def _load_repl_queue(self):
        if os.path.exists(self.repl_queue_path):
            try:
                with open(self.repl_queue_path, 'r', encoding='utf-8') as f:
                    items = json.load(f)
                for task in items:
                    pr = task.get('priority', 10)
                    seq = self._next_repl_seq()
                    self.repl_queue.put((pr, seq, task))
                logger.info(f"[REPL_QUEUE] Loaded {len(items)} pending tasks")
            except Exception as e:
                logger.exception(f"Failed to load replication queue: {e}")

    def _save_repl_queue(self):
        with self.repl_queue_lock:
            try:
                items = []
                temp_queue = queue.PriorityQueue()
                # drain queue
                while True:
                    try:
                        priority, seq, task = self.repl_queue.get_nowait()
                        items.append(task)
                        temp_queue.put((priority, seq, task))
                    except queue.Empty:
                        break

                # write tasks (persist only the task dicts)
                tmp = self.repl_queue_path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(items, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.repl_queue_path)

                # restore queue
                while True:
                    try:
                        pr, sq, t = temp_queue.get_nowait()
                        self.repl_queue.put((pr, sq, t))
                    except queue.Empty:
                        break

                logger.debug(f"[REPL_QUEUE] Saved {len(items)} tasks")
            except Exception as e:
                logger.exception(f"Failed to save replication queue: {e}")

    def _load_datanodes(self):
        if os.path.exists(self.datanodes_path):
            try:
                with open(self.datanodes_path, 'r', encoding='utf-8') as f:
                    self.datanodes = json.load(f)
                logger.info(f"[DNREG] Loaded {len(self.datanodes)} datanodes from registry")
            except Exception as e:
                logger.exception(f"Failed to load datanode registry: {e}")
                self.datanodes = {}
        self._refresh_health_view()

    def _save_datanodes(self):
        tmp = self.datanodes_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.datanodes, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.datanodes_path)

    # ==================== Alerts ====================

    def add_alert(self, alert_type, **kwargs):
        with self.alerts_lock:
            alert = {'type': alert_type, 'time': datetime.now().isoformat(), **kwargs}
            self.alerts.append(alert)
            if len(self.alerts) > self.max_alerts:
                self.alerts = self.alerts[-self.max_alerts:]
        logger.warning(f"[ALERT] {alert_type}: {kwargs}")

    # ==================== Startup/Shutdown ====================

    def start(self):
        logger.info("=" * 60)
        logger.info("Starting HDFS NameNode")
        logger.info(f"Replication Factor: {self.replication_factor}")
        logger.info(f"Heartbeat Timeout: {self.heartbeat_timeout}s")
        logger.info(f"Replication Check Interval: {self.repl_check_interval}s")
        logger.info("=" * 60)

        threading.Thread(target=self.heartbeat_listener, daemon=True, name="HeartbeatListener").start()
        threading.Thread(target=self.monitor_health, daemon=True, name="HealthMonitor").start()
        threading.Thread(target=self._replication_worker, daemon=True, name="ReplicationWorker").start()
        threading.Thread(target=self.periodic_replication_check, daemon=True, name="ReplicationChecker").start()
        threading.Thread(target=self._periodic_queue_save, daemon=True, name="QueueSaver").start()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((config.NAMENODE_HOST, config.NAMENODE_PORT))
        self.server.listen(50)
        logger.info(f"[RPC] Listening on {config.NAMENODE_HOST}:{config.NAMENODE_PORT}")

        try:
            while not self.stop_event.is_set():
                try:
                    self.server.settimeout(1.0)
                    client_socket, addr = self.server.accept()
                    threading.Thread(
                        target=self.handle_client,
                        args=(client_socket,),
                        daemon=True,
                        name=f"Client-{addr}"
                    ).start()
                except socket.timeout:
                    continue
                except OSError:
                    break
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("=" * 60)
        logger.info("Shutting down NameNode")
        self.stop_event.set()

        for srv in [self.server, self.heartbeat_server]:
            if srv:
                try:
                    srv.close()
                except Exception:
                    pass

        try:
            with self.lock:
                self.save_metadata()
            logger.info("[SHUTDOWN] Metadata saved")
        except Exception as e:
            logger.exception(f"Error saving metadata: {e}")

        try:
            self._save_repl_queue()
            logger.info("[SHUTDOWN] Replication queue saved")
        except Exception as e:
            logger.exception(f"Error saving replication queue: {e}")

        try:
            with self.datanodes_lock:
                self._save_datanodes()
            logger.info("[SHUTDOWN] Datanode registry saved")
        except Exception as e:
            logger.exception(f"Error saving datanode registry: {e}")

        logger.info("Shutdown complete")
        logger.info("=" * 60)

    def _periodic_queue_save(self):
        while not self.stop_event.is_set():
            time.sleep(60)
            try:
                self._save_repl_queue()
            except Exception as e:
                logger.error(f"Periodic queue save failed: {e}")

    # ==================== Heartbeats & Health ====================

    def heartbeat_listener(self):
        self.heartbeat_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.heartbeat_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.heartbeat_server.bind((config.NAMENODE_HOST, config.NAMENODE_HEARTBEAT_PORT))
        self.heartbeat_server.listen(50)
        self.heartbeat_server.settimeout(1.0)
        logger.info(f"[HEARTBEAT] Listening on {config.NAMENODE_HOST}:{config.NAMENODE_HEARTBEAT_PORT}")

        while not self.stop_event.is_set():
            try:
                conn, _ = self.heartbeat_server.accept()
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"Heartbeat accept error: {e}")
                break

            try:
                conn.settimeout(5.0)
                hb = recv_json(conn)
                self._process_heartbeat(hb)
            except socket.timeout:
                logger.debug("Heartbeat processing: timed out")
            except Exception as e:
                logger.debug(f"Heartbeat processing error: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def _process_heartbeat(self, hb):
        dnid = hb.get('datanode_id')
        if not dnid:
            logger.warning("Heartbeat missing datanode_id")
            return

        host = hb.get('host')
        port = hb.get('port')
        hb_port = hb.get('heartbeat_port', None)
        chunks = hb.get('chunks_stored', 0)
        used = hb.get('storage_used', 0)

        with self.datanodes_lock:
            node = self.datanodes.get(dnid)
            if not node:
                node = {
                    'host': host,
                    'port': port,
                    'heartbeat_port': hb_port,
                    'last_heartbeat': None,
                    'status': 'unknown',
                    'was_dead': False,
                    'chunks_stored': 0,
                    'storage_used': 0
                }
                # Fallback for legacy fixed nodes
                if not host or not port:
                    if dnid == 'datanode0':
                        node['host'] = getattr(config, 'DATANODE0_HOST', None)
                        node['port'] = getattr(config, 'DATANODE0_PORT', None)
                    if dnid == 'datanode1':
                        node['host'] = getattr(config, 'DATANODE1_HOST', None)
                        node['port'] = getattr(config, 'DATANODE1_PORT', None)
                self.datanodes[dnid] = node

            prev_status = node['status']
            node['last_heartbeat'] = time.time()
            node['status'] = 'alive'
            if chunks is not None:
                node['chunks_stored'] = chunks
            if used is not None:
                node['storage_used'] = used

            logger.info(f"[HEARTBEAT] {dnid} alive (chunks={node['chunks_stored']})")
            self._save_datanodes()

        self._refresh_health_view()

        if prev_status == 'dead':
            logger.info(f"[RECOVERY] {dnid} recovered from failure")
            threading.Thread(
                target=self.rebuild_for_node,
                args=(dnid,),
                daemon=True,
                name=f"Rebuild-{dnid}"
            ).start()

    def _refresh_health_view(self):
        with self.datanodes_lock:
            self.datanode_health = {
                dnid: {
                    'last_heartbeat': node.get('last_heartbeat'),
                    'status': node.get('status'),
                    'was_dead': node.get('was_dead'),
                    'chunks_stored': node.get('chunks_stored'),
                    'storage_used': node.get('storage_used')
                }
                for dnid, node in self.datanodes.items()
            }

    def monitor_health(self):
        logger.info("[HEALTH] Monitor started")
        while not self.stop_event.is_set():
            time.sleep(self.heartbeat_interval)
            now = time.time()
            dead_now = []
            with self.datanodes_lock:
                for dnid, node in self.datanodes.items():
                    last = node.get('last_heartbeat')
                    if last is None:
                        continue
                    if now - last > self.heartbeat_timeout and node.get('status') != 'dead':
                        elapsed = now - last
                        node['status'] = 'dead'
                        node['was_dead'] = True
                        dead_now.append(dnid)
                        self.add_alert('datanode_failure', datanode=dnid, timeout=elapsed)
                if dead_now:
                    self._save_datanodes()
            self._refresh_health_view()

            for dnid in dead_now:
                threading.Thread(
                    target=self.handle_node_failure,
                    args=(dnid,),
                    daemon=True,
                    name=f"Failure-{dnid}"
                ).start()

    # ==================== Failure handling ====================

    def handle_node_failure(self, dead_dnid):
        logger.info(f"[FAILURE] Processing failure of {dead_dnid}")

        immediate_tasks = []
        under_count, loss_count = 0, 0

        with self.lock, self.datanodes_lock:
            alive_nodes = [n for n, info in self.datanodes.items() if info.get('status') == 'alive' and n != dead_dnid]
            if not alive_nodes:
                logger.critical("[FAILURE] No alive datanodes available for re-replication!")
            for fname, info in self.metadata.items():
                for ci in info['chunks']:
                    chunk_id = ci['chunk_id']
                    stored = ci.get('stored_on', [])
                    if dead_dnid not in stored:
                        continue
                    if dead_dnid in stored:
                        stored.remove(dead_dnid)
                        ci['stored_on'] = stored
                    alive_replicas = [n for n in stored if n in alive_nodes]

                    if len(alive_replicas) < self.replication_factor:
                        if len(alive_replicas) == 0:
                            self.add_alert('data_loss', chunk_id=chunk_id, file=fname)
                            loss_count += 1
                        else:
                            under_count += 1
                            source = alive_replicas[0]
                            targets = self._choose_targets(ci, alive_nodes, exclude=set(stored))
                            for target in targets:
                                immediate_tasks.append((chunk_id, source, target))
                                with self._ur_lock:
                                    self.under_rep_candidates.add(chunk_id)

            try:
                self.save_metadata()
            except Exception as e:
                logger.exception(f"Failed to save metadata after failure: {e}")

        for chunk_id, source, target in immediate_tasks:
            self.enqueue_replication(chunk_id, source, target, priority=1)

        logger.info(f"[FAILURE] {dead_dnid} handled: {under_count} under-replicated, {loss_count} data loss")
        logger.info(f"[PROACTIVE] Enqueued {len(immediate_tasks)} immediate re-replication tasks")

    # ==================== RPC ====================

    def handle_client(self, client_socket):
        peer = None
        try:
            try:
                peer = client_socket.getpeername()
            except Exception:
                peer = "unknown"
            client_socket.settimeout(30.0)
            req = recv_json(client_socket)
            cmd = req.get('command')
            logger.info(f"[RPC] {cmd} from {peer}")

            handlers = {
                'upload': self.handle_upload,
                'report_chunk': self.handle_report_chunk,
                'download': self.handle_download,
                'list': self.handle_list,
                'status': self.handle_status,
            }
            handler = handlers.get(cmd)
            if handler:
                response = handler(req)
            else:
                response = {'status': 'error', 'message': f'Unknown command: {cmd}'}
            send_json(client_socket, response)
        except Exception as e:
            logger.exception(f"Client handler error ({peer}): {e}")
            try:
                send_json(client_socket, {'status': 'error', 'message': str(e)})
            except Exception:
                pass
        finally:
            try:
                client_socket.close()
            except Exception:
                pass

    # ==================== Upload / Report / Download ====================

    def handle_upload(self, req):
        filename = req.get('filename')
        file_size = req.get('file_size')
        if not filename or file_size is None:
            return {'status': 'error', 'message': 'Missing filename or file_size'}

        file_size = int(file_size)
        logger.info(f"[UPLOAD] {filename} ({file_size} bytes)")

        num_chunks = max(1, (file_size + config.CHUNK_SIZE - 1) // config.CHUNK_SIZE)
        logger.info(f"[CHUNKING] {num_chunks} chunks needed")

        with self.datanodes_lock:
            alive_nodes = [n for n, info in self.datanodes.items() if info.get('status') == 'alive']
        if len(alive_nodes) < self.replication_factor:
            return {'status': 'error', 'message': f'Insufficient datanodes ({len(alive_nodes)}/{self.replication_factor})'}

        chunks_info = []
        for i in range(num_chunks):
            chunk_id = f"{filename}_chunk{i}"
            replicas = []
            for j in range(self.replication_factor):
                node_idx = (i + j) % len(alive_nodes)
                replicas.append(alive_nodes[node_idx])

            ci = {
                'chunk_id': chunk_id,
                'chunk_index': i,
                'primary_datanode': replicas[0],
                'replica_datanode': replicas[1] if len(replicas) > 1 else None,
                'all_replicas': replicas,
                'stored_on': [],
            }
            chunks_info.append(ci)

        with self.lock:
            self.metadata[filename] = {
                'chunks': chunks_info,
                'file_size': file_size,
                'upload_time': datetime.now().isoformat(),
                'replication_factor': self.replication_factor
            }
            for ci in chunks_info:
                self.chunk_index[ci['chunk_id']] = (filename, ci)
            try:
                self.save_metadata()
            except Exception:
                logger.exception("Failed to save metadata")
                return {'status': 'error', 'message': 'Metadata save failed'}

        return {
            'status': 'success',
            'chunks': chunks_info,
            'datanode_info': self._alive_datanode_connection_info()
        }

    def handle_report_chunk(self, req):
        filename = req.get('filename')
        chunk_id = req.get('chunk_id')
        checksum = req.get('checksum')
        datanode_id = req.get('datanode_id')

        if not all([filename, chunk_id, datanode_id]):
            return {'status': 'error', 'message': 'Missing required fields'}

        logger.info(f"[REPORT] {chunk_id} on {datanode_id}")

        with self.lock:
            if chunk_id in self.chunk_index:
                fname, ci = self.chunk_index[chunk_id]

                # Soft-check: warn but don't fail if filename text differs from the owner in metadata
                if fname != filename:
                    logger.warning(f"[REPORT] Filename mismatch for {chunk_id}: reported={filename} actual={fname}")

                if checksum:
                    if 'checksum' in ci and ci['checksum'] != checksum:
                        logger.error(f"[CORRUPTION] Checksum mismatch for {chunk_id}")
                        self.add_alert('checksum_mismatch', chunk_id=chunk_id, datanode=datanode_id,
                                       expected=ci['checksum'], got=checksum)
                        self._handle_corrupted_chunk(fname, ci, datanode_id)
                        with self._ur_lock:
                            self.under_rep_candidates.add(chunk_id)
                        return {'status': 'error', 'message': 'Checksum mismatch - corruption detected'}
                    else:
                        ci['checksum'] = checksum

                stored = ci.get('stored_on', [])
                if datanode_id not in stored:
                    stored.append(datanode_id)
                    ci['stored_on'] = stored
            else:
                # fallback scan if index miss
                if filename not in self.metadata:
                    return {'status': 'error', 'message': 'File not found'}
                found = False
                for ci in self.metadata[filename]['chunks']:
                    if ci['chunk_id'] == chunk_id:
                        found = True
                        if checksum:
                            if 'checksum' in ci and ci['checksum'] != checksum:
                                logger.error(f"[CORRUPTION] Checksum mismatch for {chunk_id}")
                                self.add_alert('checksum_mismatch', chunk_id=chunk_id, datanode=datanode_id)
                                self._handle_corrupted_chunk(filename, ci, datanode_id)
                                with self._ur_lock:
                                    self.under_rep_candidates.add(chunk_id)
                                return {'status': 'error', 'message': 'Checksum mismatch'}
                            ci['checksum'] = checksum
                        stored = ci.get('stored_on', [])
                        if datanode_id not in stored:
                            stored.append(datanode_id)
                            ci['stored_on'] = stored
                        break
                if not found:
                    return {'status': 'error', 'message': 'Chunk not found in metadata'}

            try:
                self.save_metadata()
                return {'status': 'success'}
            except Exception:
                logger.exception("Failed to save metadata")
                return {'status': 'error', 'message': 'Metadata save failed'}

    def _handle_corrupted_chunk(self, filename, chunk_info, corrupted_node):
        """On corruption, replicate from a good replica to a NEW healthy target (avoid the bad node)."""
        chunk_id = chunk_info['chunk_id']
        with self.datanodes_lock:
            alive_nodes = [n for n, info in self.datanodes.items() if info.get('status') == 'alive']

        stored = set(chunk_info.get('stored_on', []))
        good_sources = [n for n in stored if n in alive_nodes and n != corrupted_node]
        if not good_sources:
            self.add_alert('no_good_replicas', chunk_id=chunk_id, file=filename)
            logger.critical(f"[CORRUPTION] No good replicas available for {chunk_id}")
            return
        source = good_sources[0]

        exclude = set(stored)
        exclude.add(corrupted_node)
        targets = self._choose_targets(chunk_info, alive_nodes, exclude=exclude)
        if not targets:
            self.add_alert('corruption_no_target', chunk_id=chunk_id, source=source)
            logger.warning(f"[CORRUPTION] No alternate target available for {chunk_id}")
            return

        target = targets[0]
        logger.info(f"[CORRUPTION] Re-replicating {chunk_id} {source}->{target} (avoiding {corrupted_node})")
        self.enqueue_replication(chunk_id, source, target, priority=1)

        with self._ur_lock:
            self.under_rep_candidates.add(chunk_id)

    def handle_download(self, req):
        filename = req.get('filename')
        if not filename:
            return {'status': 'error', 'message': 'Missing filename'}
        with self.lock:
            info = self.metadata.get(filename)
            if not info:
                return {'status': 'error', 'message': 'File not found'}
        return {
            'status': 'success',
            'file_info': info,
            'datanode_info': self._alive_datanode_connection_info()
        }

    def handle_list(self, req):
        with self.lock:
            return {'status': 'success', 'files': list(self.metadata.keys())}

    def handle_status(self, req):
        with self.lock:
        # lightweight, UI-friendly view (now also exposes full metadata for the UI map)
            status = {
            'status': 'success',
            'datanode_health': self.datanode_health,
            'total_files': len(self.metadata),
            'total_chunks': len(self.chunk_index),
            'replication_factor': self.replication_factor,
            'config': {
                'heartbeat_timeout': self.heartbeat_timeout,
                'heartbeat_interval': self.heartbeat_interval,
                'repl_check_interval': self.repl_check_interval
            },
            # 👇 add full metadata so the UI can draw the chunk map
            'metadata': self.metadata
            }
        with self.alerts_lock:
            status['alerts'] = self.alerts[-50:]
        status['replication_queue_size'] = self.repl_queue.qsize()
        return status


    # ==================== Datanode info helpers ====================

    def _alive_datanode_connection_info(self):
        with self.datanodes_lock:
            return {
                dnid: {'host': node.get('host'), 'port': node.get('port')}
                for dnid, node in self.datanodes.items()
                if node.get('status') == 'alive' and node.get('host') and node.get('port')
            }

    def _resolve_node_endpoint(self, dnid):
        with self.datanodes_lock:
            node = self.datanodes.get(dnid)
            if node and node.get('host') and node.get('port'):
                return node['host'], int(node['port'])
        return None, None

    # ==================== Replication ====================

    def enqueue_replication(self, chunk_id, source, target, retries=0, priority=10):
        """
        Add replication task with a tie-breaking sequence number so heap comparisons never hit dicts.
        """
        task_key = f"{chunk_id}:{source}:{target}"
        with self._repl_inprog_lock:
            if task_key in self.repl_tasks_in_progress:
                logger.debug(f"[REPL_QUEUE] Task already in progress: {task_key}")
                return

        # if target already has it, skip
        with self.lock:
            ci = self.chunk_index.get(chunk_id, (None, None))[1]
            if ci and target in ci.get('stored_on', []):
                logger.debug(f"[REPL_QUEUE] Target already has chunk: {chunk_id}")
                return

        task = {
            'chunk_id': chunk_id,
            'source': source,
            'target': target,
            'retries': retries,
            'priority': priority,
            'enqueued_at': time.time()
        }

        seq = self._next_repl_seq()
        self.repl_queue.put((priority, seq, task))
        logger.info(f"[REPL_QUEUE] Enqueued (pri={priority},seq={seq}): {chunk_id} {source}->{target}")

    def _replication_worker(self):
        logger.info("[REPL_WORKER] Started")
        while not self.stop_event.is_set():
            try:
                priority, seq, task = self.repl_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            chunk_id = task['chunk_id']
            source = task['source']
            target = task['target']
            retries = task.get('retries', 0)
            task_key = f"{chunk_id}:{source}:{target}"

            with self._repl_inprog_lock:
                self.repl_tasks_in_progress.add(task_key)

            try:
                success = self._execute_replication(chunk_id, source, target)
                if success:
                    logger.info(f"[REPL_SUCCESS] {chunk_id} {source}->{target}")
                    self._update_metadata_after_replication(chunk_id, target)
                    with self._ur_lock:
                        self.under_rep_candidates.discard(chunk_id)
                else:
                    retries += 1
                    if retries <= self.repl_max_retries:
                        backoff = self.repl_backoff_base ** retries
                        logger.warning(f"[REPL_RETRY] {chunk_id} {source}->{target} "
                                       f"(attempt {retries}/{self.repl_max_retries}) after {backoff}s")
                        time.sleep(backoff)
                        task['retries'] = retries
                        new_seq = self._next_repl_seq()
                        self.repl_queue.put((priority, new_seq, task))
                    else:
                        logger.error(f"[REPL_FAILED] {chunk_id} {source}->{target} after {retries} retries")
                        self.add_alert('replication_failed', chunk_id=chunk_id, source=source, target=target, retries=retries)
            finally:
                with self._repl_inprog_lock:
                    self.repl_tasks_in_progress.discard(task_key)
            time.sleep(0.05)

    def _execute_replication(self, chunk_id, source, target):
        shost, sport = self._resolve_node_endpoint(source)
        thost, tport = self._resolve_node_endpoint(target)
        if not (shost and sport and thost and tport):
            logger.error(f"[REPL] Invalid endpoints; source={source} {shost}:{sport}, target={target} {thost}:{tport}")
            return False

        src_sock = None
        tgt_sock = None

        try:
            expected_checksum = None
            with self.lock:
                ci = self.chunk_index.get(chunk_id, (None, None))[1]
                if ci:
                    expected_checksum = ci.get('checksum')

            # --- Retrieve from source ---
            src_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            src_sock.settimeout(30)
            src_sock.connect((shost, sport))
            send_json(src_sock, {'action': 'retrieve', 'chunk_id': chunk_id})
            meta = recv_json(src_sock)
            if meta.get('status') != 'success':
                logger.error(f"[REPL] Retrieve failed: {meta}")
                return False
            src_sock.sendall(b'ACK')
            chunk_size = int(meta['chunk_size'])
            data = b''
            while len(data) < chunk_size:
                buf = src_sock.recv(min(8192, chunk_size - len(data)))
                if not buf:
                    raise RuntimeError("Connection closed during read")
                data += buf
            if len(data) != chunk_size:
                raise RuntimeError(f"Size mismatch: got {len(data)}, expected {chunk_size}")

            # --- Checksum at source ---
            computed_checksum = hashlib.md5(data).hexdigest()
            if expected_checksum and computed_checksum != expected_checksum:
                logger.error(f"[REPL_CORRUPTION] Source checksum mismatch for {chunk_id}: "
                             f"expected={expected_checksum}, got={computed_checksum}")
                self.add_alert('replication_corruption', chunk_id=chunk_id, source=source,
                               expected=expected_checksum, computed=computed_checksum)
                return False

            # --- Store to target (send expected/known checksum) ---
            tgt_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tgt_sock.settimeout(30)
            tgt_sock.connect((thost, tport))
            send_json(tgt_sock, {
                'action': 'store',
                'chunk_id': chunk_id,
                'chunk_size': len(data),
                'checksum': expected_checksum or computed_checksum
            })
            ready = tgt_sock.recv(5)
            if not ready or ready != b'READY':
                raise RuntimeError(f"Target not ready: {ready}")
            tgt_sock.sendall(data)
            ack = recv_json(tgt_sock)

            if ack.get('status') != 'success':
                raise RuntimeError(f"Target store failed: {ack}")

            target_checksum = ack.get('checksum')  # REQUIRED
            if not target_checksum:
                logger.warning("[REPL] Target did not return checksum; not updating metadata (retry later)")
                return False

            if expected_checksum and target_checksum != expected_checksum:
                logger.error(f"[REPL_CORRUPTION] Target checksum mismatch for {chunk_id}: "
                             f"expected={expected_checksum}, target={target_checksum}")
                self.add_alert('replication_corruption_target', chunk_id=chunk_id,
                               target=target, expected=expected_checksum, got=target_checksum)
                return False

            # learn checksum if we had none
            if not expected_checksum:
                with self.lock:
                    ci = self.chunk_index.get(chunk_id, (None, None))[1]
                    if ci:
                        ci['checksum'] = target_checksum
                        try:
                            self.save_metadata()
                        except Exception:
                            logger.exception("Failed to save metadata after checksum learn")

            return True

        except Exception as e:
            logger.error(f"[REPL] Exception: {chunk_id} {source}->{target}: {e}")
            return False
        finally:
            for s in (src_sock, tgt_sock):
                if s:
                    try:
                        s.close()
                    except Exception:
                        pass

    def _update_metadata_after_replication(self, chunk_id, target):
        with self.lock:
            ci = self.chunk_index.get(chunk_id, (None, None))[1]
            if not ci:
                return
            stored = ci.get('stored_on', [])
            if target not in stored:
                stored.append(target)
                ci['stored_on'] = stored
            try:
                self.save_metadata()
            except Exception as e:
                logger.error(f"Failed to save metadata: {e}")

    def _choose_targets(self, ci, alive_nodes, exclude):
        """
        Return up to 'needed' targets: prefer expected replicas not already storing; then any alive not storing.
        """
        rf = self.replication_factor
        stored = set(ci.get('stored_on', []))
        expected = ci.get('all_replicas', [ci.get('primary_datanode'), ci.get('replica_datanode')])
        needed = rf - len([n for n in stored if n in alive_nodes])
        if needed <= 0:
            return []
        targets = []
        for n in expected:
            if n and n in alive_nodes and n not in stored and n not in exclude and n not in targets:
                targets.append(n)
                if len(targets) >= needed:
                    return targets
        for n in alive_nodes:
            if n not in stored and n not in exclude and n not in targets:
                targets.append(n)
                if len(targets) >= needed:
                    break
        return targets

    def rebuild_for_node(self, target_dnid):
        logger.info(f"[REBUILD] Starting for {target_dnid}")
        if self.rebuild_delay > 0:
            logger.info(f"[REBUILD] Waiting {self.rebuild_delay}s for late reports")
            time.sleep(self.rebuild_delay)

        tasks = []
        with self.lock, self.datanodes_lock:
            if self.datanodes.get(target_dnid, {}).get('status') != 'alive':
                logger.warning(f"[REBUILD] {target_dnid} not alive; skipping")
                return
            for fname, info in self.metadata.items():
                for ci in info['chunks']:
                    chunk_id = ci['chunk_id']
                    stored = ci.get('stored_on', [])
                    expected = ci.get('all_replicas', [ci.get('primary_datanode'), ci.get('replica_datanode')])
                    if target_dnid in expected and target_dnid not in stored:
                        source = None
                        for s in stored:
                            if self.datanodes.get(s, {}).get('status') == 'alive':
                                source = s
                                break
                        if source:
                            tasks.append((chunk_id, source, target_dnid))
                            with self._ur_lock:
                                self.under_rep_candidates.add(chunk_id)

        logger.info(f"[REBUILD] Found {len(tasks)} chunks to rebuild")
        for chunk_id, source, target in tasks:
            self.enqueue_replication(chunk_id, source, target, priority=5)
        logger.info(f"[REBUILD] Complete: {len(tasks)} tasks enqueued")

    # ==================== Replication Checker ====================

    def periodic_replication_check(self):
        logger.info(f"[REPL_CHECKER] Started (interval={self.repl_check_interval}s)")
        while not self.stop_event.is_set():
            time.sleep(self.repl_check_interval)
            try:
                self._checker_cycle += 1
                logger.info("[REPL_CHECKER] Running scan...")

                with self.datanodes_lock:
                    alive_nodes = [n for n, info in self.datanodes.items() if info.get('status') == 'alive']
                if not alive_nodes:
                    logger.warning("[REPL_CHECKER] No alive datanodes")
                    continue

                under = []

                # Drain candidate set first
                candidates = []
                with self._ur_lock:
                    while self.under_rep_candidates:
                        candidates.append(self.under_rep_candidates.pop())

                def eval_chunk(chunk_id, ci):
                    stored = ci.get('stored_on', [])
                    alive_replicas = [n for n in stored if n in alive_nodes]
                    if len(alive_replicas) < self.replication_factor and alive_replicas:
                        targets = self._choose_targets(ci, alive_nodes, exclude=set(stored))
                        if targets:
                            return alive_replicas[0], targets
                    return None, None

                with self.lock:
                    # Evaluate candidates
                    for chunk_id in candidates:
                        ci = self.chunk_index.get(chunk_id, (None, None))[1]
                        if not ci:
                            continue
                        src, targets = eval_chunk(chunk_id, ci)
                        if src and targets:
                            for t in targets:
                                under.append((chunk_id, src, t, len(ci.get('stored_on', []))))

                    # Infrequent full scan
                    if self._checker_cycle % self.repl_fullscan_every == 0:
                        for fname, info in self.metadata.items():
                            for ci in info['chunks']:
                                cid = ci['chunk_id']
                                src, targets = eval_chunk(cid, ci)
                                if src and targets:
                                    for t in targets:
                                        under.append((cid, src, t, len(ci.get('stored_on', []))))

                for cid, src, tgt, cur in under:
                    logger.warning(f"[REPL_CHECKER] Under-replicated: {cid} (current={cur}, target={self.replication_factor})")
                    self.enqueue_replication(cid, src, tgt, priority=10)

                if under:
                    logger.info(f"[REPL_CHECKER] Enqueued {len(under)} repair tasks")
                else:
                    logger.info("[REPL_CHECKER] All chunks properly replicated")

            except Exception as e:
                logger.exception(f"[REPL_CHECKER] Error: {e}")

# ==================== MAIN ====================

if __name__ == '__main__':
    nn = Namenode()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown")
        nn.shutdown()
        os._exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        nn.start()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        nn.shutdown()
        os._exit(1)
