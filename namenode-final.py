import socket
import threading
import json
import os
import time
import logging
import queue
import math
from datetime import datetime
import signal

import config
from protocol import send_json, recv_json

# logging
logger = logging.getLogger('namenode')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('namenode.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())

class Namenode:
    """
    Responsibilities:
      - chunk planning & replication targets
      - metadata persistence
      - heartbeats & DN liveness
      - basic status APIs used by the client dashboard
    """
    def __init__(self):
        self.metadata = {}                 # { filename: {file_size, upload_time, chunks:[{chunk_id, chunk_index, primary_datanode, replica_datanode}] } }
        self.lock = threading.Lock()
        self.datanode_health = {
            'datanode0': {'last_heartbeat': None, 'status': 'unknown', 'was_dead': False},
            'datanode1': {'last_heartbeat': None, 'status': 'unknown', 'was_dead': False},
        }
        os.makedirs(config.NAMENODE_METADATA_DIR, exist_ok=True)
        self._load_metadata()

        # stop event for graceful shutdown
        self.stop_event = threading.Event()
        self.server = None

        # replication queue stored on disk (simple persistence)
        self.repl_queue = queue.Queue()
        self.repl_queue_path = os.path.join(config.NAMENODE_METADATA_DIR, 'repl_queue.json')
        self._load_repl_queue()

        # alerts (checksum mismatches, replication failures)
        self.alerts = []

    # ---------- persistence ----------
    def _load_metadata(self):
        path = os.path.join(config.NAMENODE_METADATA_DIR, 'metadata.json')
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
            except Exception:
                self.metadata = {}

    def save_metadata(self):
        path = os.path.join(config.NAMENODE_METADATA_DIR, 'metadata.json')
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    # ---------- servers ----------
    def start(self):
        # Heartbeats listener
        threading.Thread(target=self.heartbeat_listener, daemon=True).start()
        # Liveness monitor
        threading.Thread(target=self.monitor_health, daemon=True).start()
        # Replication worker
        threading.Thread(target=self._replication_worker, daemon=True).start()
        # Periodic replication health check
        threading.Thread(target=self.periodic_replication_check, daemon=True).start()

        # RPC server for client requests
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((config.NAMENODE_HOST, config.NAMENODE_PORT))
        self.server.listen(50)
        print(f"[Namenode] Listening on {config.NAMENODE_HOST}:{config.NAMENODE_PORT}")
        try:
            while not self.stop_event.is_set():
                try:
                    client_socket, _ = self.server.accept()
                except OSError:
                    break
                threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info('Shutting down Namenode')
        try:
            self.stop_event.set()
            if self.server:
                try:
                    self.server.close()
                except Exception:
                    pass
            # persist metadata and queue
            try:
                self.save_metadata()
            except Exception:
                logger.exception('Error saving metadata during shutdown')
            try:
                self._save_repl_queue()
            except Exception:
                logger.exception('Error saving replication queue during shutdown')
        except Exception:
            logger.exception('Exception during Namenode shutdown')

    def heartbeat_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((config.NAMENODE_HOST, config.NAMENODE_HEARTBEAT_PORT))
        sock.listen(50)
        print(f"[Namenode] Heartbeats on {config.NAMENODE_HOST}:{config.NAMENODE_HEARTBEAT_PORT}")
        while True:
            conn, _ = sock.accept()
            try:
                hb = recv_json(conn)
                dnid = hb.get('datanode_id')
                with self.lock:
                    self.datanode_health.setdefault(dnid, {'last_heartbeat': None, 'status': 'unknown', 'was_dead': False})
                    prev_status = self.datanode_health[dnid]['status']
                    self.datanode_health[dnid]['last_heartbeat'] = time.time()
                    self.datanode_health[dnid]['status'] = 'alive'
                    # Log receipt of heartbeat
                    try:
                        logger.info(f"Received heartbeat from {dnid} (chunks={hb.get('chunks_stored')}) at {time.ctime(self.datanode_health[dnid]['last_heartbeat'])}")
                    except Exception:
                        logger.info(f"Received heartbeat from {dnid}")

                    # If node recovered from dead, trigger rebuild for that node
                    if prev_status != 'alive' and prev_status == 'dead':
                        logger.info(f"{dnid} appears to have recovered; scheduling rebuild of missing chunks")
                        self.datanode_health[dnid]['was_dead'] = True
                        threading.Thread(target=self.rebuild_for_node, args=(dnid,), daemon=True).start()

                    # If all known datanodes are alive, log it
                    all_alive = all(h.get('status') == 'alive' for h in self.datanode_health.values())
                    if all_alive:
                        logger.info("All datanodes reporting as alive")
            except Exception:
                pass
            finally:
                conn.close()

    def monitor_health(self):
        while True:
            time.sleep(config.HEARTBEAT_INTERVAL)
            now = time.time()
            with self.lock:
                for dnid, health in self.datanode_health.items():
                    last = health['last_heartbeat']
                    if last and now - last > config.HEARTBEAT_TIMEOUT:
                        if health['status'] != 'dead':
                            logger.warning(f"Detected datanode {dnid} as dead (last heartbeat {now-last:.1f}s ago)")
                            health['status'] = 'dead'
                            health['was_dead'] = True
                            # Trigger immediate re-replication for chunks that lost their replica
                            threading.Thread(target=self.handle_node_failure, args=(dnid,), daemon=True).start()
                        else:
                            health['status'] = 'dead'
                            health['was_dead'] = True

    def handle_node_failure(self, dead_dnid):
        """Handle immediate re-replication when a datanode fails."""
        logger.info(f"Handling failure of {dead_dnid} - checking for chunks that need re-replication")
        
        with self.lock:
            failed_chunks = []
            for fname, info in self.metadata.items():
                for ci in info['chunks']:
                    chunk_id = ci['chunk_id']
                    stored = ci.get('stored_on', [])
                    
                    # Check if this chunk is stored on the dead node
                    if dead_dnid in stored:
                        # Find alive nodes that should have this chunk
                        expected_replicas = [ci['primary_datanode'], ci['replica_datanode']]
                        
                        # Find which replicas are still alive
                        alive_replicas = []
                        for replica in expected_replicas:
                            if replica != dead_dnid and replica in self.datanode_health:
                                if self.datanode_health[replica]['status'] == 'alive':
                                    alive_replicas.append(replica)
                        
                        # If we have at least one alive replica, find a target for re-replication
                        if len(alive_replicas) >= 1:
                            # Use the other expected replica as target if it's alive, otherwise use the alive replica
                            target = None
                            for expected in expected_replicas:
                                if expected != dead_dnid and expected not in stored and expected in self.datanode_health:
                                    if self.datanode_health[expected]['status'] == 'alive':
                                        target = expected
                                        break
                            
                            # If target is not the expected replica, pick any alive node that's not the source
                            if not target:
                                for replica in alive_replicas:
                                    if replica not in stored:
                                        target = replica
                                        break
                            
                            if target:
                                # Use the first alive replica as source
                                source = alive_replicas[0]
                                failed_chunks.append({
                                    'chunk_id': chunk_id,
                                    'source': source,
                                    'target': target,
                                    'reason': f'primary_replica_{dead_dnid}_failed'
                                })
                                logger.info(f"Chunk {chunk_id} lost replica on {dead_dnid}, will re-replicate from {source} to {target}")
        
        # Enqueue replication tasks for all failed chunks
        for task in failed_chunks:
            self.enqueue_replication(task['chunk_id'], task['source'], task['target'])
        
        logger.info(f"Enqueued {len(failed_chunks)} chunks for re-replication after {dead_dnid} failure")

    # ---------- client RPC handlers ----------
    def handle_client(self, client_socket):
        try:
            try:
                peer = None
                try:
                    peer = client_socket.getpeername()
                except Exception:
                    peer = None
                logger.info(f"Accepted connection from {peer}")
                req = recv_json(client_socket)
                logger.info(f"Received request: {req} from {peer}")
            except Exception as e:
                logger.exception(f"Failed to receive JSON from client {peer}: {e}")
                raise
            cmd = req.get('command')
            if cmd == 'upload':
                response = self.handle_upload(req)
            elif cmd == 'report_chunk':
                # client/datanode reports that a chunk was stored and returns checksum
                filename = req.get('filename')
                chunk_id = req.get('chunk_id')
                checksum = req.get('checksum')
                datanode_id = req.get('datanode_id')
                logger.info(f"report_chunk received: file={filename} chunk={chunk_id} dn={datanode_id} checksum={checksum}")
                with self.lock:
                    if filename in self.metadata:
                        updated = False
                        for ci in self.metadata[filename]['chunks']:
                            if ci['chunk_id'] == chunk_id:
                                # store checksum (once) and record that this datanode has the chunk
                                if 'checksum' not in ci:
                                    ci['checksum'] = checksum
                                else:
                                    if ci['checksum'] != checksum:
                                        logger.error(f"Checksum mismatch for {chunk_id}: expected={ci['checksum']} reported={checksum}")
                                        self.alerts.append({'chunk_id': chunk_id, 'issue': 'checksum_mismatch', 'reported': checksum, 'expected': ci.get('checksum'), 'time': datetime.now().isoformat()})
                                # track 'stored_on' set
                                stored = ci.get('stored_on', [])
                                if datanode_id not in stored:
                                    stored.append(datanode_id)
                                    ci['stored_on'] = stored
                                updated = True
                                break
                        if updated:
                            logger.info(f"Saving metadata after report_chunk for {filename} {chunk_id}")
                            try:
                                self.save_metadata()
                            except Exception:
                                logger.exception('Failed while saving metadata')
                            response = {'status': 'success'}
                        else:
                            response = {'status': 'error', 'message': 'chunk not found in metadata'}
                    else:
                        response = {'status': 'error', 'message': 'file not known'}
            elif cmd == 'download':
                response = self.handle_download(req)
            elif cmd == 'list':
                with self.lock:
                    response = {'files': list(self.metadata.keys())}
            elif cmd == 'status':
                with self.lock:
                    response = {
                        'datanode_health': self.datanode_health,
                        'total_files': len(self.metadata),
                        'metadata': self.metadata,
                        'alerts': self.alerts,
                    }
            else:
                response = {'status': 'error', 'message': 'Unknown command'}
            send_json(client_socket, response)
        except Exception as e:
            try:
                send_json(client_socket, {'status': 'error', 'message': str(e)})
            except Exception:
                pass
        finally:
            client_socket.close()

    def handle_upload(self, req):
        filename = req['filename']
        file_size = int(req['file_size'])
        logger.info(f"Upload request for '{filename}' size={file_size}")
        num_chunks = (file_size + config.CHUNK_SIZE - 1) // config.CHUNK_SIZE

        # Alternate primary, but always replicate to both DNs (RF=2)
        chunks_info = []
        for i in range(num_chunks):
            chunk_id = f"{filename}chunk{i}"
            primary = 'datanode0' if i % 2 == 0 else 'datanode1'
            replica = 'datanode1' if primary == 'datanode0' else 'datanode0'
            chunks_info.append({
                'chunk_id': chunk_id,
                'chunk_index': i,
                'primary_datanode': primary,
                'replica_datanode': replica,
            })

        with self.lock:
            self.metadata[filename] = {
                'chunks': chunks_info,
                'file_size': file_size,
                'upload_time': datetime.now().isoformat(),
            }
            self.save_metadata()
        logger.info(f"Stored metadata for '{filename}' with {len(chunks_info)} chunks")

        return {
            'status': 'success',
            'chunks': chunks_info,
            'datanode_info': {
                'datanode0': {'host': config.DATANODE0_HOST, 'port': config.DATANODE0_PORT},
                'datanode1': {'host': config.DATANODE1_HOST, 'port': config.DATANODE1_PORT},
            },
        }

    def handle_download(self, req):
        filename = req['filename']
        logger.info(f"Download request for '{filename}'")
        with self.lock:
            if filename not in self.metadata:
                return {'status': 'error', 'message': 'File not found'}
            info = self.metadata[filename]
        logger.info(f"Returning file plan for '{filename}' -> {len(info['chunks'])} chunks")
        return {
            'status': 'success',
            'file_info': info,
            'datanode_info': {
                'datanode0': {'host': config.DATANODE0_HOST, 'port': config.DATANODE0_PORT},
                'datanode1': {'host': config.DATANODE1_HOST, 'port': config.DATANODE1_PORT},
            },
        }

    # ---------- replication queue persistence & worker ----------
    def _load_repl_queue(self):
        try:
            if os.path.exists(self.repl_queue_path):
                with open(self.repl_queue_path, 'r', encoding='utf-8') as f:
                    items = json.load(f)
                for it in items:
                    self.repl_queue.put(it)
                logger.info(f"Loaded replication queue with {len(items)} items from {self.repl_queue_path}")
        except Exception:
            logger.exception('Failed to load replication queue')

    def _save_repl_queue(self):
        try:
            # drain queue temporarily
            items = []
            while not self.repl_queue.empty():
                items.append(self.repl_queue.get_nowait())
            tmp = self.repl_queue_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2)
                f.flush(); os.fsync(f.fileno())
            os.replace(tmp, self.repl_queue_path)
            logger.info(f"Saved replication queue with {len(items)} items to {self.repl_queue_path}")
            # put back
            for it in items:
                self.repl_queue.put(it)
        except Exception:
            logger.exception('Failed to save replication queue')

    def enqueue_replication(self, chunk_id, source_dn, target_dn, retries=0):
        task = {'chunk_id': chunk_id, 'source': source_dn, 'target': target_dn, 'retries': retries}
        self.repl_queue.put(task)
        self._save_repl_queue()

    def _replication_worker(self):
        logger.info('Replication worker started')
        while True:
            try:
                task = self.repl_queue.get()
            except Exception:
                time.sleep(1)
                continue
            chunk_id = task.get('chunk_id')
            source = task.get('source')
            target = task.get('target')
            retries = task.get('retries', 0)

            dn_info = {
                'datanode0': {'host': config.DATANODE0_HOST, 'port': config.DATANODE0_PORT},
                'datanode1': {'host': config.DATANODE1_HOST, 'port': config.DATANODE1_PORT},
            }

            src = dn_info.get(source)
            tgt = dn_info.get(target)
            if not src or not tgt:
                logger.error(f'Invalid replication task {task}, skipping')
                self._save_repl_queue()
                continue

            success = False
            try:
                # retrieve from source
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)
                s.connect((src['host'], src['port']))
                send_json(s, {'action': 'retrieve', 'chunk_id': chunk_id})
                meta = recv_json(s)
                if meta.get('status') != 'success':
                    s.close()
                    raise RuntimeError(f"Failed to fetch {chunk_id} from {source}: {meta}")
                s.sendall(b'ACK')
                remaining = int(meta['chunk_size'])
                data = b''
                while len(data) < remaining:
                    part = s.recv(min(8192, remaining - len(data)))
                    if not part:
                        break
                    data += part
                s.close()

                if len(data) != remaining:
                    raise RuntimeError(f"Short read from {source} for {chunk_id}")

                # send to target
                t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                t.settimeout(10)
                t.connect((tgt['host'], tgt['port']))
                send_json(t, {'action': 'store', 'chunk_id': chunk_id, 'chunk_size': len(data)})
                ready = t.recv(5)
                if not ready:
                    t.close()
                    raise RuntimeError(f"No READY from target {target}")
                t.sendall(data)
                ack = recv_json(t)
                t.close()
                if ack.get('status') != 'success':
                    raise RuntimeError(f"Target {target} failed to store {chunk_id}: {ack}")

                logger.info(f"RE-REPLICATION SUCCESS: Chunk {chunk_id} replicated from {source} to {target}")
                success = True
                
                # Update metadata to reflect that target now has the chunk
                with self.lock:
                    for fname, info in self.metadata.items():
                        for ci in info['chunks']:
                            if ci['chunk_id'] == chunk_id:
                                stored = ci.get('stored_on', [])
                                if target not in stored:
                                    stored.append(target)
                                    ci['stored_on'] = stored
                                    logger.info(f"Updated metadata: {chunk_id} now stored on {stored}")
                                self.save_metadata()
                                break
            except Exception as e:
                logger.warning(f"RE-REPLICATION FAILED: Chunk {chunk_id} from {source} to {target}: {e}")
                success = False

            if not success:
                retries += 1
                if retries <= config.REPL_MAX_RETRIES:
                    backoff = config.REPL_BACKOFF_BASE ** retries
                    logger.info(f"Re-enqueueing {chunk_id} -> {target} for retry {retries} after {backoff}s")
                    task['retries'] = retries
                    time.sleep(backoff)
                    self.repl_queue.put(task)
                else:
                    logger.error(f"Replication failed for {chunk_id} -> {target} after {retries} retries; alerting")
                    with self.lock:
                        self.alerts.append({'chunk_id': chunk_id, 'target': target, 'error': 'replication_failed', 'time': datetime.now().isoformat()})

            self._save_repl_queue()
            time.sleep(0.1)

    # ---------- replication helpers ----------
    def rebuild_for_node(self, target_dnid):
        """When a datanode comes back, enqueue replication tasks to ensure chunks are restored onto it."""
        logger.info(f"Scheduling rebuild for {target_dnid} - restoring all missing chunks")

        # Wait a short time to allow any late report_chunk RPCs to arrive and be persisted.
        try:
            delay = getattr(config, 'REBUILD_DELAY', 3)
            if delay > 0:
                logger.info(f"Delaying rebuild_for_node for {target_dnid} by {delay}s to allow late reports")
                time.sleep(delay)
        except Exception:
            pass

        with self.lock:
            to_replicate = []
            for fname, info in self.metadata.items():
                for ci in info['chunks']:
                    # only enqueue if target is expected to have the chunk
                    if ci['primary_datanode'] == target_dnid or ci['replica_datanode'] == target_dnid:
                        # if stored_on already records target, skip
                        stored = ci.get('stored_on', [])
                        if target_dnid in stored:
                            continue
                        to_replicate.append({'chunk_id': ci['chunk_id'], 'chunk_meta': ci})

        logger.info(f"Found {len(to_replicate)} chunks to rebuild on {target_dnid}")

        for item in to_replicate:
            chunk_id = item['chunk_id']
            ci = item['chunk_meta']
            # prefer a source that NN metadata already records as having the chunk
            stored = ci.get('stored_on', [])
            source = None
            for s in stored:
                if s != target_dnid:
                    source = s
                    break

            # Fallback: use the other planned replica (only if it appears in metadata at all)
            if not source:
                other = ci['primary_datanode'] if ci['primary_datanode'] != target_dnid else ci['replica_datanode']
                # double-check that we think 'other' actually has the chunk
                if other in stored:
                    source = other

            if not source:
                logger.warning(f"No known source (per metadata.stored_on) for {chunk_id}; skipping enqueue")
                continue

            logger.info(f"Rebuilding chunk {chunk_id} from {source} to {target_dnid} (expected replica location)")
            self.enqueue_replication(chunk_id, source, target_dnid)
        
        logger.info(f"Rebuild complete: enqueued {len(to_replicate)} chunks for {target_dnid}")

    def periodic_replication_check(self):
        """Periodically check replication status and fix under-replicated chunks."""
        logger.info("Starting periodic replication health check")
        
        while True:
            try:
                time.sleep(30)  # Check every 30 seconds
                logger.info("Running periodic replication health check")
                
                with self.lock:
                    under_replicated = []
                    for fname, info in self.metadata.items():
                        for ci in info['chunks']:
                            chunk_id = ci['chunk_id']
                            stored = ci.get('stored_on', [])
                            
                            # Check if chunk has proper replication (should be on 2 datanodes)
                            if len(stored) < 2:
                                # Find missing replicas
                                expected_replicas = [ci['primary_datanode'], ci['replica_datanode']]
                                missing_replicas = []
                                
                                for expected in expected_replicas:
                                    if expected not in stored:
                                        # Check if the expected node is alive
                                        if expected in self.datanode_health:
                                            if self.datanode_health[expected]['status'] == 'alive':
                                                missing_replicas.append(expected)
                                
                                # If we have sources and missing replicas, schedule re-replication
                                if stored and missing_replicas:
                                    for target in missing_replicas:
                                        source = stored[0]  # Use first available replica as source
                                        under_replicated.append({
                                            'chunk_id': chunk_id,
                                            'source': source,
                                            'target': target,
                                            'reason': 'under_replicated'
                                        })
                                        logger.info(f"Chunk {chunk_id} under-replicated ({len(stored)}/2 replicas), will re-replicate from {source} to {target}")
                
                # Enqueue replication tasks
                for task in under_replicated:
                    self.enqueue_replication(task['chunk_id'], task['source'], task['target'])
                
                if under_replicated:
                    logger.info(f"Periodic check: enqueued {len(under_replicated)} chunks for re-replication")
                else:
                    logger.info("Periodic check: all chunks properly replicated")
                    
            except Exception as e:
                logger.exception(f"Error in periodic replication check: {e}")
                time.sleep(30)

if __name__ == '__main__':
    nn = Namenode()
    def _sig(signum, frame):
        nn.shutdown()
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    nn.start()