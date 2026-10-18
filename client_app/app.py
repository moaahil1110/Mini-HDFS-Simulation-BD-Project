from flask import Flask, render_template, request, jsonify, send_file
import socket
import os
import traceback

from werkzeug.utils import secure_filename

import config
from protocol import send_json, recv_json

app = Flask(__name__)

class HDFSClient:
    """
    Client speaks to Namenode for plans, and directly to Datanodes for data.
    Uses framed JSON for control messages and raw bytes for chunk payloads.
    """

    def __init__(self):
        self.namenode_host = config.NAMENODE_HOST
        self.namenode_port = config.NAMENODE_PORT

    # --- NN RPC ---
    def _send_nn(self, req: dict) -> dict:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.namenode_host, self.namenode_port))
        send_json(s, req)
        resp = recv_json(s)
        s.close()
        return resp

    # --- Upload path ---
    def upload_file(self, filepath, filename):
        file_size = os.path.getsize(filepath)
        resp = self._send_nn({'command': 'upload', 'filename': filename, 'file_size': file_size})
        if resp.get('status') != 'success':
            return resp

        chunks_info, dn_info = resp['chunks'], resp['datanode_info']

        with open(filepath, 'rb') as f:
            for ci in chunks_info:
                chunk = f.read(config.CHUNK_SIZE)
                # write to both replicas
                for dnname in ['primary_datanode', 'replica_datanode']:
                    host, port = dn_info[ci[dnname]]['host'], dn_info[ci[dnname]]['port']
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((host, port))
                    send_json(s, {'action': 'store', 'chunk_id': ci['chunk_id'], 'chunk_size': len(chunk)})
                    ready = s.recv(5)  # 'READY'
                    s.sendall(chunk)
                    ack = recv_json(s)
                    s.close()
                    if ack.get('status') != 'success':
                        return {'status': 'error', 'message': f"Store failed on {dnname}: {ack}"}
                    # report checksum and storage to Namenode so it can persist per-chunk checksum
                    try:
                        report = {
                            'command': 'report_chunk',
                            'filename': filename,
                            'chunk_id': ci['chunk_id'],
                            'datanode_id': ci[dnname],
                            'checksum': ack.get('checksum'),
                            'chunk_size': ack.get('chunk_size'),
                        }
                        self._send_nn(report)
                    except Exception:
                        traceback.print_exc()
        return {'status': 'success', 'message': 'Upload complete!'}

    # --- Download helpers ---
    def _download_from_dn(self, host, port, chunk_id):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(7)
        s.connect((host, port))
        send_json(s, {'action': 'retrieve', 'chunk_id': chunk_id})
        meta = recv_json(s)
        if meta.get('status') != 'success':
            s.close()
            return None, meta.get('message', 'unknown error')

        # acknowledge and read exact number of bytes
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
            return None, 'short read'
        return data, None

    # --- Download path ---
    def download_file(self, filename, output_path):
        resp = self._send_nn({'command': 'download', 'filename': filename})
        if resp.get('status') != 'success':
            return resp

        chunks, dn_info = resp['file_info']['chunks'], resp['datanode_info']

        with open(output_path, 'wb') as f:
            for ci in chunks:
                primary = ci['primary_datanode']
                replica = ci['replica_datanode']
                attempts = [primary, replica]
                success = False
                errs = []
                for dnname in attempts:
                    host, port = dn_info[dnname]['host'], dn_info[dnname]['port']
                    data, err = self._download_from_dn(host, port, ci['chunk_id'])
                    if data is not None:
                        f.write(data)
                        success = True
                        break
                    errs.append((dnname, err))
                if not success:
                    return {'status': 'error', 'message': f"Failed chunk {ci['chunk_id']} from both replicas: {errs}"}

        return {'status': 'success', 'message': 'Download complete!'}


client = HDFSClient()

# -------------------- Flask Routes --------------------

@app.route('/')
def index():
    # Serve dashboard UI
    return send_file(os.path.join(os.path.dirname(__file__), 'templates', 'index.html'))


@app.route('/api/upload', methods=['POST'])
def upload():
    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'no file part in request'}), 400
        file = request.files['file']
        # Sanitize the name: it is used as a local path here and becomes part of
        # the chunk IDs that Datanodes join onto their storage directory.
        filename = secure_filename(file.filename or '')
        if not filename:
            return jsonify({'status': 'error', 'message': 'invalid filename'}), 400
        os.makedirs(config.UPLOAD_DIR, exist_ok=True)
        path = os.path.join(config.UPLOAD_DIR, filename)
        file.save(path)

        result = client.upload_file(path, filename)

        try:
            os.remove(path)
        except Exception:
            pass

        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/download/<filename>')
def download(filename):
    try:
        os.makedirs(config.UPLOAD_DIR, exist_ok=True)
        path = os.path.abspath(os.path.join(config.UPLOAD_DIR, filename))

        # Prevent directory traversal (security)
        if not path.startswith(os.path.abspath(config.UPLOAD_DIR)):
            return jsonify({'status': 'error', 'message': 'Invalid filename'}), 400

        result = client.download_file(filename, path)

        if result.get('status') == 'success' and os.path.exists(path):
            # ✅ File ready to send
            return send_file(path, as_attachment=True)

        # ❌ Download logic failed
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/files')
def files():
    return jsonify(client._send_nn({'command': 'list'}))


@app.route('/api/status')
def status():
    return jsonify(client._send_nn({'command': 'status'}))


if __name__ == '__main__':
    # Never enable debug here: the Werkzeug debugger allows remote code execution
    # and the app binds to all interfaces.
    app.run(host=config.CLIENT_HOST, port=config.CLIENT_PORT, debug=False)
