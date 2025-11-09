# protocol.py - Length-prefixed JSON framing for control messages
# WHY: TCP is a stream; we must frame JSON messages to avoid partial/coalesced reads.

import json
import struct

_HDR_SIZE = 4  # 4-byte big-endian length prefix

class ProtocolError(Exception):
    pass

def _recv_exact(sock, n: int) -> bytes:
    """Receive exactly n bytes or raise if the socket closes early."""
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('socket closed during recv')
        buf += chunk
    return buf

def send_json(sock, obj) -> None:
    """Send a JSON object with a 4-byte length prefix."""
    data = json.dumps(obj).encode('utf-8')
    sock.sendall(struct.pack('!I', len(data)))
    sock.sendall(data)

def recv_json(sock):
    """Receive a length-prefixed JSON object."""
    hdr = _recv_exact(sock, _HDR_SIZE)
    (n,) = struct.unpack('!I', hdr)
    if n < 0 or n > 10_000_000:
        raise ProtocolError(f'invalid frame length: {n}')
    body = _recv_exact(sock, n)
    return json.loads(body.decode('utf-8'))