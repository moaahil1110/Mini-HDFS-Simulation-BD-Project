# Smoke tests for the length-prefixed JSON framing in protocol.py
import socket
import struct

import pytest

import protocol


@pytest.fixture(autouse=True)
def _isolate_storage(tmp_path, monkeypatch):
    # protocol.py appends a trace to ./storage; keep it out of the repo
    monkeypatch.chdir(tmp_path)


def test_send_recv_roundtrip():
    a, b = socket.socketpair()
    with a, b:
        msg = {"type": "HEARTBEAT", "node_id": 0, "chunks": ["c1", "c2"]}
        protocol.send_json(a, msg)
        assert protocol.recv_json(b) == msg


def test_recv_rejects_oversized_frame():
    a, b = socket.socketpair()
    with a, b:
        a.sendall(struct.pack("!I", 10_000_001))
        with pytest.raises(protocol.ProtocolError):
            protocol.recv_json(b)
