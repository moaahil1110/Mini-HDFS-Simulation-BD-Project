# Upload route must sanitize the client-supplied filename before using it
import importlib.util
import io
import sys
from pathlib import Path

import pytest

CLIENT_DIR = Path(__file__).resolve().parent.parent / 'client_app'


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(CLIENT_DIR))
    spec = importlib.util.spec_from_file_location('client_app_app', CLIENT_DIR / 'app.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod.config, 'UPLOAD_DIR', str(tmp_path / 'uploads'))
    yield mod
    sys.modules.pop('client_app_app', None)


def _post(mod, name, calls):
    def fake_upload(path, filename):
        calls.append((path, filename))
        return {'status': 'success'}

    mod.client.upload_file = fake_upload
    data = {'file': (io.BytesIO(b'hello'), name)}
    return mod.app.test_client().post('/api/upload', data=data, content_type='multipart/form-data')


def test_traversal_name_is_sanitized(app_module, tmp_path):
    calls = []
    resp = _post(app_module, '../../evil.txt', calls)
    assert resp.status_code == 200
    (path, filename), = calls
    assert filename == 'evil.txt'
    assert Path(path).resolve().parent == (tmp_path / 'uploads').resolve()
    assert not (tmp_path.parent / 'evil.txt').exists()


def test_name_that_sanitizes_to_empty_is_rejected(app_module):
    calls = []
    resp = _post(app_module, '..', calls)
    assert resp.status_code == 400
    assert calls == []
