import hashlib
import zipfile

import pytest

from streaming import fetch_data as fd

PAYLOAD = bytes(range(256)) * 400  # 102,400 bytes standing in for the zip


class FakeResponse:
    def __init__(self, body: bytes, status: int):
        self.body, self.status_code = body, status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, n):
        for i in range(0, len(self.body), n):
            yield self.body[i : i + n]


class FakeSession:
    def __init__(self, honour_range=True):
        self.honour_range, self.ranges = honour_range, []

    def get(self, url, headers, stream, timeout):
        rng = headers.get("Range")
        self.ranges.append(rng)
        if rng and self.honour_range:
            start = int(rng.split("=")[1].rstrip("-"))
            return FakeResponse(PAYLOAD[start:], 206)
        return FakeResponse(PAYLOAD, 200)


def test_fresh_download(tmp_path):
    z = tmp_path / "f.zip"
    s = FakeSession()
    fd.download(z, url="x", total=len(PAYLOAD), session=s)
    assert z.read_bytes() == PAYLOAD and s.ranges == [None]


def test_resume_partial_download(tmp_path):
    z = tmp_path / "f.zip"
    z.write_bytes(PAYLOAD[:30_000])
    s = FakeSession()
    fd.download(z, url="x", total=len(PAYLOAD), session=s)
    assert z.read_bytes() == PAYLOAD and s.ranges == ["bytes=30000-"]


def test_server_ignoring_range_restarts_cleanly(tmp_path):
    z = tmp_path / "f.zip"
    z.write_bytes(PAYLOAD[:30_000])
    fd.download(z, url="x", total=len(PAYLOAD), session=FakeSession(honour_range=False))
    assert z.read_bytes() == PAYLOAD


def test_complete_file_is_not_downloaded_again(tmp_path):
    z = tmp_path / "f.zip"
    z.write_bytes(PAYLOAD)
    s = FakeSession()
    fd.download(z, url="x", total=len(PAYLOAD), session=s)
    assert s.ranges == []


def test_extract_flattens_folders(tmp_path):
    z = tmp_path / "d.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("Mendeley_data/", "")
        zf.writestr("Mendeley_data/a.csv", "x,y\n1,2\n")
    out = fd.extract_flat(z, tmp_path)
    assert [p.name for p in out] == ["a.csv"] and (tmp_path / "a.csv").read_text() == "x,y\n1,2\n"


def test_md5(tmp_path):
    p = tmp_path / "f"
    p.write_bytes(PAYLOAD)
    assert fd.md5_of(p) == hashlib.md5(PAYLOAD).hexdigest()


def test_bad_checksum_deletes_zip(tmp_path, monkeypatch):
    monkeypatch.setattr(fd, "download", lambda path: path.write_bytes(b"corrupt"))
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        fd.fetch(tmp_path)
    assert not (tmp_path / fd.ZIP_NAME).exists()
