import hashlib
from pathlib import Path

from radar.common.hashing import sha256_file


def test_sha256_file_reads_in_chunks(tmp_path: Path) -> None:
    payload = b"radar" * 100
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)

    assert sha256_file(path, chunk_size=7) == hashlib.sha256(payload).hexdigest()
