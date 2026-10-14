"""Download, verify and unzip the IndPenSim 100-batch dataset.

    python -m streaming.fetch_data --dest data/raw

Source: Mendeley Data, dataset pdnjz7zz5x v1 (CC BY 4.0), "Data for: Modern day
monitoring and control challenges outlined on an industrial-scale benchmark
fermentation process" (Goldrick et al.).

* Resumes a partial download (HTTP Range) instead of starting again.
* Verifies the zip's MD5 before trusting it.
* Extracts the CSVs flat into --dest (no Mendeley_data/ subfolder).
* Does nothing if the extracted CSV is already there at the right size.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import shutil
import sys
import zipfile
from pathlib import Path

import requests

log = logging.getLogger("fetch_data")

URL = (
    "https://data.mendeley.com/public-files/datasets/pdnjz7zz5x/files/"
    "e7007dff-4061-494f-a9f5-16c20f457d1d/file_downloaded"
)
ZIP_NAME = "100_Batches_IndPenSim.zip"
ZIP_SIZE = 503_792_426
ZIP_MD5 = "d4662f3eafe726f7e3d43354e431c0eb"
CSV_NAME = "100_Batches_IndPenSim_V3.csv"
CSV_SIZE = 2_566_827_626
CHUNK = 1 << 20


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def download(dest_zip: Path, url: str = URL, total: int = ZIP_SIZE, session: requests.Session | None = None) -> None:
    """Download to dest_zip, resuming from whatever is already there."""
    session = session or requests.Session()
    have = dest_zip.stat().st_size if dest_zip.exists() else 0
    if have > total:
        log.warning("%s is larger than expected; starting again", dest_zip)
        dest_zip.unlink()
        have = 0
    if have == total:
        return
    headers = {"Range": f"bytes={have}-"} if have else {}
    log.info("downloading %s (%s of %.0f MB already here)", dest_zip.name, f"{have / 1e6:.0f} MB", total / 1e6)
    with session.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        if have and r.status_code != 206:  # server ignored Range: start again
            log.info("server did not resume; downloading from the start")
            have = 0
        mode = "ab" if have else "wb"
        next_log = have + 50_000_000
        with open(dest_zip, mode) as f:
            for block in r.iter_content(CHUNK):
                f.write(block)
                have += len(block)
                if have >= next_log:
                    log.info("  %.0f / %.0f MB", have / 1e6, total / 1e6)
                    next_log += 50_000_000


def extract_flat(zip_path: Path, dest: Path) -> list[Path]:
    """Extract every file in the zip directly into dest, ignoring folders inside the zip."""
    out = []
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            target = dest / Path(info.filename).name
            log.info("extracting %s (%.0f MB)", target.name, info.file_size / 1e6)
            with z.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, 8 * CHUNK)
            out.append(target)
    return out


def fetch(dest: Path, keep_zip: bool = True) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    csv = dest / CSV_NAME
    if csv.exists() and csv.stat().st_size == CSV_SIZE:
        log.info("%s already present; nothing to do", csv)
        return csv
    zip_path = dest / ZIP_NAME
    download(zip_path)
    log.info("verifying checksum")
    got = md5_of(zip_path)
    if got != ZIP_MD5:
        zip_path.unlink()
        raise RuntimeError(f"checksum mismatch ({got}); deleted the zip, run again to re-download")
    extract_flat(zip_path, dest)
    if not keep_zip:
        zip_path.unlink()
    log.info("ready: %s", csv)
    return csv


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dest", type=Path, default=Path("data/raw"))
    p.add_argument("--delete-zip", action="store_true", help="remove the zip after extracting")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    fetch(args.dest, keep_zip=not args.delete_zip)
    return 0


if __name__ == "__main__":
    sys.exit(main())
