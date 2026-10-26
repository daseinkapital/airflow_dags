from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


STAGING_DIR = Path("/opt/airflow/data/staging")
CACHE_DB = Path("/opt/airflow/data/media_cache.db")
VIDEO_SUFFIXES = {
    ".mkv",
    ".mp4",
    ".m4v",
    ".mov",
    ".avi",
    ".ts",
    ".m2ts",
    ".wmv",
    ".webm",
}
WRITE_STABILITY_SECONDS = 60
MEDIA_TYPE = "tv"


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _ensure_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS media_metadata_cache (
            media_type TEXT NOT NULL,
            id TEXT NOT NULL,
            path TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            mtime REAL NOT NULL,
            has_chapters INTEGER NOT NULL,
            checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (media_type, id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_media_metadata_cache_tv_state
        ON media_metadata_cache (media_type, file_size, mtime)
        """
    )
    conn.commit()


def _ffprobe_has_chapters(file_path: Path) -> bool:
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        raise FileNotFoundError("ffprobe not found on PATH")

    result = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_chapters",
            str(file_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        error_text = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"ffprobe failed for {file_path}: {error_text}")

    stdout = result.stdout.strip()
    if not stdout:
        return False

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe returned invalid JSON for {file_path}: {exc}") from exc

    return bool(payload.get("chapters") or [])


def process_single_file(file_path_str: str) -> tuple[str, str, bool]:
    file_path = Path(file_path_str)
    has_chapters = _ffprobe_has_chapters(file_path)
    return MEDIA_TYPE, str(file_path.name), has_chapters


@task
def scan_staging_media_parallel() -> str:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    current_time = time.time()
    files_to_process: list[str] = []

    conn = _connect_db(CACHE_DB)
    try:
        _ensure_cache_table(conn)

        for file_path in sorted(STAGING_DIR.rglob("*")):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in VIDEO_SUFFIXES:
                continue

            stat = file_path.stat()
            if (current_time - stat.st_mtime) < WRITE_STABILITY_SECONDS:
                continue

            file_id = file_path.name
            abs_path = str(file_path.resolve())
            row = conn.execute(
                "SELECT file_size, mtime FROM media_metadata_cache WHERE media_type = ? AND id = ?",
                (MEDIA_TYPE, file_id),
            ).fetchone()

            if row is None or row[0] != stat.st_size or row[1] != stat.st_mtime:
                files_to_process.append(abs_path)
    finally:
        conn.close()

    if not files_to_process:
        return "No new files to process."

    max_workers = max(1, min(len(files_to_process), os.cpu_count() or 1))
    print(f"Processing {len(files_to_process)} files in parallel with {max_workers} workers...")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_file, file_path): file_path
            for file_path in files_to_process
        }

        write_conn = _connect_db(CACHE_DB)
        try:
            for future in as_completed(futures):
                source_path = futures[future]
                try:
                    media_type, file_id, has_chapters = future.result()
                    stat = Path(source_path).stat()

                    write_conn.execute(
                        """
                        INSERT INTO media_metadata_cache (
                            media_type,
                            id,
                            path,
                            file_size,
                            mtime,
                            has_chapters,
                            checked_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(media_type, id) DO UPDATE SET
                            path = excluded.path,
                            file_size = excluded.file_size,
                            mtime = excluded.mtime,
                            has_chapters = excluded.has_chapters,
                            checked_at = excluded.checked_at
                        """,
                        (media_type, file_id, source_path, stat.st_size, stat.st_mtime, int(has_chapters)),
                    )
                    write_conn.commit()
                except Exception as exc:
                    print(f"Failed to process {source_path}: {exc}")
        finally:
            write_conn.close()

    return f"Processed {len(files_to_process)} files."


@dag(
    dag_id="tv_chapters_check",
    description=(
        "Scan staged TV media, skip unstable files, "
        "check chapter metadata with ffprobe, and cache results in SQLite."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    tags=["media", "tv", "chapters"],
)
def tv_chapters_check_dag():
    scan_staging_media_parallel()


tv_chapters_check_dag()
