from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

STORAGE_ROOT = os.getenv("STORAGE_ROOT") or os.getenv("AIRFLOW_VAR_STORAGE_ROOT") or "/opt/airflow/data/desktop/external"
BANDCAMP_STAGING_DIR = os.path.join(STORAGE_ROOT, "staging", "music", "bandcamp")
MUSIC_DIR = os.path.join(STORAGE_ROOT, "music")
COOKIE_PATH = "/opt/airflow/config/bc-cookie.txt"


@dag(start_date=datetime(2026, 1, 1), schedule="@hourly")
def process_bandcamp_files():
    @task
    def sync_bandcamp_downloads() -> str:
        os.makedirs(BANDCAMP_STAGING_DIR, exist_ok=True)
        os.makedirs(MUSIC_DIR, exist_ok=True)

        if not os.path.exists(COOKIE_PATH):
            raise FileNotFoundError(f"Bandcamp cookie file not found: {COOKIE_PATH}")

        cmd = [
            "bandcampsync",
            "-c",
            COOKIE_PATH,
            "-d",
            BANDCAMP_STAGING_DIR,
            "-f",
            "mp3-320",
        ]

        print(f"Running Bandcamp sync: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"bandcampsync failed with exit code {result.returncode}")

        return BANDCAMP_STAGING_DIR

    @task
    def list_album_dirs(staging_dir: str) -> list[str]:
        staging_path = Path(staging_dir)
        if not staging_path.exists():
            raise FileNotFoundError(f"Bandcamp staging directory does not exist: {staging_dir}")

        albums = []
        for child in sorted(staging_path.iterdir()):
            if child.name.startswith("."):
                continue
            if child.name in {"music", "downloads"}:
                continue
            if child.is_dir():
                albums.append(str(child))
        return albums

    @task
    def move_album(album_dir: str) -> str:
        src_dir = Path(album_dir)
        if not src_dir.is_dir():
            raise FileNotFoundError(f"Album directory not found: {album_dir}")

        music_path = Path(MUSIC_DIR)
        music_path.mkdir(parents=True, exist_ok=True)

        dest_dir = music_path / src_dir.name
        dest_dir.mkdir(parents=True, exist_ok=True)

        for src_root, _, files in os.walk(src_dir):
            src_root_path = Path(src_root)
            rel_root = src_root_path.relative_to(src_dir)
            target_root = dest_dir / rel_root
            target_root.mkdir(parents=True, exist_ok=True)

            for filename in files:
                src_file = src_root_path / filename
                dst_file = target_root / filename
                if dst_file.exists():
                    print(f"Replacing existing file: {dst_file}")
                    dst_file.unlink()
                shutil.move(str(src_file), str(dst_file))

        for path in sorted(src_dir.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass

        if src_dir.exists():
            try:
                src_dir.rmdir()
            except OSError:
                pass

        print(f"Moved album from {src_dir} to {dest_dir}")
        return str(dest_dir)

    sync_task = sync_bandcamp_downloads()
    album_dirs = list_album_dirs(sync_task)
    moved_albums = move_album.expand(album_dir=album_dirs)

    sync_task >> album_dirs >> moved_albums


process_bandcamp_files()