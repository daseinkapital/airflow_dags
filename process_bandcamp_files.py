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
            "-f mp3-320"
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
    def move_downloads_to_music(staging_dir: str) -> str:
        staging_path = Path(staging_dir)
        if not staging_path.exists():
            raise FileNotFoundError(f"Bandcamp staging directory does not exist: {staging_dir}")

        music_path = Path(MUSIC_DIR)
        music_path.mkdir(parents=True, exist_ok=True)

        moved = 0
        for entry in sorted(staging_path.iterdir()):
            if entry.name.startswith("."):
                continue
            if entry.name in {"music", "downloads"}:
                continue

            dest = music_path / entry.name
            if entry.is_dir():
                if dest.exists():
                    print(f"Destination already exists, skipping: {dest}")
                    continue
                shutil.move(str(entry), str(dest))
                moved += 1
            elif entry.is_file():
                if dest.exists():
                    continue
                shutil.move(str(entry), str(dest))
                moved += 1

        print(f"Moved {moved} item(s) from Bandcamp staging into {MUSIC_DIR}")
        return str(music_path)

    sync_task = sync_bandcamp_downloads()
    move_task = move_downloads_to_music(sync_task)

    sync_task >> move_task


process_bandcamp_files()