"""
DAG: tv_library_structure_check
==============================
Validates a TV library directory for the expected folder structure:

  Show Name (Year)/Season 0X/Show Name (Year) - SXXEXX[ - Episode Name].ext

Examples:
  The Office (2005)/Season 01/The Office (2005) - S01E01.mkv
  The Office (2005)/Season 01/The Office (2005) - S01E02 - Diversity Day.mkv
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.sdk import Param, dag, task

log = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {
    ".avi",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ts",
    ".webm",
    ".wmv",
}

SHOW_DIR_RE = re.compile(r"^(?P<show>.+?) \((?P<year>\d{4})\)$")
SEASON_DIR_RE = re.compile(r"^Season (?P<season>\d{2})$")
EPISODE_FILE_RE = re.compile(
    r"^(?P<show>.+?) \((?P<year>\d{4})\) - S(?P<season>\d{2})E(?P<episode>\d{2})(?: - (?P<name>.+?))?(?P<ext>\.[A-Za-z0-9]+)$"
)

DEFAULT_STORAGE_ROOT = (
    os.getenv("STORAGE_ROOT")
    or os.getenv("AIRFLOW_VAR_STORAGE_ROOT")
    or "/opt/airflow/data/desktop/external"
)

DEFAULT_TV_ROOT = os.path.join(DEFAULT_STORAGE_ROOT, "tv")


def _validate_show_dir(name: str) -> tuple[str | None, str | None]:
    match = SHOW_DIR_RE.fullmatch(name)
    if not match:
        return None, None
    return match.group("show"), match.group("year")


def _validate_episode_path(path: Path, root: Path) -> list[str]:
    rel = path.relative_to(root)
    parts = rel.parts
    violations: list[str] = []

    if len(parts) != 3:
        violations.append(
            f"{rel}: expected '<Show Name (Year)>/<Season 0X>/<Show Name (Year)> - SXXEXX[ - Episode Name].ext'"
        )
        return violations

    show_dir, season_dir, episode_name = parts
    show_name, show_year = _validate_show_dir(show_dir)
    if show_name is None:
        violations.append(
            f"{rel}: folder '{show_dir}' is not in the format 'Show Name (Year)'"
        )

    if not SEASON_DIR_RE.fullmatch(season_dir):
        violations.append(f"{rel}: season folder '{season_dir}' is not in the format 'Season 0X'")

    file_match = EPISODE_FILE_RE.fullmatch(episode_name)
    if not file_match:
        violations.append(
            f"{rel}: episode filename '{episode_name}' is not in the format 'Show Name (Year) - SXXEXX[ - Episode Name].ext'"
        )
        return violations

    if show_name is not None:
        if file_match.group("show") != show_name:
            violations.append(
                f"{rel}: episode file show name '{file_match.group('show')}' does not match folder show '{show_name}'"
            )
        if file_match.group("year") != show_year:
            violations.append(
                f"{rel}: episode file year '{file_match.group('year')}' does not match folder year '{show_year}'"
            )

    if Path(episode_name).suffix.lower() not in VIDEO_EXTENSIONS:
        violations.append(f"{rel}: unsupported extension '{Path(episode_name).suffix}'")

    return violations


def validate_tv_directory(root_dir: str) -> list[str]:
    root_path = Path(root_dir)
    if not root_path.exists():
        raise AirflowException(f"TV root does not exist: {root_dir}")
    if not root_path.is_dir():
        raise AirflowException(f"TV root is not a directory: {root_dir}")

    violations: list[str] = []

    for current_root, dirnames, filenames in os.walk(root_path):
        current_path = Path(current_root)
        dirnames.sort()
        for dirname in sorted(dirnames):
            directory = current_path / dirname
            relative = directory.relative_to(root_path)
            if len(relative.parts) == 1:
                if _validate_show_dir(dirname)[0] is None:
                    violations.append(
                        f"{relative}: root-level directory is not in the format 'Show Name (Year)'"
                    )
            elif len(relative.parts) == 2:
                if not SEASON_DIR_RE.fullmatch(dirname):
                    violations.append(
                        f"{relative}: season directory is not in the format 'Season 0X'"
                    )

        for filename in sorted(filenames):
            file_path = current_path / filename
            if file_path.suffix.lower() not in VIDEO_EXTENSIONS:
                continue

            violations.extend(_validate_episode_path(file_path, root_path))

    return violations


@dag(
    dag_id="tv_library_structure_check",
    description="Validate that each TV show follows Show Name (Year)/Season 0X/Show Name (Year) - SXXEXX naming conventions.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "tv_root": Param(
            DEFAULT_TV_ROOT,
            type="string",
            description="TV folder containing every show in the format Show Name (Year). Set TV_ROOT or AIRFLOW_VAR_TV_ROOT. The mounted storage root can be changed via STORAGE_ROOT.",
        )
    },
    tags=["tv", "media", "compliance"],
)
def tv_library_structure_check():
    @task
    def check_tv_structure(tv_root: str) -> dict:
        log.info("Checking TV library structure for directory: %s", tv_root)
        violations = validate_tv_directory(tv_root)

        if violations:
            displayed_violations = violations[:50]
            if len(violations) > 50:
                displayed_violations.append(f"... and {len(violations) - 50} more invalid paths")
            summary = "\n".join(f"- {item}" for item in displayed_violations)
            log.error(
                "TV library structure validation failed. Invalid paths:\n%s",
                summary,
            )
            raise AirflowException(
                f"Found {len(violations)} invalid file/folder paths under {tv_root}:\n{summary}"
            )

        log.info("TV library structure validation passed for %s", tv_root)
        return {"tv_root": tv_root, "valid": True, "violation_count": 0}

    check_tv_structure("{{ params.tv_root }}")


tv_library_structure_check()
