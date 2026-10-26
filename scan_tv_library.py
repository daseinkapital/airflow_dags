# """
# DAG: video_process
# ==================
# Scans a directory for video files, probes each one with ffprobe, then runs
# ffmpeg from the host environment using whatever is available on PATH.

# Workflow
# --------
#   scan_and_process   <- single task: walks a configured directory, probes
#                         each file with ffprobe, runs ffmpeg locally, and
#                         executes file work in a ThreadPoolExecutor.
# """

# from __future__ import annotations

# import json
# import logging
# import os
# import shutil
# import subprocess
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from datetime import datetime, timedelta

# from airflow.sdk import Param, dag, task

# log = logging.getLogger(__name__)

# # ── Constants ──────────────────────────────────────────────────────────────

# VIDEO_EXTENSIONS = frozenset({".mp4", ".mkv", ".mov", ".avi", ".m4v", ".ts", ".webm"})

# HOST_DATA_DIR      = "/mnt/desktop/shared/tv"                 # <-- set this
# CONTAINER_DATA_DIR = "/opt/airflow/data/desktop/external/tv"  # must match your compose mount

# MAX_PARALLEL = 4   # max simultaneous ffmpeg containers


# # ── Helpers ─────────────────────────────────────────────────────────────────

# def _resolve_executable(name: str) -> str:
#     """Return the executable path from PATH or raise a clear error."""
#     executable = shutil.which(name)
#     if executable is None:
#         raise FileNotFoundError(f"{name} not found on PATH")
#     return executable


# def _ffprobe(file_path: str, flag: str) -> dict:
#     """Run ffprobe from the host environment and parse its JSON output."""
#     try:
#         ffprobe_path = _resolve_executable("ffprobe")
#         result = subprocess.run(
#             [ffprobe_path, "-v", "quiet", "-print_format", "json", flag, file_path],
#             capture_output=True,
#             text=True,
#             check=False,
#         )
#         if result.returncode != 0:
#             raise RuntimeError(result.stderr.strip() or "ffprobe failed")

#         if not result.stdout.strip():
#             return {}

#         return json.loads(result.stdout)
#     except Exception as exc:
#         log.error(
#             json.dumps({
#                 "event": "ffprobe_error",
#                 "file": os.path.basename(file_path),
#                 "flag": flag,
#                 "error": str(exc),
#             })
#         )
#         return {}


# def build_ffmpeg_command(file_path: str, chapters_data: dict) -> list[str] | None:
#     """
#     Return the ffmpeg argv list to run locally, or None to skip.

#     Edit this function to change what ffmpeg does — transcode, extract
#     chapters, add subtitles, watermark, etc.

#     Default: lossless remux to .mkv (fast, no re-encode).
#     """
#     chapters = chapters_data.get("chapters", [])
#     if chapters:
#         log.info(
#             json.dumps({
#                 "event": "skipping_ffmpeg",
#                 "reason": "file_already_has_chapters",
#                 "file": file_path,
#                 "chapter_count": len(chapters),
#             })
#         )
#         return None

#     ffmpeg_path = _resolve_executable("ffmpeg")
#     base, _ = os.path.splitext(file_path)
#     return [ffmpeg_path, "-y", "-i", file_path, "-c", "copy", f"{base}.converted.mkv"]


# def _process_one(file_path: str) -> dict:
#     """Probe a single file and run ffmpeg on the host if needed."""
#     file_name = os.path.basename(file_path)

#     chapters_data = _ffprobe(file_path, "-show_chapters")
#     format_data = _ffprobe(file_path, "-show_format")
#     fmt = format_data.get("format", {})

#     log.info(
#         json.dumps({
#             "event": "probe_complete",
#             "file_name": file_name,
#             "chapter_count": len(chapters_data.get("chapters", [])),
#             "duration_seconds": round(float(fmt.get("duration", 0) or 0), 3),
#             "format_name": fmt.get("format_name", "unknown"),
#         })
#     )

#     ffmpeg_cmd = build_ffmpeg_command(file_path, chapters_data)
#     if ffmpeg_cmd is None:
#         return {"file": file_name, "status": "skipped"}

#     log.info(json.dumps({"event": "ffmpeg_start", "file": file_name, "command": ffmpeg_cmd}))

#     subprocess.run(ffmpeg_cmd, check=True)

#     log.info(json.dumps({"event": "ffmpeg_done", "file": file_name}))
#     return {"file": file_name, "status": "done"}


# # ── DAG ─────────────────────────────────────────────────────────────────────

# @dag(
#     dag_id="video_process",
#     description="Probe and process video files with ffprobe + ffmpeg from PATH.",
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     default_args={
#         "retries": 1,
#         "retry_delay": timedelta(minutes=2),
#     },
#     params={
#         "video_dir": Param(
#             HOST_DATA_DIR,
#             type="string",
#             description="Directory to scan on the host machine running Airflow.",
#         ),
#     },
#     tags=["media", "tv", "ffmpeg"],
# )
# def video_process():

#     @task
#     def scan_and_process(video_dir: str) -> None:
#         """
#         Walk *video_dir*, probe every video file, and run ffmpeg on each one
#         that needs processing. MAX_PARALLEL local ffmpeg processes run at a time.
#         """
#         if not os.path.isdir(video_dir):
#             log.warning(json.dumps({"event": "directory_not_found", "path": video_dir}))
#             return

#         files = sorted(
#             os.path.join(root, fname)
#             for root, _dirs, fnames in os.walk(video_dir)
#             for fname in fnames
#             if os.path.splitext(fname)[1].lower() in VIDEO_EXTENSIONS
#         )

#         log.info(json.dumps({"event": "scan_complete", "file_count": len(files)}))

#         counts = {"done": 0, "skipped": 0, "error": 0}

#         with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
#             futures = {pool.submit(_process_one, f): f for f in files}
#             for future in as_completed(futures):
#                 file_path = futures[future]
#                 try:
#                     result = future.result()
#                     counts[result["status"]] += 1
#                 except Exception as exc:
#                     counts["error"] += 1
#                     log.error(
#                         json.dumps({
#                             "event": "file_error",
#                             "file": os.path.basename(file_path),
#                             "error": str(exc),
#                         })
#                     )

#         log.info(json.dumps({"event": "run_complete", **counts}))

#     scan_and_process("{{ params.video_dir }}")


# video_process()