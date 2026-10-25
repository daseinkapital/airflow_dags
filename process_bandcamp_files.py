from airflow.sdk import dag, task
from datetime import datetime
import mutagen
import zipfile
import os
import shutil

@dag(start_date=datetime(2026, 1, 1), schedule='@hourly')
def process_bandcamp_files():
    @task
    def unzip_files():
        input_dir = '/opt/airflow/data/desktop/external/staging/music/bandcamp'
        zips_to_process = [x for x in os.listdir(input_dir) if x.endswith('.zip')]

        for zip_filename in zips_to_process:
            zip_path = os.path.join(input_dir, zip_filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                extract_path = os.path.join(input_dir, zip_filename[:-4])
                if not os.path.exists(extract_path):
                    zip_ref.extractall(extract_path)
                    print(f'Extracted {zip_filename} to {extract_path}')
                else:
                    print(f'Skipping {zip_filename}, already extracted.')
    
    @task
    def list_extracted_folders():
        input_dir = '/opt/airflow/data/desktop/external/staging/music/bandcamp'
        return [f for f in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, f)) and ' - ' in f]

    @task
    def validate_folder(folder_name: str):
        input_dir = '/opt/airflow/data/desktop/external/staging/music/bandcamp'
        folder_path = os.path.join(input_dir, folder_name)
        artist = folder_name.split(' - ')[0]
        album = ' - '.join(folder_name.split(' - ')[1:])
        for root, _, files in os.walk(folder_path):
            for fname in files:
                if fname.lower().endswith(('.mp3', '.flac', '.wav', '.m4a')):
                    fp = os.path.join(root, fname)
                    try:
                        audio = mutagen.File(fp, easy=True)
                        if audio is None:
                            return None
                        title = audio.get('title', [None])[0]
                        a = audio.get('artist', [artist])[0]
                        al = audio.get('album', [album])[0]
                        if not title or not a or not al:
                            return None
                    except Exception:
                        return None
        return {"folder_name": folder_name, "artist": artist, "album": album}

    @task
    def filter_valid(validations: list):
        return [v for v in validations if v]

    @task
    def move_files(valid_info: dict):
        input_dir = '/opt/airflow/data/desktop/external/staging/music/bandcamp'
        output_dir = '/opt/airflow/data/desktop/external/music'
        folder_name = valid_info["folder_name"]
        artist = valid_info["artist"]
        album = valid_info["album"]
        src_root = os.path.join(input_dir, folder_name)
        dest_root = os.path.join(output_dir, artist, album)
        os.makedirs(dest_root, exist_ok=True)
        for root, _, files in os.walk(src_root):
            for fname in files:
                if fname.lower().endswith(('.mp3', '.flac', '.wav', '.m4a')):
                    shutil.move(os.path.join(root, fname), os.path.join(dest_root, fname))
        return {"folder_name": folder_name, "zip_path": os.path.join(input_dir, f"{folder_name}.zip"), "folder_path": src_root}

    @task
    def cleanup_originals(info: dict):
        zip_path = info.get("zip_path")
        folder_path = info.get("folder_path")
        try:
            if zip_path and os.path.exists(zip_path):
                os.remove(zip_path)
            if folder_path and os.path.exists(folder_path):
                shutil.rmtree(folder_path)
        except Exception as e:
            print(f"Cleanup error for {folder_path}: {e}")

    # Define the workflow
    unzipped = unzip_files()
    folders = list_extracted_folders()
    validated = validate_folder.expand(folder_name=folders)
    valids = filter_valid(validated)
    moved = move_files.expand(valid_info=valids)
    cleaned = cleanup_originals.expand(info=moved)
    
    # Set dependencies
    unzipped >> folders >> validated >> valids >> moved >> cleaned
   
process_bandcamp_files()