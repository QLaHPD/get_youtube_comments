import argparse
import os
import subprocess
from threading import Thread
from tqdm import tqdm
import pathlib
from urllib.parse import urlparse, parse_qs
import re
from datetime import datetime
import json

# --------------------------------------------------------------------------
# Helper to normalize any YouTube URL (or bare ID) to the canonical 11-char ID
YT_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")

def extract_video_id(url_or_id: str) -> str:
    """
    Return the canonical 11-char video ID from any YouTube URL or from a bare ID.
    Falls back to the original string if it cannot detect an ID.
    """
    s = url_or_id.strip()

    # 1) Already an ID?
    if YT_ID_RE.fullmatch(s):
        return s

    # 2) Parse URL variants
    if s.startswith(("http://", "https://")):
        parsed = urlparse(s)
        host = parsed.netloc.lower()
        path = parsed.path

        # youtu.be/<id>
        if host.endswith("googleusercontent.com"):
            if path.startswith("/youtube.com/v/"):
                candidate = path.split("/")[3]
                if YT_ID_RE.fullmatch(candidate):
                    return candidate

        if 'youtube.com' in host:
            if path == '/watch':
                qs = parse_qs(parsed.query)
                if 'v' in qs and YT_ID_RE.fullmatch(qs['v'][0]):
                    return qs['v'][0]
            for prefix in ("/shorts/", "/embed/"):
                if path.startswith(prefix):
                    candidate = path[len(prefix):].split("/")[0]
                    if YT_ID_RE.fullmatch(candidate):
                        return candidate
        elif 'youtu.be' in host:
            candidate = path.lstrip('/')
            if YT_ID_RE.fullmatch(candidate):
                return candidate

    # 3) Give up
    return s
# --------------------------------------------------------------------------

def get_urls_to_process(channel_id, output_folder, cookies_file=None):
    """
    Fetches all video URLs from a given channel ID, skipping those already downloaded.
    """
    # Construct the channel URL for the 'videos' tab.
    source_url = f"https://www.youtube.com/channel/{channel_id}"
    
    print(f"Fetching video list from: {source_url}")
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--skip-download",      # don’t download video data
        "--print", "%(url)s",   # output one URL per line
    ]
    if cookies_file:
        cmd += ["--cookies", cookies_file]
    cmd.append(source_url)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True
        )
        all_urls = result.stdout.strip().splitlines()
        print(f"Found {len(all_urls)} videos in channel.")
    except subprocess.CalledProcessError as e:
        print(f"Error fetching list for channel {channel_id}: {e.stderr}")
        return []

    # Build the set of IDs already present on disk for this channel
    # This logic still works because the video ID is the first 11 characters.
    output_path = pathlib.Path(output_folder)
    downloaded_ids = {
        extract_video_id(f.name[:11])
        for f in output_path.iterdir()
        if f.suffix == ".json"
    }

    # Filter out any URLs whose ID is already downloaded
    urls_to_download = []
    for url in all_urls:
        vid = extract_video_id(url)
        if vid and vid not in downloaded_ids:
            urls_to_download.append(url)

    print(f"Found {len(urls_to_download)} new videos to process for channel {channel_id}.")
    return urls_to_download

def download_comments(url, output_path, pbar, cookies_file=None):
    """
    Fetches YouTube comments and saves them to a JSON file.
    The filename will be in the format <videoid>_<upload_date>.json.
    If the comments data is trivial (less than 3 bytes), the file is discarded.
    """
    vid = extract_video_id(url)
    # Belt-and-suspenders: skip if there's already a dump file for this video ID.
    # The glob `f"{vid}_*.json"` will match any timestamp format.
    if any(output_path.glob(f"{vid}_*.json")):
        pbar.update(1)
        return

    # --- MODIFIED COMMAND ---
    # Use --dump-json to get all metadata, including comments, in one go.
    # --write-comments is still needed to trigger the comment fetching logic.
    cmd = [
        "yt-dlp",
        "--skip-download",
        "--write-comments",
        "--dump-json",
        "--user-agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0",
        url,
    ]
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, encoding='utf-8')
        
        # --- NEW LOGIC TO PROCESS THE FULL JSON METADATA ---
        video_info = json.loads(result.stdout)
        
        comments_data = video_info.get('comments')
        upload_date = video_info.get('upload_date') # Format: YYYYMMDD
        video_id = video_info.get('id')

        # Proceed only if we have comments data and the required metadata
        if comments_data and upload_date and video_id:
            # Convert just the comments part to a JSON string to check its size
            comments_json_str = json.dumps(comments_data, indent=4) # Using indent for readability
            
            # Check if the captured data is more than 3 bytes.
            if len(comments_json_str.encode('utf-8')) > 3:
                # Construct the filename using yt-dlp's naming pattern
                filename = f"{video_id}_{upload_date}.json"
                filepath = output_path / filename

                # Write the comments to the final destination
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(comments_json_str)
            else:
                tqdm.write(f"Skipping {url}: No comments found or comments are trivial.")
        else:
             tqdm.write(f"Skipping {url}: No comments found in metadata.")


    except subprocess.CalledProcessError as e:
        err = e.stderr
        # This error handling remains effective
        if "comments" in err.lower():
            tqdm.write(f"Skipping {url}: comments disabled or unavailable.")
        else:
            tqdm.write(f"yt-dlp error on {url}: {err}")
    finally:
        pbar.update(1)

def process_urls_threaded(urls, output_path, num_threads, cookies_file=None):
    """Manages the multithreaded processing of the video URLs."""
    if not urls:
        return

    # This progress bar is for videos within a single channel
    with tqdm(total=len(urls), desc="Downloading Comments", leave=False) as pbar:
        threads = []
        
        for url in urls:
            # Normalizing the URL isn't strictly necessary with the new method but is good practice.
            video_id = extract_video_id(url)
            normalized_url = f"https://www.youtube.com/watch?v={video_id}"

            thread = Thread(
                target=download_comments,
                args=(normalized_url, pathlib.Path(output_path), pbar, cookies_file)
            )
            threads.append(thread)
            thread.start()
            
            # Simple thread management to avoid creating too many at once
            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []
                
        # Wait for any remaining threads to finish
        for t in threads:
            t.join()

# ---------------- NEW: utilities for channel IDs from file ----------------
def read_channel_ids_from_file(path: str):
    """
    Read channel IDs from a text file, one per line.
    Empty lines and lines starting with '#' are ignored.
    """
    chan_ids = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            chan_ids.append(s)
    return chan_ids

def dedupe_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out
# --------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Download video comments from one or more YouTube channels using yt-dlp.\n'
            'You can pass channels via --channel_ids and/or --channels_file (one ID per line).'
        )
    )
    parser.add_argument(
        '--channel_ids',
        nargs='+',
        help='The ID(s) of the YouTube channel(s).'
    )
    parser.add_argument(
        '--channels_file', type=str,
        help='Path to a text file containing one channel ID per line (lines starting with # are ignored).'
    )
    parser.add_argument(
        '--num_threads', type=int, default=4,
        help='The number of threads to use for parallel processing.'
    )
    parser.add_argument(
        '--output', type=str, required=True,
        help='The path to the root output folder.'
    )
    parser.add_argument(
        '--cookies', type=str,
        help='Path to a cookies file (e.g., cookies.txt).'
    )

    args = parser.parse_args()

    # Gather channel IDs from CLI and/or file, then dedupe while preserving order
    combined_ids = []
    if args.channel_ids:
        combined_ids.extend([s.strip() for s in args.channel_ids if s and s.strip()])
    if args.channels_file:
        try:
            file_ids = read_channel_ids_from_file(args.channels_file)
            if file_ids:
                print(f"Loaded {len(file_ids)} channel IDs from {args.channels_file}.")
            combined_ids.extend(file_ids)
        except Exception as e:
            raise SystemExit(f"Failed to read --channels_file '{args.channels_file}': {e}")

    # If nothing provided, show an error
    combined_ids = dedupe_preserve_order(combined_ids)
    if not combined_ids:
        parser.error('You must provide --channel_ids and/or --channels_file with at least one channel ID.')

    root_output_path = pathlib.Path(args.output)
    channel_ids = combined_ids
    
    # Set up the outer progress bar for channels if there are multiple
    channel_iterator = (
        tqdm(channel_ids, desc="Processing Channels")
        if len(channel_ids) > 1
        else channel_ids
    )

    for channel_id in channel_iterator:
        # Create a dedicated folder for the current channel
        channel_output_path = root_output_path / channel_id
        channel_output_path.mkdir(parents=True, exist_ok=True)
        tqdm.write(f"\nProcessing channel: {channel_id}")
        
        urls_to_process = get_urls_to_process(
            channel_id, channel_output_path, args.cookies
        )
        
        if urls_to_process:
            process_urls_threaded(
                urls_to_process, channel_output_path, args.num_threads, args.cookies
            )
        else:
            tqdm.write(f"No new videos to process for {channel_id}.")
            
    print("\nComment download process finished.")
