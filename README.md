# get_youtube_comments

> A lightweight, multi-threaded script that downloads (and continues) YouTube video comments for one or more channels using **yt-dlp** as the backend.

This repository provides a small command-line tool that:

* Lists videos for given channel IDs (in the order provided),
* Spawns multiple workers to download comments in parallel,
* Supports continuing partial downloads (useful for long-running collection jobs),
* Accepts a cookies file when needed to access age-gated or region-restricted content.

---

## Features

* Parallel comment collection (configurable worker count).
* Works with multiple YouTube channels in a single run.
* Uses `yt-dlp` for robust video listing and comment extraction.
* Simple filesystem-based output (one folder per channel/video).
* Optional cookies support for authenticated or region-restricted access.
* **NEW:** Accepts channel IDs directly from a file (one ID per line).

---

## Prerequisites

* Python 3.8+
* `yt-dlp` in your `PATH` (install with pip or follow official instructions):

```bash
pip install yt-dlp
```

* `tqdm` for progress bars:

```bash
pip install tqdm
```

---

## Installation

Clone this repo (or copy the script) and install the Python dependency:

```bash
git clone <repo-url>
cd get_youtube_comments
pip install -r requirements.txt   # if you prefer a requirements file
# or
pip install yt-dlp tqdm
```

If you plan to access content behind login/age gate, export cookies using your browser (`cookies.txt`) and pass it with `--cookies`.

---

## Usage

### From command-line arguments
```bash
python get_comments.py --channel_ids <CHANNEL_ID_1> <CHANNEL_ID_2> ... --output <path/to/output> [--num_threads N] [--cookies cookies.txt]
```

### From a file of channel IDs
Create a text file with one channel ID per line (ignore empty lines and `#` comments). Example `channels.txt`:
```
UC_x5XG1OV2P6uZZ5FSM9Ttw
# This is a comment line
UCYO_jab_esuFRV4b17AJtAw
```

Run with:
```bash
python get_comments.py --channels_file channels.txt --output <path/to/output>
```

### Mix both sources
You can provide both inline IDs and a file. Duplicates are automatically removed:
```bash
python get_comments.py --channels_file channels.txt --channel_ids UC123... UC456... --output <path/to/output>
```

---

## Command-line options

| Option            | Required | Description                                                            |
| ----------------- | -------: | ---------------------------------------------------------------------- |
| `--channel_ids`   |       No | One or more YouTube channel IDs to collect from.                       |
| `--channels_file` |       No | Path to a text file containing one channel ID per line.                |
| `--output`        |      Yes | Root output folder where results will be written.                      |
| `--num_threads`   |       No | Number of worker threads (default: `4`).                               |
| `--cookies`       |       No | Path to a cookies file (e.g., `cookies.txt`) for authenticated access. |
| `-h, --help`      |       No | Show usage and exit.                                                   |

---

## How it works
1. For each channel ID from CLI or file, the script asks `yt-dlp` to list the channel's videos.
2. The videos are queued (preserving channel order).
3. The script spawns `N` worker threads (`--num_threads`) that pull the next video from the queue and run `yt-dlp --write-comments` to fetch comments.
4. Each worker writes a file (or files) under the provided `--output` directory. Partially downloaded channels are preserved so the process can be continued later without re-downloading everything.

---

## Contributing

Contributions, bug reports and improvements are welcome.
