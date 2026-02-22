# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Running the Tool

```bash
# Activate the virtual environment first
source .venv/Scripts/activate  # Windows/Git Bash

# Run the sync
python folder_updater.py
```

No build step or package installation is required — the tool uses only Python standard library modules (`os`, `shutil`, `json`, `logging`, `pathlib`, `datetime`).

## Architecture

The entire tool lives in a single file: `folder_updater.py`.

**Two classes + one `main()` function:**

- `StateManager` — persists last-sync timestamps per configuration to `last_sync.json` (next to the script). Timestamps are stored as ISO-format strings keyed by config name.
- `FolderUpdater` — performs the actual sync. Its `sync_configuration()` method orchestrates four phases:
  1. `_sync_root_files()` — always copies files directly in the source root
  2. `_scan_changed_directories()` — walks the source tree and collects subdirectories whose `mtime > last_sync`
  3. `_sync_directory()` — copies all files from each changed directory to the mirrored target path
  4. `_cleanup_target()` — removes files/empty dirs from target that no longer exist in source
- `main()` — defines the list of `configurations` (hardcoded `name`/`source`/`target` dicts), iterates them, and updates `StateManager` only on success.

**Key design constraint:** Sync granularity is at the *directory* level, not the file level. If any file in a directory changes (causing the directory's `mtime` to update), all files in that directory are re-copied. This avoids per-file hashing but may redundantly copy unchanged files.

**State files written at runtime (gitignored via `versies/`):**
- `last_sync.json` — sync timestamps
- `folder_updater.log` — append-only log

## Configuration

Sync targets are defined in `config.json` (gitignored) next to the script. Use `config.example.json` as a template. The file is a JSON array of objects with `name`, `source`, and `target` keys. `load_configurations()` in `main()` reads this file at startup.
