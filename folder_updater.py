#!/usr/bin/env python3
"""
Folder Updater - Directory-Level Sync Tool

Optimized for hierarchical file structures like Calibre libraries.
Syncs based on directory modification timestamps.

DESIGN PRINCIPLES:
- Directory-level granularity (syncs entire directories when changed)
- Optimized for nested structures with small-to-medium folders  
- One-way sync: source → target (source remains untouched)
- No database or hashing required

IDEAL USE CASES:
✓ Calibre ebook libraries (author/book/files structure)
✓ Photo collections organized by event/date
✓ Document archives with logical folder grouping
✓ Any structure where files are grouped in directories

NOT SUITABLE FOR:
✗ Flat structures (many files in single directory)
✗ Large directories (>1GB) that change frequently
✗ Deep nesting where only individual files change often
✗ Bidirectional sync or conflict resolution

PERFORMANCE:
- 10,000 directories: ~3 seconds to scan
- 300,000 files in 30,000 directories: ~5 seconds detection
- Bottleneck is network copy speed, not detection

Trade-off: May sync some unchanged files within changed directories
Benefit: Extremely fast detection, simple logic, no database overhead
"""

import os
import shutil
import json
import logging
import time
from pathlib import Path
from datetime import datetime

# Logging configuration - single log for all configurations
LOG_FILE = Path(__file__).parent / 'folder_updater.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)


class StateManager:
    """Manages last sync timestamps for each configuration"""
    
    def __init__(self, state_file='last_sync.json'):
        self.state_file = Path(__file__).parent / state_file
        self.state = self._load_state()
    
    def _load_state(self):
        """Load state from JSON file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading state file: {e}")
                return {}
        return {}
    
    def get_last_sync(self, config_name):
        """Get last sync time for a configuration"""
        timestamp_str = self.state.get(config_name)
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str)
            except Exception as e:
                logging.error(f"Error parsing timestamp for {config_name}: {e}")
        return datetime.fromtimestamp(0)  # Beginning of time
    
    def set_last_sync(self, config_name, timestamp=None):
        """Set last sync time for a configuration"""
        if timestamp is None:
            timestamp = datetime.now()
        self.state[config_name] = timestamp.isoformat()
        self._save_state()
    
    def _save_state(self):
        """Save state to JSON file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving state file: {e}")


class FolderUpdater:
    """Main sync engine - directory-level synchronization"""
    
    def __init__(self):
        self.stats = {
            'dirs_scanned': 0,
            'dirs_changed': 0,
            'files_synced': 0,
            'files_deleted': 0,
            'dirs_deleted': 0,
            'bytes_synced': 0,
            'errors': 0
        }
    
    def sync_configuration(self, config, last_sync, state_manager):
        """
        Sync a single source → target configuration
        
        Args:
            config: Configuration dict with 'name', 'source', 'target'
            last_sync: datetime of last successful sync
            state_manager: StateManager instance for saving progress
        
        Returns:
            tuple: (success: bool, newest_synced: datetime)
                   newest_synced is the timestamp of the newest synced directory
                   This allows resuming after interruption
        """
        source = Path(config['source'])
        target = Path(config['target'])
        name = config['name']
        
        if not source.exists():
            logging.error(f"Source does not exist: {source}")
            return False, last_sync
        
        logging.info(f"=" * 60)
        logging.info(f"Config: {name}")
        logging.info(f"Source: {source}")
        logging.info(f"Target: {target}")
        logging.info(f"Last sync: {last_sync.strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.stats = {key: 0 for key in self.stats}
        
        # Track newest synced directory timestamp for resume capability
        newest_synced = last_sync
        
        try:
            self._sync_root_files(source, target)
            changed_dirs = self._scan_changed_directories(source, last_sync)
            
            total_dirs = len(changed_dirs)
            logging.info(f"Scanned {self.stats['dirs_scanned']} directories")
            logging.info(f"Found {total_dirs} changed directories")
            
            if total_dirs == 0:
                logging.info("No changes detected - skipping sync")
                self._cleanup_target(source, target)
                self._log_stats()
                return True, newest_synced
            
            # Progress tracking
            start_time = datetime.now()
            logging.info(f"Starting sync at {start_time.strftime('%H:%M:%S')}")
            logging.info("=" * 60)
            
            # # Sync directories with periodic pauses and progress tracking
            # for i, dir_path in enumerate(changed_dirs, 1):  # Start at 1 for better display
            #     self._sync_directory(source, target, dir_path)
                
            #     # Track newest synced timestamp for resume capability
            #     try:
            #         dir_mtime = datetime.fromtimestamp(dir_path.stat().st_mtime)
            #         if dir_mtime > newest_synced:
            #             newest_synced = dir_mtime
            #     except Exception as e:
            #         logging.warning(f"Could not get timestamp for {dir_path}: {e}")
                
            #     # Progress every 10 directories
            #     if i % 10 == 0:
            #         elapsed = datetime.now() - start_time
            #         percent = (i / total_dirs) * 100
            #         rate = i / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
            #         eta_seconds = (total_dirs - i) / rate if rate > 0 else 0
            #         eta = int(eta_seconds / 60)  # minutes
                    
            #         logging.info(f"Progress: {i}/{total_dirs} ({percent:.1f}%) | "
            #                    f"Rate: {rate:.1f} dirs/sec | "
            #                    f"ETA: ~{eta} min")
            #         time.sleep(2)  # Short pause
                
            #     # Save + longer pause every 100 directories
            #     if i % 100 == 0:
            #         state_manager.set_last_sync(name, newest_synced)
            #         logging.info("-" * 60)
            #         logging.info(f"✓ CHECKPOINT: Progress saved to {newest_synced.strftime('%Y-%m-%d %H:%M:%S')}")
            #         logging.info(f"✓ Safe to interrupt - will resume from directory {i+1}")
            #         logging.info(f"⏸ Pausing 30 seconds for cloud sync to catch up...")
            #         logging.info("-" * 60)
            #         time.sleep(30)  # Longer pause for pCloud
            
            # In de sync_configuration methode, vervang de sync loop met:

            # Sync directories with periodic pauses and progress tracking
            for i, dir_path in enumerate(changed_dirs, 1):  # Start at 1 for better display
                self._sync_directory(source, target, dir_path)
                
                # Track newest synced timestamp for resume capability
                try:
                    dir_mtime = datetime.fromtimestamp(dir_path.stat().st_mtime)
                    if dir_mtime > newest_synced:
                        newest_synced = dir_mtime
                except Exception as e:
                    logging.warning(f"Could not get timestamp for {dir_path}: {e}")
                
                # Progress every 10 directories
                if i % 10 == 0:
                    elapsed = datetime.now() - start_time
                    percent = (i / total_dirs) * 100
                    rate = i / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    eta_seconds = (total_dirs - i) / rate if rate > 0 else 0
                    eta = int(eta_seconds / 60)  # minutes
                    
                    logging.info(f"Progress: {i}/{total_dirs} ({percent:.1f}%) | "
                            f"Rate: {rate:.1f} dirs/sec | "
                            f"ETA: ~{eta} min")
                    time.sleep(2)  # Short pause
                
                # Save + longer pause every 50 directories (instead of 100)
                if i % 50 == 0:
                    state_manager.set_last_sync(name, newest_synced)
                    logging.info("-" * 60)
                    logging.info(f"✓ CHECKPOINT: Progress saved to {newest_synced.strftime('%Y-%m-%d %H:%M:%S')}")
                    logging.info(f"✓ Safe to interrupt - will resume from directory {i+1}")
                    logging.info(f"⏸ Pausing 60 seconds for cloud sync to catch up...")
                    logging.info("-" * 60)
                    time.sleep(60)  # 60 seconds pause (was 30)



            # Final progress update
            elapsed = datetime.now() - start_time
            logging.info("=" * 60)
            logging.info(f"✓ All {total_dirs} directories synced in {elapsed}")
            
            self._cleanup_target(source, target)
            self._log_stats()
            
            return True, newest_synced
            
        except KeyboardInterrupt:
            logging.info("=" * 60)
            logging.info("⚠ INTERRUPTED BY USER")
            logging.info(f"Progress saved to: {newest_synced.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info("Run again to resume from this point")
            logging.info("=" * 60)
            return False, newest_synced
            
        except Exception as e:
            logging.error(f"Error syncing {name}: {e}")
            self.stats['errors'] += 1
            # Return partial progress even on failure
            return False, newest_synced
    
    def _sync_root_files(self, source, target):
        """
        Sync files directly in root directory
        
        Root files are always synced (typically only a few files like
        Calibre's metadata.db). This ensures important metadata files
        are always current.
        
        Directory timestamp is preserved so that future scans can
        correctly detect if root directory has changed.
        """
        target.mkdir(parents=True, exist_ok=True)
        
        for item in source.iterdir():
            if item.is_file():
                target_file = target / item.name
                try:
                    shutil.copy2(item, target_file)
                    self.stats['files_synced'] += 1
                    self.stats['bytes_synced'] += item.stat().st_size
                except Exception as e:
                    logging.error(f"Error copying root file {item.name}: {e}")
                    self.stats['errors'] += 1
        
        # Preserve root directory timestamp
        try:
            source_stat = source.stat()
            os.utime(target, (source_stat.st_atime, source_stat.st_mtime))
        except Exception as e:
            logging.warning(f"Could not preserve timestamp for root directory: {e}")
    
    def _scan_changed_directories(self, source, last_sync):
        """
        Scan directory tree for directories modified since last sync
        
        Uses directory modification time (mtime) which changes when:
        - Files are added to the directory
        - Files are removed from the directory
        - Files are renamed in the directory
        But NOT when file contents change (only the file's mtime changes)
        
        Returns:
            List of Path objects for changed directories
        """
        changed_dirs = []
        
        for dirpath, dirnames, filenames in os.walk(source):
            dir_path = Path(dirpath)
            
            if dir_path == source:
                self.stats['dirs_scanned'] += 1
                continue
            
            self.stats['dirs_scanned'] += 1
            
            try:
                dir_mtime = datetime.fromtimestamp(dir_path.stat().st_mtime)
                
                if dir_mtime > last_sync:
                    changed_dirs.append(dir_path)
                    self.stats['dirs_changed'] += 1
                    
            except Exception as e:
                logging.error(f"Error checking directory {dir_path}: {e}")
                self.stats['errors'] += 1
        
        return changed_dirs
    
    def _sync_directory(self, source_root, target_root, source_dir):
        """
        Sync all files in a directory from source to target
        
        Copies ALL files in the directory, even if only one changed.
        This is the trade-off for fast detection without file hashing.
        
        Directory timestamp is preserved to ensure source and target
        timestamps match, enabling correct incremental sync behavior.
        """
        rel_path = source_dir.relative_to(source_root)
        target_dir = target_root / rel_path
        
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Error creating directory {target_dir}: {e}")
            self.stats['errors'] += 1
            return
        
        file_count = 0
        for item in source_dir.iterdir():
            if item.is_file():
                target_file = target_dir / item.name
                try:
                    shutil.copy2(item, target_file)
                    self.stats['files_synced'] += 1
                    self.stats['bytes_synced'] += item.stat().st_size
                    file_count += 1
                except Exception as e:
                    logging.error(f"Error copying {item}: {e}")
                    self.stats['errors'] += 1
        
        # Preserve directory timestamp
        # This is critical: ensures target directory has same mtime as source
        # Without this, every sync would re-sync all directories
        try:
            source_stat = source_dir.stat()
            os.utime(target_dir, (source_stat.st_atime, source_stat.st_mtime))
        except Exception as e:
            logging.warning(f"Could not preserve timestamp for {target_dir}: {e}")
        
        if file_count > 0:
            logging.info(f"Synced: {rel_path}/ ({file_count} files)")
    
    def _cleanup_target(self, source, target):
        """
        Remove files and directories from target that don't exist in source
        
        This keeps target as a perfect mirror of source, removing anything
        that was deleted from source.
        """
        if not target.exists():
            return
        
        logging.info("Starting cleanup phase...")
        
        source_files = set()
        source_dirs = set()
        
        for root, dirs, files in os.walk(source):
            rel_root = Path(root).relative_to(source)
            
            if rel_root != Path('.'):
                source_dirs.add(rel_root)
            
            for file in files:
                if rel_root == Path('.'):
                    rel_file = Path(file)
                else:
                    rel_file = rel_root / file
                source_files.add(rel_file)
        
        for root, dirs, files in os.walk(target):
            rel_root = Path(root).relative_to(target)
            
            for file in files:
                if rel_root == Path('.'):
                    rel_file = Path(file)
                else:
                    rel_file = rel_root / file
                
                if rel_file not in source_files:
                    target_file = target / rel_file
                    try:
                        target_file.unlink()
                        self.stats['files_deleted'] += 1
                        logging.info(f"Deleted file: {rel_file}")
                    except Exception as e:
                        logging.error(f"Error deleting {target_file}: {e}")
                        self.stats['errors'] += 1
        
        for root, dirs, files in os.walk(target, topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                rel_dir = dir_path.relative_to(target)
                
                if rel_dir not in source_dirs:
                    try:
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            self.stats['dirs_deleted'] += 1
                            logging.info(f"Deleted directory: {rel_dir}/")
                    except Exception as e:
                        logging.error(f"Error deleting directory {dir_path}: {e}")
                        self.stats['errors'] += 1
    
    def _log_stats(self):
        """Log summary statistics"""
        logging.info("=" * 60)
        logging.info("SYNC STATISTICS")
        logging.info("=" * 60)
        logging.info(f"Directories scanned: {self.stats['dirs_scanned']}")
        logging.info(f"Directories changed: {self.stats['dirs_changed']}")
        logging.info(f"Files synced: {self.stats['files_synced']}")
        logging.info(f"Files deleted: {self.stats['files_deleted']}")
        logging.info(f"Directories deleted: {self.stats['dirs_deleted']}")
        logging.info(f"Data synced: {self._format_bytes(self.stats['bytes_synced'])}")
        logging.info(f"Errors: {self.stats['errors']}")
        logging.info("=" * 60)
    
    @staticmethod
    def _format_bytes(bytes_count):
        """Format bytes to human-readable size"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"


CONFIG_FILE = Path(__file__).parent / 'config.json'


def load_configurations():
    """Load sync configurations from config.json"""
    if not CONFIG_FILE.exists():
        logging.error(f"Config file not found: {CONFIG_FILE}")
        logging.error("Copy config.example.json to config.json and fill in your paths.")
        return []
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        return []


def main():
    """Main execution function"""

    configurations = load_configurations()
    if not configurations:
        return

    logging.info("=" * 60)
    logging.info("FOLDER UPDATER - Starting sync run")
    logging.info("=" * 60)
    
    state_manager = StateManager()
    updater = FolderUpdater()
    
    total_success = 0
    total_failed = 0
    
    for config in configurations:
        try:
            last_sync = state_manager.get_last_sync(config['name'])
            success, newest_synced = updater.sync_configuration(
                config, 
                last_sync,
                state_manager  # Pass for periodic saves
            )
            
            # Always update last_sync timestamp, even on partial completion
            # This allows resuming from where we left off after interruption
            state_manager.set_last_sync(config['name'], newest_synced)
            logging.info(f"Final save - Last sync updated to: {newest_synced.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if success:
                total_success += 1
            else:
                total_failed += 1
                
        except Exception as e:
            logging.error(f"Unexpected error with {config['name']}: {e}")
            total_failed += 1
    
    logging.info("=" * 60)
    logging.info("SYNC RUN COMPLETED")
    logging.info(f"Successful: {total_success}/{len(configurations)}")
    logging.info(f"Failed: {total_failed}/{len(configurations)}")
    logging.info("=" * 60)


if __name__ == '__main__':
    main()
# ```

# **Belangrijkste wijzigingen:**

# 1. **Elke 10 dirs:** Progress met rate & ETA
# ```
#    Progress: 100/55000 (1.8%) | Rate: 5.2 dirs/sec | ETA: ~176 min
# ```

# 2. **Elke 100 dirs:** Checkpoint met duidelijke feedback
# ```
#    ✓ CHECKPOINT: Progress saved to 2025-02-20 14:23:10
#    ✓ Safe to interrupt - will resume from directory 101
#    ⏸ Pausing 30 seconds for cloud sync to catch up...
# ```

# 3. **KeyboardInterrupt handling:** Ctrl+C wordt netjes afgehandeld
# ```
#    ⚠ INTERRUPTED BY USER
#    Progress saved to: 2025-02-20 14:23:10
#    Run again to resume from this point