"""Module for generating trace.json files for checkpoint comparisons.

This module provides functionality to track file changes between checkpoints,
including content hashes and line-level diffs.
"""

import difflib
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from checkpoint.constants import TEXT_EXTENSIONS, TRACE_FILENAME
from checkpoint.utils import get_reader_by_extension


def compute_file_hash(content: bytes) -> str:
    """Compute SHA-256 hash of file content.

    Parameters
    ----------
    content: bytes
        The file content as bytes.

    Returns
    -------
    str
        The hexadecimal SHA-256 hash string.
    """
    return hashlib.sha256(content).hexdigest()


def compute_line_diff(old_lines: List[str], new_lines: List[str]) -> List[Dict[str, Any]]:
    """Compute line-level changes between two file versions.

    Uses difflib.SequenceMatcher to identify added, deleted, and modified lines.

    Parameters
    ----------
    old_lines: List[str]
        Lines from the old version of the file.
    new_lines: List[str]
        Lines from the new version of the file.

    Returns
    -------
    List[Dict[str, Any]]
        List of change ranges with start/end lines and change type.
        Each dict contains:
        - start_line: 1-indexed start line in new file
        - end_line: 1-indexed end line in new file
        - change_type: 'added', 'deleted', or 'modified'
        - old_range: [start, end] for old file (if applicable)
    """
    changes = []
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            continue

        # Map difflib tags to our change types
        if tag == 'insert':
            change_type = 'added'
        elif tag == 'delete':
            change_type = 'deleted'
        elif tag == 'replace':
            change_type = 'modified'
        else:
            continue

        change = {
            'start_line': j1 + 1,  # Convert to 1-indexed
            'end_line': j2,  # Already correct for 1-indexed end
            'change_type': change_type,
        }

        # Include old range for modified and deleted lines
        if tag in ('replace', 'delete'):
            change['old_range'] = [i1 + 1, i2]

        changes.append(change)

    return changes


def compute_line_stats(old_lines: List[str], new_lines: List[str]) -> Dict[str, int]:
    """Compute line-level statistics for a file change.

    Parameters
    ----------
    old_lines: List[str]
        Lines from the old version of the file.
    new_lines: List[str]
        Lines from the new version of the file.

    Returns
    -------
    Dict[str, int]
        Statistics containing lines_added, lines_deleted, lines_modified.
    """
    stats = {
        'lines_added': 0,
        'lines_deleted': 0,
        'lines_modified': 0,
    }

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            continue
        elif tag == 'insert':
            stats['lines_added'] += j2 - j1
        elif tag == 'delete':
            stats['lines_deleted'] += i2 - i1
        elif tag == 'replace':
            # For replacements, count as both additions and deletions
            # The smaller count is considered "modified", rest are pure add/delete
            old_count = i2 - i1
            new_count = j2 - j1
            min_count = min(old_count, new_count)
            stats['lines_modified'] += min_count
            if new_count > old_count:
                stats['lines_added'] += new_count - old_count
            elif old_count > new_count:
                stats['lines_deleted'] += old_count - new_count

    return stats


def is_text_file(file_path: str) -> bool:
    """Check if a file is a text file based on its extension.

    Parameters
    ----------
    file_path: str
        Path to the file.

    Returns
    -------
    bool
        True if the file is considered a text file.
    """
    extension = file_path.split('.')[-1].lower() if '.' in file_path else ''
    return extension in TEXT_EXTENSIONS


def is_binary_content(content: bytes) -> bool:
    """Check if content appears to be binary (not text).

    Parameters
    ----------
    content: bytes
        The file content.

    Returns
    -------
    bool
        True if the content appears to be binary (not decodable as text).
    """
    # Try common text encodings
    encodings_to_try = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'latin-1']
    
    for encoding in encodings_to_try:
        try:
            content.decode(encoding)
            return False  # Successfully decoded, not binary
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return True  # Could not decode with any encoding, likely binary


def decode_text_content(content: bytes) -> Optional[Tuple[str, str]]:
    """Try to decode text content using various encodings.

    Parameters
    ----------
    content: bytes
        The file content.

    Returns
    -------
    Optional[Tuple[str, str]]
        Tuple of (decoded_text, encoding_used) if successful, None otherwise.
    """
    # Try common text encodings in order of preference
    encodings_to_try = ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'latin-1']
    
    for encoding in encodings_to_try:
        try:
            decoded = content.decode(encoding)
            return (decoded, encoding)
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return None


def generate_trace(
    checkpoint_name: str,
    checkpoint_type: str,
    current_files: Dict[str, bytes],
    previous_files: Optional[Dict[str, bytes]] = None,
    previous_checkpoint_name: Optional[str] = None,
    subtype: Optional[str] = None
) -> Dict[str, Any]:
    """Generate the complete trace.json structure.

    Parameters
    ----------
    checkpoint_name: str
        Name of the current checkpoint.
    checkpoint_type: str
        Type of checkpoint ('human', 'ai', or 'codebase').
    current_files: Dict[str, bytes]
        Dictionary mapping file paths to their content in the current checkpoint.
    previous_files: Optional[Dict[str, bytes]]
        Dictionary mapping file paths to their content in the previous checkpoint.
    previous_checkpoint_name: Optional[str]
        Name of the previous checkpoint.
    subtype: Optional[str]
        Optional subtype for the checkpoint.

    Returns
    -------
    Dict[str, Any]
        The complete trace data structure.
    """
    trace = {
        'checkpoint_name': checkpoint_name,
        'checkpoint_type': checkpoint_type,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'previous_checkpoint': previous_checkpoint_name,
        'files': {},
        'summary': {
            'total_files_changed': 0,
            'total_lines_added': 0,
            'total_lines_deleted': 0,
            'total_lines_modified': 0,
            'new_files': 0,
            'deleted_files': 0,
        }
    }

    # Add subtype to trace if provided
    if subtype is not None:
        trace['checkpoint_subtype'] = subtype

    # Track all files we've processed
    processed_files = set()

    # Process current files
    for file_path, content in current_files.items():
        processed_files.add(file_path)
        file_info = _process_file(file_path, content, previous_files)
        trace['files'][file_path] = file_info

        # Update summary
        if file_info['status'] != 'unchanged':
            trace['summary']['total_files_changed'] += 1
            if file_info['status'] == 'added':
                trace['summary']['new_files'] += 1

        if 'stats' in file_info:
            trace['summary']['total_lines_added'] += file_info['stats'].get('lines_added', 0)
            trace['summary']['total_lines_deleted'] += file_info['stats'].get('lines_deleted', 0)
            trace['summary']['total_lines_modified'] += file_info['stats'].get('lines_modified', 0)

    # Check for deleted files (in previous but not in current)
    if previous_files:
        for file_path in previous_files:
            if file_path not in processed_files:
                file_info = _process_deleted_file(file_path, previous_files[file_path])
                trace['files'][file_path] = file_info
                trace['summary']['total_files_changed'] += 1
                trace['summary']['deleted_files'] += 1

    return trace


def _process_file(
    file_path: str,
    content: bytes,
    previous_files: Optional[Dict[str, bytes]] = None
) -> Dict[str, Any]:
    """Process a single file and generate its trace info.

    Parameters
    ----------
    file_path: str
        Path to the file.
    content: bytes
        Current content of the file.
    previous_files: Optional[Dict[str, bytes]]
        Dictionary of previous checkpoint files.

    Returns
    -------
    Dict[str, Any]
        File trace information.
    """
    current_hash = compute_file_hash(content)
    is_binary = is_binary_content(content) or not is_text_file(file_path)

    # First checkpoint - all files are new
    if previous_files is None:
        file_info = {
            'status': 'added',
            'hash': current_hash,
            'line_changes': [],
            'stats': {'lines_added': 0, 'lines_deleted': 0, 'lines_modified': 0}
        }

        if not is_binary:
            decoded = decode_text_content(content)
            if decoded:
                lines = decoded[0].splitlines(keepends=True)
                file_info['stats']['lines_added'] = len(lines)
                file_info['line_changes'] = [{
                    'start_line': 1,
                    'end_line': len(lines),
                    'change_type': 'added'
                }]
            else:
                file_info['is_binary'] = True

        return file_info

    # Check if file existed in previous checkpoint
    if file_path not in previous_files:
        # New file
        file_info = {
            'status': 'added',
            'hash': current_hash,
            'line_changes': [],
            'stats': {'lines_added': 0, 'lines_deleted': 0, 'lines_modified': 0}
        }

        if not is_binary:
            decoded = decode_text_content(content)
            if decoded:
                lines = decoded[0].splitlines(keepends=True)
                file_info['stats']['lines_added'] = len(lines)
                file_info['line_changes'] = [{
                    'start_line': 1,
                    'end_line': len(lines),
                    'change_type': 'added'
                }]
            else:
                file_info['is_binary'] = True

        return file_info

    # File exists in both - check for changes
    previous_content = previous_files[file_path]
    previous_hash = compute_file_hash(previous_content)

    if current_hash == previous_hash:
        # Unchanged file
        return {
            'status': 'unchanged',
            'hash': current_hash,
            'line_changes': [],
            'stats': {'lines_added': 0, 'lines_deleted': 0, 'lines_modified': 0}
        }

    # Modified file
    file_info = {
        'status': 'modified',
        'hash': current_hash,
        'line_changes': [],
        'stats': {'lines_added': 0, 'lines_deleted': 0, 'lines_modified': 0}
    }

    # Compute line diff for text files
    if not is_binary:
        decoded_old = decode_text_content(previous_content)
        decoded_new = decode_text_content(content)
        
        if decoded_old and decoded_new:
            old_lines = decoded_old[0].splitlines(keepends=True)
            new_lines = decoded_new[0].splitlines(keepends=True)

            file_info['line_changes'] = compute_line_diff(old_lines, new_lines)
            file_info['stats'] = compute_line_stats(old_lines, new_lines)
        else:
            file_info['is_binary'] = True

    return file_info


def _process_deleted_file(file_path: str, content: bytes) -> Dict[str, Any]:
    """Process a file that was deleted.

    Parameters
    ----------
    file_path: str
        Path to the deleted file.
    content: bytes
        Content of the file from the previous checkpoint.

    Returns
    -------
    Dict[str, Any]
        File trace information for deleted file.
    """
    file_hash = compute_file_hash(content)
    is_binary = is_binary_content(content) or not is_text_file(file_path)

    file_info = {
        'status': 'deleted',
        'hash': file_hash,
        'line_changes': [],
        'stats': {'lines_added': 0, 'lines_deleted': 0, 'lines_modified': 0}
    }

    if not is_binary:
        decoded = decode_text_content(content)
        if decoded:
            lines = decoded[0].splitlines(keepends=True)
            file_info['stats']['lines_deleted'] = len(lines)
            file_info['line_changes'] = [{
                'start_line': 1,
                'end_line': len(lines),
                'change_type': 'deleted',
                'old_range': [1, len(lines)]
            }]
        else:
            file_info['is_binary'] = True

    return file_info


def save_trace(trace_data: Dict[str, Any], checkpoint_dir: str) -> str:
    """Save trace.json to the checkpoint directory.

    Parameters
    ----------
    trace_data: Dict[str, Any]
        The trace data to save.
    checkpoint_dir: str
        Path to the checkpoint directory.

    Returns
    -------
    str
        Path to the saved trace.json file.
    """
    trace_path = os.path.join(checkpoint_dir, TRACE_FILENAME)
    with open(trace_path, 'w+', encoding='utf-8') as trace_file:
        json.dump(trace_data, trace_file, indent=4)
    return trace_path


class TraceGenerator:
    """Class to generate trace.json for checkpoint comparisons.

    This class provides a higher-level interface for trace generation,
    integrating with the checkpoint system's IO operations.
    """

    def __init__(
        self,
        checkpoint_name: str,
        checkpoint_type: str,
        source_dir: str,
        dest_dir: Optional[str] = None,
        subtype: Optional[str] = None
    ):
        """Initialize the TraceGenerator.

        Parameters
        ----------
        checkpoint_name: str
            Name of the current checkpoint.
        checkpoint_type: str
            Type of checkpoint ('human', 'ai', or 'codebase').
        source_dir: str
            Source directory of the project (for reference).
        dest_dir: str, optional
            Destination directory for checkpoint storage.
            Defaults to source_dir if not provided.
        subtype: str, optional
            Optional subtype for the checkpoint (saved to trace.json).
        """
        self.checkpoint_name = checkpoint_name
        self.checkpoint_type = checkpoint_type
        self.subtype = subtype
        self.source_dir = source_dir
        self.dest_dir = dest_dir or source_dir
        # Keep root_dir as an alias for source_dir for backward compatibility
        self.root_dir = self.source_dir

    def get_previous_checkpoint_name(self) -> Optional[str]:
        """Get the name of the previous checkpoint.

        Returns
        -------
        Optional[str]
            Name of the previous checkpoint, or None if this is the first.
        """
        # Read config from destination directory
        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')
        if not os.path.exists(config_path):
            return None

        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)

        checkpoints = config.get('checkpoints', [])
        if len(checkpoints) < 1:
            return None

        # Get the checkpoint before the current one
        try:
            current_index = checkpoints.index(self.checkpoint_name)
            if current_index > 0:
                return checkpoints[current_index - 1]
        except ValueError:
            # Current checkpoint not in list yet, get the last one
            if checkpoints:
                return checkpoints[-1]

        return None

    def generate_and_save(
        self,
        current_files: Dict[str, bytes],
        previous_files: Optional[Dict[str, bytes]] = None,
        previous_checkpoint_name: Optional[str] = None
    ) -> str:
        """Generate trace.json and save it to the checkpoint directory.

        Parameters
        ----------
        current_files: Dict[str, bytes]
            Dictionary mapping file paths to their content.
        previous_files: Optional[Dict[str, bytes]]
            Dictionary of previous checkpoint files.
        previous_checkpoint_name: Optional[str]
            Name of the previous checkpoint.

        Returns
        -------
        str
            Path to the saved trace.json file.
        """
        trace_data = generate_trace(
            checkpoint_name=self.checkpoint_name,
            checkpoint_type=self.checkpoint_type,
            current_files=current_files,
            previous_files=previous_files,
            previous_checkpoint_name=previous_checkpoint_name,
            subtype=self.subtype
        )

        # Save trace to destination directory
        checkpoint_dir = os.path.join(self.dest_dir, '.checkpoint', self.checkpoint_name)
        return save_trace(trace_data, checkpoint_dir)


def has_changes(
    source_dir: str,
    dest_dir: str,
    ignore_dirs: List[str],
    current_files: Optional[Dict[str, bytes]] = None
) -> Tuple[bool, Optional[str]]:
    """Check if there are any changes compared to the latest checkpoint.

    This function performs a lightweight comparison between the current files
    and the latest checkpoint to determine if any changes exist.

    Parameters
    ----------
    source_dir: str
        Path to the source directory to check.
    dest_dir: str
        Path to the destination directory containing .checkpoint folder.
    ignore_dirs: List[str]
        List of directories to ignore during comparison.
    current_files: Optional[Dict[str, bytes]]
        Pre-computed current files dictionary. If None, files will be read
        from the source directory.

    Returns
    -------
    Tuple[bool, Optional[str]]
        A tuple containing:
        - bool: True if changes detected, False otherwise.
        - Optional[str]: Name of the previous checkpoint, or None if no previous checkpoint exists.
    """
    from checkpoint.crypt import Crypt
    from checkpoint.io import IO

    # Check if .checkpoint directory exists
    checkpoint_base = os.path.join(dest_dir, '.checkpoint')
    print(f"[DEBUG] Checking for checkpoint base: {checkpoint_base}")
    print(f"[DEBUG] Checkpoint base exists: {os.path.exists(checkpoint_base)}")
    
    if not os.path.exists(checkpoint_base):
        print(f"[DEBUG] No checkpoint base found - treating as first checkpoint")
        return True, None  # No checkpoint exists, treat as having changes (first checkpoint)

    # Check if config file exists
    config_path = os.path.join(checkpoint_base, '.config')
    print(f"[DEBUG] Config path: {config_path}")
    print(f"[DEBUG] Config exists: {os.path.exists(config_path)}")
    
    if not os.path.exists(config_path):
        print(f"[DEBUG] No config file found - treating as having changes")
        return True, None  # No config, treat as having changes

    with open(config_path, 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)

    checkpoints = config.get('checkpoints', [])
    print(f"[DEBUG] Checkpoints in config: {checkpoints}")
    
    if not checkpoints:
        print(f"[DEBUG] No checkpoints in config - treating as first checkpoint")
        return True, None  # No checkpoints, treat as having changes (first checkpoint)

    # Get the latest checkpoint
    latest_checkpoint = checkpoints[-1]
    latest_checkpoint_path = os.path.join(
        checkpoint_base, latest_checkpoint, f'{latest_checkpoint}.json'
    )
    print(f"[DEBUG] Latest checkpoint: {latest_checkpoint}")
    print(f"[DEBUG] Latest checkpoint path: {latest_checkpoint_path}")
    print(f"[DEBUG] Checkpoint file exists: {os.path.exists(latest_checkpoint_path)}")

    if not os.path.exists(latest_checkpoint_path):
        print(f"[DEBUG] Checkpoint file missing - treating as having changes")
        return True, None  # Checkpoint file missing, treat as having changes

    # Load the encryption key and decrypt previous checkpoint files
    try:
        crypt = Crypt(key='crypt.key', key_path=checkpoint_base)
        print(f"[DEBUG] Crypt initialized successfully")
    except Exception as e:
        print(f"[DEBUG] Crypt initialization failed: {type(e).__name__}: {e}")
        return True, None  # Can't decrypt, treat as having changes

    # Load previous checkpoint files
    previous_files = {}
    try:
        with open(latest_checkpoint_path, 'r', encoding='utf-8') as f:
            prev_enc_files = json.load(f)

        print(f"[DEBUG] Previous checkpoint contains {len(prev_enc_files)} encrypted files")

        for file_path, encrypted_content in prev_enc_files.items():
            try:
                content = crypt.decrypt(encrypted_content)
                previous_files[file_path] = content
            except Exception as e:
                print(f"[DEBUG] Failed to decrypt file {file_path}: {type(e).__name__}: {e}")
                return True, latest_checkpoint
        
        print(f"[DEBUG] Successfully decrypted {len(previous_files)} previous files")
        if previous_files:
            sample_paths = list(previous_files.keys())[:3]
            print(f"[DEBUG] Sample previous file paths: {sample_paths}")
    except Exception as e:
        print(f"[DEBUG] Failed to load previous checkpoint: {type(e).__name__}: {e}")
        return True, latest_checkpoint

    # Get current files if not provided
    if current_files is None:
        current_files = {}
        source_io = IO(path=source_dir, ignore_dirs=ignore_dirs)
        
        print(f"[DEBUG] Reading current files from: {source_dir}")
        print(f"[DEBUG] Ignore dirs: {ignore_dirs}")
        
        for root, file in source_io.walk_directory():
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'rb') as f:
                    current_files[file_path] = f.read()
            except Exception as e:
                print(f"[DEBUG] Failed to read file {file_path}: {type(e).__name__}: {e}")
                return True, latest_checkpoint
        
        print(f"[DEBUG] Read {len(current_files)} current files")
        if current_files:
            sample_paths = list(current_files.keys())[:3]
            print(f"[DEBUG] Sample current file paths: {sample_paths}")

    # Filter current files by reader availability (same as seq_map_readers does)
    filtered_current_files = {}
    for file_path, content in current_files.items():
        extension = os.path.basename(file_path).split('.')[-1].lower() if '.' in file_path else ''
        reader = get_reader_by_extension(extension)
        if reader is not None:
            filtered_current_files[file_path] = content
        else:
            print(f"[DEBUG] Filtering out file without reader: {file_path} (extension: {extension})")
    
    print(f"[DEBUG] Filtered current files: {len(current_files)} -> {len(filtered_current_files)}")
    current_files = filtered_current_files

    # Compare file sets
    current_file_set = set(current_files.keys())
    previous_file_set = set(previous_files.keys())

    print(f"[DEBUG] Current file set size: {len(current_file_set)}")
    print(f"[DEBUG] Previous file set size: {len(previous_file_set)}")
    print(f"[DEBUG] Sets equal: {current_file_set == previous_file_set}")

    # Check for new or deleted files
    if current_file_set != previous_file_set:
        only_in_current = current_file_set - previous_file_set
        only_in_previous = previous_file_set - current_file_set
        if only_in_current:
            print(f"[DEBUG] Only in current (new files): {list(only_in_current)[:5]}")
        if only_in_previous:
            print(f"[DEBUG] Only in previous (deleted files): {list(only_in_previous)[:5]}")
        return True, latest_checkpoint

    # Compare file contents using hashes
    print(f"[DEBUG] Starting hash comparison for {len(current_file_set)} files...")
    for file_path in current_file_set:
        current_hash = compute_file_hash(current_files[file_path])
        previous_hash = compute_file_hash(previous_files.get(file_path, b''))
        
        if current_hash != previous_hash:
            print(f"[DEBUG] Hash mismatch for {file_path}")
            print(f"[DEBUG]   Current hash: {current_hash}")
            print(f"[DEBUG]   Previous hash: {previous_hash}")
            return True, latest_checkpoint

    print(f"[DEBUG] All hashes match - no changes detected")
    return False, latest_checkpoint
