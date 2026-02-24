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
    previous_checkpoint_name: Optional[str] = None
) -> Dict[str, Any]:
    """Generate the complete trace.json structure.

    Parameters
    ----------
    checkpoint_name: str
        Name of the current checkpoint.
    checkpoint_type: str
        Type of checkpoint ('human' or 'ai').
    current_files: Dict[str, bytes]
        Dictionary mapping file paths to their content in the current checkpoint.
    previous_files: Optional[Dict[str, bytes]]
        Dictionary mapping file paths to their content in the previous checkpoint.
    previous_checkpoint_name: Optional[str]
        Name of the previous checkpoint.

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
        dest_dir: Optional[str] = None
    ):
        """Initialize the TraceGenerator.

        Parameters
        ----------
        checkpoint_name: str
            Name of the current checkpoint.
        checkpoint_type: str
            Type of checkpoint ('human' or 'ai').
        source_dir: str
            Source directory of the project (for reference).
        dest_dir: str, optional
            Destination directory for checkpoint storage.
            Defaults to source_dir if not provided.
        """
        self.checkpoint_name = checkpoint_name
        self.checkpoint_type = checkpoint_type
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
            previous_checkpoint_name=previous_checkpoint_name
        )

        # Save trace to destination directory
        checkpoint_dir = os.path.join(self.dest_dir, '.checkpoint', self.checkpoint_name)
        return save_trace(trace_data, checkpoint_dir)
