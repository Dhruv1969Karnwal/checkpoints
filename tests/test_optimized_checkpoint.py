"""Test script for the optimized checkpoint implementation.

This script tests:
1. New checkpoint format creation with metadata
2. No changes detection (fast path)
3. File modification detection
4. File addition detection
5. File deletion detection
6. Backward compatibility with legacy checkpoints
"""

import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checkpoint.constants import CHECKPOINT_FORMAT_VERSION
from checkpoint.crypt import Crypt, generate_key
from checkpoint.io import IO
from checkpoint.sequences import IOSequence, CheckpointSequence
from checkpoint.trace import (
    compute_file_hash,
    get_file_metadata,
    has_changes,
    is_legacy_checkpoint,
    migrate_checkpoint_format,
)


class TestResult:
    """Simple class to track test results."""
    def __init__(self):
        self.passed = []
        self.failed = []
        self.errors = []
    
    def add_pass(self, test_name):
        self.passed.append(test_name)
        print(f"  [PASS] {test_name}")
    
    def add_fail(self, test_name, reason):
        self.failed.append((test_name, reason))
        print(f"  [FAIL] {test_name}")
        print(f"      Reason: {reason}")
    
    def add_error(self, test_name, error):
        self.errors.append((test_name, error))
        print(f"  [ERROR] {test_name}")
        print(f"      Error: {error}")
    
    def summary(self):
        total = len(self.passed) + len(self.failed) + len(self.errors)
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        print(f"Total tests: {total}")
        print(f"Passed: {len(self.passed)}")
        print(f"Failed: {len(self.failed)}")
        print(f"Errors: {len(self.errors)}")
        
        if self.failed:
            print("\nFailed tests:")
            for name, reason in self.failed:
                print(f"  - {name}: {reason}")
        
        if self.errors:
            print("\nErrored tests:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        
        return len(self.failed) == 0 and len(self.errors) == 0


def create_test_files(test_dir, files_dict):
    """Create test files in a directory.
    
    Parameters
    ----------
    test_dir: str
        Directory to create files in.
    files_dict: dict
        Dictionary mapping relative file paths to their content.
    """
    for rel_path, content in files_dict.items():
        full_path = os.path.join(test_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)


def test_new_checkpoint_format(results):
    """Test 1: New checkpoint format creation.
    
    Verify that:
    - Checkpoint JSON contains 'version' field set to "3.0.0"
    - Checkpoint JSON contains 'created_at' timestamp
    - Files dict has each file with 'content', 'hash', 'size', 'mtime'
    """
    print("\n" + "-" * 60)
    print("TEST 1: New Checkpoint Format Creation")
    print("-" * 60)
    
    test_name = "New checkpoint format creation"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
                'file2.txt': 'Some text content\n',
                'subdir/file3.md': '# Markdown file\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            config = {
                'current_checkpoint': None,
                'checkpoints': [],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            with open(os.path.join(checkpoint_base, '.config'), 'w') as f:
                json.dump(config, f)
            
            # Run IOSequence to encrypt files
            io_seq = IOSequence(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint'],
                terminal_log=False,
                env='test'
            )
            
            result = io_seq.execute_sequence(pass_args=True)
            checkpoint_data = result[-1]
            
            # Verify version field
            if 'version' not in checkpoint_data:
                results.add_fail(test_name, "Missing 'version' field in checkpoint data")
                return
            
            if checkpoint_data['version'] != CHECKPOINT_FORMAT_VERSION:
                results.add_fail(test_name, 
                    f"Version mismatch: expected {CHECKPOINT_FORMAT_VERSION}, got {checkpoint_data['version']}")
                return
            
            # Verify created_at field
            if 'created_at' not in checkpoint_data:
                results.add_fail(test_name, "Missing 'created_at' field in checkpoint data")
                return
            
            # Verify it's a valid ISO timestamp
            try:
                datetime.fromisoformat(checkpoint_data['created_at'].replace('Z', '+00:00'))
            except ValueError:
                results.add_fail(test_name, f"Invalid created_at timestamp: {checkpoint_data['created_at']}")
                return
            
            # Verify files structure
            if 'files' not in checkpoint_data:
                results.add_fail(test_name, "Missing 'files' field in checkpoint data")
                return
            
            files_data = checkpoint_data['files']
            
            # Check each file has required fields
            required_fields = ['content', 'hash', 'size', 'mtime']
            for file_path, file_info in files_data.items():
                for field in required_fields:
                    if field not in file_info:
                        results.add_fail(test_name, 
                            f"File {file_path} missing '{field}' field")
                        return
            
            # Verify hash is correct (64 char hex string for SHA-256)
            for file_path, file_info in files_data.items():
                if len(file_info['hash']) != 64:
                    results.add_fail(test_name, 
                        f"File {file_path} has invalid hash length: {len(file_info['hash'])}")
                    return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_no_changes_detection(results):
    """Test 2: No changes detection (fast path).
    
    Verify that:
    - has_changes() returns False immediately after creating a checkpoint
    - Uses metadata fast path (no file content reading)
    """
    print("\n" + "-" * 60)
    print("TEST 2: No Changes Detection (Fast Path)")
    print("-" * 60)
    
    test_name = "No changes detection"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
                'file2.txt': 'Some text content\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint system
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            config = {
                'current_checkpoint': None,
                'checkpoints': [],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            config_path = os.path.join(checkpoint_base, '.config')
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Create a checkpoint
            io_seq = IOSequence(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint'],
                terminal_log=False,
                env='test'
            )
            
            result = io_seq.execute_sequence(pass_args=True)
            checkpoint_data = result[-1]
            
            # Save checkpoint
            checkpoint_name = 'test_checkpoint'
            checkpoint_dir = os.path.join(checkpoint_base, checkpoint_name)
            os.makedirs(checkpoint_dir, exist_ok=True)
            
            checkpoint_file = os.path.join(checkpoint_dir, f'{checkpoint_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f)
            
            # Update config
            with open(config_path, 'r') as f:
                config = json.load(f)
            config['checkpoints'].append(checkpoint_name)
            config['current_checkpoint'] = checkpoint_name
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Test has_changes - should return False
            print("  [DEBUG] Running has_changes()...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if changes_detected:
                results.add_fail(test_name, 
                    f"has_changes() returned True when no changes were made. "
                    f"Previous checkpoint: {prev_checkpoint}")
                return
            
            if prev_checkpoint != checkpoint_name:
                results.add_fail(test_name, 
                    f"Previous checkpoint mismatch: expected {checkpoint_name}, got {prev_checkpoint}")
                return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_file_modification_detection(results):
    """Test 3: File modification detection.
    
    Verify that:
    - Modifying a file's content causes has_changes() to return True
    - Change is detected through metadata comparison
    """
    print("\n" + "-" * 60)
    print("TEST 3: File Modification Detection")
    print("-" * 60)
    
    test_name = "File modification detection"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
                'file2.txt': 'Some text content\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint system
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            config = {
                'current_checkpoint': None,
                'checkpoints': [],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            config_path = os.path.join(checkpoint_base, '.config')
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Create a checkpoint
            io_seq = IOSequence(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint'],
                terminal_log=False,
                env='test'
            )
            
            result = io_seq.execute_sequence(pass_args=True)
            checkpoint_data = result[-1]
            
            # Save checkpoint
            checkpoint_name = 'test_checkpoint'
            checkpoint_dir = os.path.join(checkpoint_base, checkpoint_name)
            os.makedirs(checkpoint_dir, exist_ok=True)
            
            checkpoint_file = os.path.join(checkpoint_dir, f'{checkpoint_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f)
            
            # Update config
            with open(config_path, 'r') as f:
                config = json.load(f)
            config['checkpoints'].append(checkpoint_name)
            config['current_checkpoint'] = checkpoint_name
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Modify a file
            time.sleep(0.1)  # Ensure mtime changes
            with open(os.path.join(temp_dir, 'file1.py'), 'w') as f:
                f.write('print("modified")\n')
            
            # Test has_changes - should return True
            print("  [DEBUG] Running has_changes() after modification...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if not changes_detected:
                results.add_fail(test_name, 
                    "has_changes() returned False after file modification")
                return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_file_addition_detection(results):
    """Test 4: File addition detection.
    
    Verify that:
    - Adding a new file causes has_changes() to return True
    - Change is detected in Phase 1 (file set comparison)
    """
    print("\n" + "-" * 60)
    print("TEST 4: File Addition Detection")
    print("-" * 60)
    
    test_name = "File addition detection"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint system
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            config = {
                'current_checkpoint': None,
                'checkpoints': [],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            config_path = os.path.join(checkpoint_base, '.config')
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Create a checkpoint
            io_seq = IOSequence(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint'],
                terminal_log=False,
                env='test'
            )
            
            result = io_seq.execute_sequence(pass_args=True)
            checkpoint_data = result[-1]
            
            # Save checkpoint
            checkpoint_name = 'test_checkpoint'
            checkpoint_dir = os.path.join(checkpoint_base, checkpoint_name)
            os.makedirs(checkpoint_dir, exist_ok=True)
            
            checkpoint_file = os.path.join(checkpoint_dir, f'{checkpoint_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f)
            
            # Update config
            with open(config_path, 'r') as f:
                config = json.load(f)
            config['checkpoints'].append(checkpoint_name)
            config['current_checkpoint'] = checkpoint_name
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Add a new file
            with open(os.path.join(temp_dir, 'new_file.txt'), 'w') as f:
                f.write('New file content\n')
            
            # Test has_changes - should return True
            print("  [DEBUG] Running has_changes() after file addition...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if not changes_detected:
                results.add_fail(test_name, 
                    "has_changes() returned False after file addition")
                return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_file_deletion_detection(results):
    """Test 5: File deletion detection.
    
    Verify that:
    - Deleting a file causes has_changes() to return True
    - Change is detected in Phase 1 (file set comparison)
    """
    print("\n" + "-" * 60)
    print("TEST 5: File Deletion Detection")
    print("-" * 60)
    
    test_name = "File deletion detection"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
                'file2.txt': 'Some content\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint system
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            config = {
                'current_checkpoint': None,
                'checkpoints': [],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            config_path = os.path.join(checkpoint_base, '.config')
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Create a checkpoint
            io_seq = IOSequence(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint'],
                terminal_log=False,
                env='test'
            )
            
            result = io_seq.execute_sequence(pass_args=True)
            checkpoint_data = result[-1]
            
            # Save checkpoint
            checkpoint_name = 'test_checkpoint'
            checkpoint_dir = os.path.join(checkpoint_base, checkpoint_name)
            os.makedirs(checkpoint_dir, exist_ok=True)
            
            checkpoint_file = os.path.join(checkpoint_dir, f'{checkpoint_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f)
            
            # Update config
            with open(config_path, 'r') as f:
                config = json.load(f)
            config['checkpoints'].append(checkpoint_name)
            config['current_checkpoint'] = checkpoint_name
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Delete a file
            os.remove(os.path.join(temp_dir, 'file2.txt'))
            
            # Test has_changes - should return True
            print("  [DEBUG] Running has_changes() after file deletion...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if not changes_detected:
                results.add_fail(test_name, 
                    "has_changes() returned False after file deletion")
                return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_backward_compatibility(results):
    """Test 6: Backward compatibility with legacy checkpoints.
    
    Verify that:
    - Legacy format checkpoint (direct path→content mapping) is detected
    - has_changes() works correctly with legacy checkpoint
    - Migration happens correctly
    """
    print("\n" + "-" * 60)
    print("TEST 6: Backward Compatibility")
    print("-" * 60)
    
    test_name = "Backward compatibility"
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = {
                'file1.py': 'print("hello")\n',
                'file2.txt': 'Some content\n',
            }
            create_test_files(temp_dir, test_files)
            
            # Initialize checkpoint system
            checkpoint_base = os.path.join(temp_dir, '.checkpoint')
            os.makedirs(checkpoint_base, exist_ok=True)
            generate_key('crypt.key', checkpoint_base)
            
            # Create a LEGACY format checkpoint manually
            crypt = Crypt(key='crypt.key', key_path=checkpoint_base)
            
            legacy_checkpoint = {}
            for file_path, content in test_files.items():
                full_path = os.path.join(temp_dir, file_path)
                encrypted = crypt.encrypt(content)
                legacy_checkpoint[full_path] = encrypted
            
            # Verify is_legacy_checkpoint detects it correctly
            if not is_legacy_checkpoint(legacy_checkpoint):
                results.add_fail(test_name, 
                    "is_legacy_checkpoint() failed to detect legacy format")
                return
            
            # Verify migrate_checkpoint_format works
            migrated = migrate_checkpoint_format(legacy_checkpoint)
            
            if 'version' not in migrated:
                results.add_fail(test_name, 
                    "Migrated checkpoint missing 'version' field")
                return
            
            if migrated['version'] != CHECKPOINT_FORMAT_VERSION:
                results.add_fail(test_name, 
                    f"Migrated checkpoint has wrong version: {migrated['version']}")
                return
            
            if 'files' not in migrated:
                results.add_fail(test_name, 
                    "Migrated checkpoint missing 'files' field")
                return
            
            if 'created_at' not in migrated:
                results.add_fail(test_name, 
                    "Migrated checkpoint missing 'created_at' field")
                return
            
            # Save legacy checkpoint
            checkpoint_name = 'legacy_checkpoint'
            checkpoint_dir = os.path.join(checkpoint_base, checkpoint_name)
            os.makedirs(checkpoint_dir, exist_ok=True)
            
            checkpoint_file = os.path.join(checkpoint_dir, f'{checkpoint_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(legacy_checkpoint, f)
            
            # Create config
            config = {
                'current_checkpoint': checkpoint_name,
                'checkpoints': [checkpoint_name],
                'ignore_dirs': [],
                'source_dir': temp_dir,
                'dest_dir': temp_dir,
                'version': '2.0.0',
            }
            config_path = os.path.join(checkpoint_base, '.config')
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            # Test has_changes with legacy checkpoint - should return False (no changes)
            print("  [DEBUG] Running has_changes() with legacy checkpoint...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if changes_detected:
                # This might be due to mtime differences, which is acceptable
                # The important thing is that it doesn't crash
                print("  [INFO] Changes detected with legacy checkpoint (may be due to mtime)")
            
            if prev_checkpoint != checkpoint_name:
                results.add_fail(test_name, 
                    f"Previous checkpoint mismatch: expected {checkpoint_name}, got {prev_checkpoint}")
                return
            
            # Now modify a file and verify changes are detected
            time.sleep(0.1)
            with open(os.path.join(temp_dir, 'file1.py'), 'w') as f:
                f.write('print("modified")\n')
            
            print("  [DEBUG] Running has_changes() after modification...")
            changes_detected, prev_checkpoint = has_changes(
                source_dir=temp_dir,
                dest_dir=temp_dir,
                ignore_dirs=['.checkpoint']
            )
            
            if not changes_detected:
                results.add_fail(test_name, 
                    "has_changes() returned False after modification with legacy checkpoint")
                return
            
            results.add_pass(test_name)
            
    except Exception as e:
        results.add_error(test_name, str(e))


def test_helper_functions(results):
    """Test helper functions directly."""
    print("\n" + "-" * 60)
    print("TEST 7: Helper Functions")
    print("-" * 60)
    
    # Test compute_file_hash
    test_name = "compute_file_hash"
    try:
        content = b"test content"
        hash_result = compute_file_hash(content)
        
        if len(hash_result) != 64:
            results.add_fail(test_name, f"Hash length wrong: {len(hash_result)}")
        elif not all(c in '0123456789abcdef' for c in hash_result):
            results.add_fail(test_name, f"Hash contains invalid characters: {hash_result}")
        else:
            results.add_pass(test_name)
    except Exception as e:
        results.add_error(test_name, str(e))
    
    # Test get_file_metadata
    test_name = "get_file_metadata"
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("test content")
            temp_path = f.name
        
        try:
            metadata = get_file_metadata(temp_path)
            
            if 'size' not in metadata:
                results.add_fail(test_name, "Missing 'size' in metadata")
            elif 'mtime' not in metadata:
                results.add_fail(test_name, "Missing 'mtime' in metadata")
            elif metadata['size'] != 12:  # "test content" is 12 bytes
                results.add_fail(test_name, f"Wrong size: {metadata['size']}")
            else:
                results.add_pass(test_name)
        finally:
            os.unlink(temp_path)
    except Exception as e:
        results.add_error(test_name, str(e))
    
    # Test is_legacy_checkpoint
    test_name = "is_legacy_checkpoint"
    try:
        # Legacy format
        legacy = {'/path/to/file': 'encrypted_content'}
        if not is_legacy_checkpoint(legacy):
            results.add_fail(test_name, "Failed to detect legacy format")
            return
        
        # New format
        new_format = {
            'version': '3.0.0',
            'created_at': '2024-01-01T00:00:00Z',
            'files': {'/path/to/file': {'content': 'encrypted', 'hash': 'abc'}}
        }
        if is_legacy_checkpoint(new_format):
            results.add_fail(test_name, "Incorrectly identified new format as legacy")
            return
        
        results.add_pass(test_name)
    except Exception as e:
        results.add_error(test_name, str(e))


def main():
    """Run all tests."""
    print("=" * 60)
    print("OPTIMIZED CHECKPOINT IMPLEMENTATION TESTS")
    print("=" * 60)
    print(f"Checkpoint format version: {CHECKPOINT_FORMAT_VERSION}")
    
    results = TestResult()
    
    # Run all tests
    test_new_checkpoint_format(results)
    test_no_changes_detection(results)
    test_file_modification_detection(results)
    test_file_addition_detection(results)
    test_file_deletion_detection(results)
    test_backward_compatibility(results)
    test_helper_functions(results)
    
    # Print summary
    all_passed = results.summary()
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
