import json
import logging
import os
from argparse import Namespace
from collections import OrderedDict
from datetime import datetime, timezone
from itertools import count
from multiprocessing import cpu_count
from tempfile import TemporaryDirectory as InTemporaryDirectory
from types import MethodType

from joblib import Parallel, delayed

from rich.progress import Progress, SpinnerColumn

from checkpoint import __version__ as version
from checkpoint.constants import CHECKPOINT_FORMAT_VERSION
from checkpoint.crypt import Crypt, generate_key
from checkpoint.io import IO
from checkpoint.readers import get_all_readers
from checkpoint.trace import (
    TraceGenerator, compute_file_hash, get_file_metadata, has_changes,
    is_legacy_checkpoint
)
from checkpoint.utils import LogColors, get_reader_by_extension, Logger

_logger = Logger()
logger = logging.getLogger(__name__)


def migrate_config_if_needed(config_path):
    """Migrate old config format to new format.

    Parameters
    ----------
    config_path: str
        Path to the .config file.

    Returns
    -------
    dict
        The migrated config dictionary.
    """
    if not os.path.exists(config_path):
        return None

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Check if migration needed
    if 'root_dir' in config and 'source_dir' not in config:
        config['source_dir'] = config['root_dir']
        config['dest_dir'] = config['root_dir']
        config['version'] = '2.0.0'

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)

        _logger.log(
            "Migrated config to new format (v2.0.0)",
            LogColors.INFO, timestamp=True, log_type="INFO"
        )

    return config


def load_config_with_migration(config_path):
    """Load config with backward compatibility.

    Parameters
    ----------
    config_path: str
        Path to the .config file.

    Returns
    -------
    dict
        The config dictionary with source_dir and dest_dir populated.
    """
    if not os.path.exists(config_path):
        return None

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Handle old format - provide defaults without modifying file
    if 'root_dir' in config:
        config.setdefault('source_dir', config['root_dir'])
        config.setdefault('dest_dir', config['root_dir'])

    return config


class Sequence:
    """Class to represent a sequence of operations."""

    _progress = Progress(
        SpinnerColumn(spinner_name="line"), *Progress.get_default_columns(), transient=False)

    def __init__(self, sequence_name, order_dict=None, terminal_log=False, env='UI'):
        """Initialize the sequence class.

        Parameters
        ----------
        sequence_name: str
            Name of the sequence.
        order_dict: dict, optional
            Dictionary of function names and their order in the sequence.
        logger: `checkpoint.utils.Logger`, optional
            Logger for the sequence class
        log: bool, optional
            If True, the sequence will be logged.
        """
        self.terminal_log = terminal_log
        self.log_mode = 't' if self.terminal_log else 'f'
        self.env = env

        self.logger = _logger
        self.logger.log_mode = self.log_mode

        self.sequence_name = sequence_name
        self.sequence_dict = OrderedDict()
        self.order_dict = order_dict or {}

        self._sequence_functions = self.sequence_dict.items()
        self.sequence_functions = []

        self._progress_state = "idle"

        self._task_ids = {}

        self.get_sequence_functions()

        # User hook that is triggered when the sequence/sequence function has finished
        self.on_sequence_end = lambda seq: None
        self.on_sequence_function_end = lambda seq: None

    def __repr__(self):
        """Return the string representation of the Sequence."""
        _member_functions = [
            _func.__name__ for _func in self.sequence_dict.values()]
        return f'Name: {self.name}, Member Function: {_member_functions}'

    def add_sequence_function(self, func, order=0):
        """Add a member function to the sequence.

        Parameters
        ----------
        func: method
            Function that is to be added to the sequence.
        order: int, optional
            The order of the function in the sequence
        """
        if not func.__name__.startswith('seq'):
            raise ValueError('Function name must start with "seq"')

        if order in self.sequence_dict:
            _msg = f'Warning: overriting {self.sequence_dict[order].__name__} with {func.__name__}'
            self.log(
                _msg, LogColors.WARNING, timestamp=True, log_caller=True, log_type="INFO")

        self.sequence_dict[order] = func

    def add_sub_sequence(self, sequence, order=0):
        """Add a sub sequence to the current sequence.

        Parameter
        ---------
        sequence: :class: `Sequence`
            The sub sequence that is to be added
        order: int, optional
            The order of the sub sequence in the sequence
        """
        if not isinstance(sequence, Sequence):
            raise TypeError('Sub sequence must be of type Sequence')

        _iterator = (count(start=order, step=1))
        for func_obj in sequence.sequence_dict.items():
            self.add_sequence_function(func_obj[1], order=next(_iterator))

    def execute_sequence(self, execution_policy='decreasing_order', pass_args=False):
        """Execute all functions in the current sequence.

        Parameters
        ----------
        execution_policy: str
            The policy to be followed while executing the functions.
            Possible values are 'increasing_order' or 'decreasing_order'.
        pass_args: bool
            If True, the arguments of the executed function will be passed to the next function.
        """
        self._process_task_ids()
        if self._progress_state in ["idle", "stopped"]:
            self._start_progress_bars()

        self.update_order()
        _return_values = []

        if execution_policy == 'decreasing_order':
            _sorted_sequence = sorted(self.sequence_dict.items(), reverse=True)
            for func_obj in _sorted_sequence:
                context_text = func_obj[1].__name__.split(
                    'seq_')[-1].replace('_', ' ').title()

                _current_task_id = self._task_ids[context_text]

                try:
                    if pass_args:
                        if len(_return_values) > 0:
                            _return_value = func_obj[1](_return_values[-1])
                        else:
                            _return_value = func_obj[1]()
                    else:
                        _return_value = func_obj[1]()
                except Exception as e:
                    _msg = f'{context_text}'
                    self.log(
                        _msg, [LogColors.ERROR, LogColors.UNDERLINE],
                        timestamp=True, log_type="ERROR")

                    self._progress.update(
                        _current_task_id,
                        description=f"{LogColors.ERROR}{_msg} - FAILED{LogColors.ENDC}"
                    )

                    self._stop_progress_bars()
                    raise type(e)(f'{context_text} failed with error: {e}')

                _msg = f'{context_text}'
                self.log(
                    _msg, [LogColors.SUCCESS, LogColors.BOLD],
                    timestamp=True, log_type="SUCCESS")

                self._progress.update(
                    _current_task_id,
                    description=f"{LogColors.SUCCESS}{_msg} - SUCCESS{LogColors.ENDC}", advance=1
                )

                _return_values.append(_return_value)

                self.on_sequence_function_end(self)
                _return_values.append(_return_value)

            _finish_msgs = {
                "success": "All actions finished successfully!",
                "error": "One or more actions failed!"
            }

            _tasks_finished = all(self._progress.tasks[tid].finished for tid in self._task_ids.values())

            if _tasks_finished:
                self.log(_finish_msgs["success"], [
                    LogColors.SUCCESS, LogColors.BOLD], timestamp=True, log_type="SUCCESS")

            else:
                self.log(_finish_msgs["error"], [
                    LogColors.SUCCESS, LogColors.BOLD], timestamp=True, log_type="ERROR")

            self._stop_progress_bars()
            # Only clear_live if progress was started (not for nested sequences)
            if self._progress_state == "started":
                try:
                    self._progress.console.clear_live()
                except (IndexError, RuntimeError):
                    pass  # Already cleared or not in live context
            self.on_sequence_end(self)

        elif execution_policy == 'increasing_order':
            for _, func in self.sequence_dict.items():
                if pass_args:
                    _return_value = func(_return_values[-1])
                else:
                    _return_value = func()

                _return_values.append(_return_value)

            self.on_sequence_end(self)
        else:
            raise ValueError(
                f'{execution_policy} is an invalid execution policy')
        return _return_values

    def update_order(self):
        """Update the order of sequence functions in sequence dict."""
        self.sequence_dict = OrderedDict(sorted(self.sequence_dict.items()))

    def flush_sequence(self):
        """Flush the sequence."""
        self.sequence_dict.clear()

    def get_sequence_functions(self):
        """Get all the sequence functions."""
        self.sequence_functions.clear()

        for name in dir(self):
            if name.startswith('seq') and isinstance(getattr(self, name), MethodType):
                _func = getattr(self, name)
                if name not in self.order_dict:
                    self.order_dict[name] = len(self.sequence_functions)

                self.sequence_functions.append(_func)

        self.generate_sequence()

    def generate_sequence(self):
        """Generate a sequence from all memeber functions."""
        for func in self.sequence_functions:
            _name = func.__name__
            _order = self.order_dict[_name]
            self.add_sequence_function(func, _order)

    def _process_task_ids(self):
        """Process the task ids."""
        for _, func in self.sequence_dict.items():
            _context_text = func.__name__.split('seq_')[-1].replace(
                '_', ' ').title()

            _current_task_id = Sequence._progress.add_task(
                description=_context_text,
                total=1
            )
            self._task_ids[_context_text] = _current_task_id

    def _stop_progress_bars(self):
        Sequence._progress.stop()
        self._progress_state = "stopped"

    def _start_progress_bars(self):
        Sequence._progress.start()
        self._progress_state = "started"

    def log(self, *args, **kwargs):
        """Wrapper function for `logger.log`"""
        self.logger.log(*args, **kwargs)

    @property
    def name(self):
        return self.sequence_name

    @property
    def sequence_functions(self):
        return self._sequence_functions

    @sequence_functions.setter
    def sequence_functions(self, functions):
        """Set the value of sequence functions to a list.

        Parameters
        ----------
        functions: list of methods
            List of methods that are to be assigned
        """
        self._sequence_functions = functions[:]


class IOSequence(Sequence):
    """Class to represent a sequence of IO operations."""

    def __init__(self, sequence_name='IO_Sequence', order_dict=None,
                 source_dir=None, dest_dir=None, root_dir=None, ignore_dirs=None,
                 num_cores=None, terminal_log=False, env='UI', exclusion_config=None):
        """Initialize the IO sequence class.

        Default execution sequence is:
        1. Walk through the source directory
        2. Group files by extension
        3. Map readers based on extension
        4. Read files
        5. Encrypt the files

        Parameters
        ----------
        sequence_name: str
            Name of the sequence.
        order_dict: dict, optional
            Dictionary of function names and their order in the sequence.
        source_dir: str, optional
            The source directory to track/monitor.
        dest_dir: str, optional
            The destination directory for .checkpoint storage.
            Defaults to source_dir if not provided.
        root_dir: str, optional
            [DEPRECATED] Use source_dir instead. Kept for backward compatibility.
        ignore_dirs: list of str, optional
            List of directories to be ignored.
        num_cores: int, optional
            Number of cores to be used for parallel processing.
        terminal_log: bool, optional
            If True, messages will be logged to the terminal
        exclusion_config: ExclusionConfig, optional
            Configuration for the three-tier exclusion system.
        """
        self.default_order_dict = {
            'seq_walk_directories': 4,
            'seq_group_files': 3,
            'seq_map_readers': 2,
            'seq_read_files': 1,
            'seq_encrypt_files': 0,
        }

        super(IOSequence, self).__init__(sequence_name,
                                         order_dict or self.default_order_dict,
                                         terminal_log=terminal_log, env=env)

        # Handle backward compatibility with root_dir parameter
        if root_dir is not None and source_dir is None:
            source_dir = root_dir

        self.source_dir = source_dir or os.getcwd()
        self.dest_dir = dest_dir or self.source_dir

        # Keep root_dir as an alias for source_dir for backward compatibility
        self.root_dir = self.source_dir

        self.ignore_dirs = ignore_dirs or []
        # Only add .checkpoint to ignore if source == destination
        if self.source_dir == self.dest_dir:
            self.ignore_dirs.append('.checkpoint')

        self.exclusion_config = exclusion_config
        self.io = IO(self.source_dir, ignore_dirs=self.ignore_dirs,
                     exclusion_config=exclusion_config)
        self.num_cores = num_cores or cpu_count()

    def seq_walk_directories(self):
        """Walk through all directories in the root directory.

        Parameters
        ----------
        root_directory: str
            The root directory to be walked through.
        """
        directory2files = {}
        for root, file in self.io.walk_directory():
            if root in directory2files:
                directory2files[root].append(os.path.join(root, file))
            else:
                directory2files[root] = [os.path.join(root, file)]

        return directory2files

    def seq_group_files(self, directory2files):
        """Group files in the same directory.

        Parameters
        ----------
        directory2files: dict
            Dictionary of directory names and their files.
        """
        extensions_dict = {}

        for files in directory2files.items():
            for file in files[1]:
                base_file = os.path.basename(file)
                extension = base_file.split('.')[-1].lower()

                if extension not in extensions_dict:
                    extensions_dict[extension] = [file]
                else:
                    extensions_dict[extension].append(file)

        return extensions_dict

    def seq_map_readers(self, extensions_dict):
        """Map the extensions to their respective Readers.

        Parameters
        ----------
        extensions_dict: dict
            Dictionary of extensions and their files.

        Returns
        -------
        dict
            Dictionary of extensions and their Readers.
        """
        _readers = {}
        unavailabe_extensions = []
        for extension, _ in extensions_dict.items():
            _readers[extension] = get_reader_by_extension(extension)
            if not _readers[extension]:
                # all_readers = get_all_readers()
                # with InTemporaryDirectory() as temp_dir:
                #     temp_file = os.path.join(temp_dir, f'temp.{extension}')
                #     self.io.write(temp_file, 'w+', 'test content')
                #     selected_reader = None
                #     for reader in all_readers:
                #         try:
                #             _msg = f'Trying {reader.__name__} for extension {extension}'
                #             self.log(
                #                 _msg, colors=LogColors.BOLD, log_caller=True, log_type="INFO")
                #             reader = reader()
                #             reader.read(temp_file, validate=False)
                #             selected_reader = reader
                #         except Exception:
                #             selected_reader = None
                #             continue

                #     if selected_reader:
                #         _msg = f'{selected_reader.__class__.__name__} selected'
                #         self.log(
                #             _msg, colors=LogColors.SUCCESS, timestamp=True, log_type="SUCCESS")
                #         _readers[extension] = selected_reader
                #     else:
                #         unavailabe_extensions.append(extension)
                #         del _readers[extension]
                #         self.log(
                #             f'No reader found for extension {extension}, skipping',
                #             colors=LogColors.WARNING, log_caller=True, log_type="WARNING")
                self.log(
                    f'No reader found for extension {extension}, skipping',
                    colors=LogColors.WARNING, log_caller=True, log_type="WARNING")

                unavailabe_extensions.append(extension)
                del _readers[extension]

        for extension in unavailabe_extensions:
            del extensions_dict[extension]

        return [_readers, extensions_dict]

    def seq_read_files(self, readers_extension):
        """Read the gathered files using their respective reader.

        Parameters
        ----------
        readers_extension: list
            Readers dict and extensions dict packed in a list.

        Returns
        -------
        dict
            Dictionary of files and their content.
        """
        readers_dict, extension_dict = readers_extension

        # Count total files for logging
        total_files = sum(len(files) for files in extension_dict.values())
        logger.debug(f"[Read] Starting to read {total_files} files...")
        
        for ext, files in extension_dict.items():
            for file in files:
                logger.debug(f"[Read] Reading: {file}")

        contents = \
            Parallel(self.num_cores)(delayed(readers_dict[ext].read)(files,
                                     validate=False) for (ext, files) in
                                     extension_dict.items())
        
        logger.debug(f"[Read] Completed reading {total_files} files")
        return contents

    def seq_encrypt_files(self, contents):
        """Encrypt the read files and return new format with metadata.

        Parameters
        ----------
        contents: dict
            Dictionary of file paths and their content.

        Returns
        -------
        dict
            Dictionary with new checkpoint format containing:
            - version: Checkpoint format version
            - created_at: ISO timestamp
            - files: Dict of file paths to their encrypted content and metadata
        """
        # TODO: Parallelize this
        files_data = {}
        # Use dest_dir for .checkpoint path (where the key is stored)
        crypt_obj = Crypt(key='crypt.key', key_path=os.path.join(
            self.dest_dir, '.checkpoint'))

        # Count total files for logging
        total_files = sum(len(content) for content in contents)
        logger.debug(f"[Encrypt] Starting encryption of {total_files} files...")

        for content in contents:
            for obj in content:
                path = list(obj.keys())[0]
                content_bytes = obj[path]
                
                logger.debug(f"[Encrypt] Processing: {path}")
                
                # Get file metadata
                logger.debug(f"[Encrypt] Getting metadata for: {path}")
                try:
                    metadata = get_file_metadata(path)
                except OSError:
                    # File might not exist or be accessible, use defaults
                    metadata = {'size': 0, 'mtime': 0}
                logger.debug(f"[Encrypt] Size: {metadata['size']}, Mtime: {metadata['mtime']}")
                
                # Compute hash of original content (already bytes)
                logger.debug(f"[Encrypt] Computing hash for: {path}")
                content_hash = compute_file_hash(content_bytes)
                logger.debug(f"[Encrypt] Hash: {content_hash}")
                
                # Encrypt the file content
                logger.debug(f"[Encrypt] Encrypting: {path}")
                encrypted_content = crypt_obj.encrypt(content_bytes)
                
                files_data[path] = {
                    'content': encrypted_content,
                    'hash': content_hash,
                    'size': metadata['size'],
                    'mtime': metadata['mtime']
                }

        logger.debug(f"[Encrypt] Completed encryption of {total_files} files")

        # Return new format with metadata
        return {
            'version': CHECKPOINT_FORMAT_VERSION,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'files': files_data
        }


class CheckpointSequence(Sequence):
    """Sequence to perform checkpoint operations."""

    def __init__(self, sequence_name, order_dict, source_dir, dest_dir, ignore_dirs,
                 terminal_log=False, env='UI', checkpoint_type=None, subtype=None, 
                 force=False, exclusion_config=None):
        """Initialize the CheckpointSequence class.

        Parameters
        ----------
        sequence_name: str
            Name of the sequence.
        order_dict: dict
            Dictionary of function names and their order in the sequence.
        source_dir: str
            The source directory to track/monitor.
        dest_dir: str
            The destination directory for .checkpoint storage.
        ignore_dirs: list of str
            List of directories to be ignored.
        terminal_log: bool, optional
            If True, messages will be logged to the terminal
        checkpoint_type: str, optional
            Type of checkpoint ('human', 'ai', or 'codebase'). If provided, generates trace.json.
        subtype: str, optional
            Optional subtype for the checkpoint (saved to trace.json).
        force: bool, optional
            If True, create checkpoint even if no changes detected. Default is False.
        exclusion_config: ExclusionConfig, optional
            Configuration for the three-tier exclusion system.
        """
        self.sequence_name = sequence_name
        self.order_dict = order_dict
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        # Keep root_dir as an alias for source_dir for backward compatibility
        self.root_dir = source_dir
        self.ignore_dirs = ignore_dirs
        self.checkpoint_type = checkpoint_type
        self.subtype = subtype
        self.force = force
        self.exclusion_config = exclusion_config
        super(CheckpointSequence, self).__init__(sequence_name, order_dict,
                                                 terminal_log=terminal_log, env=env)

    def _validate_checkpoint(self):
        """Validate if a checkpoint is valid."""
        # Use dest_dir for checkpoint validation
        checkpoint_path = os.path.join(
            self.dest_dir, '.checkpoint', self.sequence_name)
        if not os.path.isdir(checkpoint_path):
            raise ValueError(f'Checkpoint {self.sequence_name} does not exist')

    def seq_init_checkpoint(self):
        """Initialize the checkpoint directory."""
        # Create .checkpoint in destination directory
        _io = IO(path=self.dest_dir, mode="a",
                 ignore_dirs=self.ignore_dirs)
        
        checkpoint_dir = os.path.join(self.dest_dir, '.checkpoint')
        if not os.path.exists(checkpoint_dir):
            path = _io.make_dir('.checkpoint')
            generate_key('crypt.key', path)
        else:
            path = checkpoint_dir

        checkpoint_config = {
            'current_checkpoint': None,
            'checkpoints': [],
            'ignore_dirs': self.ignore_dirs,
            'source_dir': self.source_dir,
            'dest_dir': self.dest_dir,
            'version': '2.0.0',
        }

        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')
        _io.write(config_path, 'w+', json.dumps(checkpoint_config))

    def seq_create_checkpoint(self):
        """Create a new checkpoint for the target directory."""
        # Ensure .checkpoint exists in destination
        if not os.path.isdir(os.path.join(self.dest_dir, '.checkpoint')):
            self.seq_init_checkpoint()

        # Check if checkpoint exists in destination
        checkpoint_path = os.path.join(
            self.dest_dir, '.checkpoint', self.sequence_name)
        if os.path.isdir(checkpoint_path):
            raise ValueError(f'Checkpoint {self.sequence_name} already exists')

        # Pre-creation change detection
        # Build ignore_dirs for change detection - add .checkpoint if source == dest
        _change_detection_ignore_dirs = list(self.ignore_dirs)
        if self.source_dir == self.dest_dir:
            _change_detection_ignore_dirs.append('.checkpoint')
        
        changes_detected, previous_checkpoint = has_changes(
            source_dir=self.source_dir,
            dest_dir=self.dest_dir,
            ignore_dirs=_change_detection_ignore_dirs,
            exclusion_config=self.exclusion_config
        )

        if not changes_detected:
            if not self.force:
                self.log("No changes detected since the last checkpoint. Use --force to create a checkpoint anyway.",
                         colors=LogColors.WARNING, timestamp=True, log_type="INFO")
                return
            else:
                self.log("No changes detected, but --force specified. Creating checkpoint anyway.",
                         colors=LogColors.WARNING, timestamp=True, log_type="INFO")

        self.log(f"Creating checkpoint: {self.sequence_name}",
                 colors=LogColors.INFO, timestamp=True, log_type="INFO")

        # IO for destination (write checkpoint data)
        _io = IO(path=self.dest_dir, mode="a",
                 ignore_dirs=self.ignore_dirs)

        # IOSequence reads from source, encrypts, and we store in destination
        _io_sequence = IOSequence(source_dir=self.source_dir,
                                  dest_dir=self.dest_dir,
                                  ignore_dirs=self.ignore_dirs,
                                  terminal_log=self.terminal_log, env=self.env,
                                  exclusion_config=self.exclusion_config)

        enc_files = _io_sequence.execute_sequence(pass_args=True)[-1]

        # Create checkpoint directory in destination
        checkpoint_path = os.path.join(
            self.dest_dir, '.checkpoint', self.sequence_name)
        checkpoint_path = _io.make_dir(checkpoint_path)
        checkpoint_file_path = os.path.join(
            checkpoint_path, f'{self.sequence_name}.json')

        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')

        with open(checkpoint_file_path, 'w+') as checkpoint_file:
            json.dump(enc_files, checkpoint_file, indent=4)

        with open(config_path, 'r') as config_file:
            checkpoint_config = json.load(config_file)
            checkpoint_config['checkpoints'].append(self.sequence_name)
            checkpoint_config['current_checkpoint'] = self.sequence_name

        with open(config_path, 'w+') as config_file:
            json.dump(checkpoint_config, config_file, indent=4)

        # Walk source directory for metadata
        root2file = {}
        source_io = IO(path=self.source_dir, ignore_dirs=self.ignore_dirs,
                      exclusion_config=self.exclusion_config)
        for root, file in source_io.walk_directory():
            if root in root2file:
                root2file[root].append(os.path.join(root, file))
            else:
                root2file[root] = [os.path.join(root, file)]

        with open(os.path.join(checkpoint_path, '.metadata'), 'w+') as metadata_file:
            json.dump(root2file, metadata_file, indent=4)

        # Generate trace.json if checkpoint_type is provided
        if self.checkpoint_type:
            self.log(f"Generating trace for checkpoint: {self.sequence_name}",
                     colors=LogColors.INFO, timestamp=True, log_type="INFO")
            self._generate_trace(enc_files, checkpoint_path)

        self.log(f"Checkpoint {self.sequence_name} created successfully!",
                 colors=LogColors.SUCCESS, timestamp=True, log_type="SUCCESS")

    def _generate_trace(self, enc_files, checkpoint_path):
        """Generate trace.json for the checkpoint.

        Parameters
        ----------
        enc_files: dict
            Dictionary of encrypted file paths and their content.
            Can be in legacy format (path→encrypted_content) or
            new format (with 'version', 'created_at', 'files' keys).
        checkpoint_path: str
            Path to the checkpoint directory.
        """
        # Read key from destination
        _key = os.path.join(self.dest_dir, '.checkpoint')
        crypt = Crypt(key='crypt.key', key_path=_key)

        # Handle both legacy and new checkpoint formats
        if is_legacy_checkpoint(enc_files):
            # Legacy format: direct path→encrypted_content mapping
            files_data = enc_files
        else:
            # New format: files are nested under 'files' key
            files_data = enc_files.get('files', {})

        current_files = {}
        for file_path, file_info in files_data.items():
            # In new format, file_info is a dict with 'content', 'hash', etc.
            # In legacy format, file_info is just the encrypted content string
            if isinstance(file_info, dict):
                encrypted_content = file_info['content']
            else:
                encrypted_content = file_info
            
            content = crypt.decrypt(encrypted_content)
            current_files[file_path] = content

        # Get previous checkpoint files if available
        previous_files = None
        previous_checkpoint_name = None

        # Read config from destination
        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)

        checkpoints = config.get('checkpoints', [])
        # Get the checkpoint before the current one (current is already added)
        if len(checkpoints) > 1:
            previous_checkpoint_name = checkpoints[-2]
            previous_checkpoint_path = os.path.join(
                self.dest_dir, '.checkpoint', previous_checkpoint_name,
                f'{previous_checkpoint_name}.json')

            if os.path.exists(previous_checkpoint_path):
                with open(previous_checkpoint_path, 'r') as prev_file:
                    prev_enc_files = json.load(prev_file)

                # Handle both legacy and new checkpoint formats for previous checkpoint
                if is_legacy_checkpoint(prev_enc_files):
                    prev_files_data = prev_enc_files
                else:
                    prev_files_data = prev_enc_files.get('files', {})

                previous_files = {}
                for file_path, file_info in prev_files_data.items():
                    if isinstance(file_info, dict):
                        encrypted_content = file_info['content']
                    else:
                        encrypted_content = file_info
                    
                    content = crypt.decrypt(encrypted_content)
                    previous_files[file_path] = content

        # Generate and save trace
        trace_generator = TraceGenerator(
            checkpoint_name=self.sequence_name,
            checkpoint_type=self.checkpoint_type,
            source_dir=self.source_dir,
            dest_dir=self.dest_dir,
            subtype=self.subtype
        )
        trace_generator.generate_and_save(
            current_files=current_files,
            previous_files=previous_files,
            previous_checkpoint_name=previous_checkpoint_name
        )

    def seq_delete_checkpoint(self):
        """Delete the checkpoint for the target directory."""
        self._validate_checkpoint()
        # Use dest_dir for checkpoint operations
        _io = IO(path=self.dest_dir, mode="a",
                 ignore_dirs=self.ignore_dirs)
        checkpoint_path = os.path.join(
            self.dest_dir, '.checkpoint', self.sequence_name)

        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')
        with open(config_path, 'r') as config_file:
            checkpoint_config = json.load(config_file)
            checkpoint_config['checkpoints'].remove(self.sequence_name)
            if len(checkpoint_config['checkpoints']):
                _new_current_checkpoint = checkpoint_config['checkpoints'][-1]
            else:
                _new_current_checkpoint = None
            checkpoint_config['current_checkpoint'] = _new_current_checkpoint

        with open(config_path, 'w+') as config_file:
            json.dump(checkpoint_config, config_file, indent=4)

        _io.delete_dir(checkpoint_path)

    def seq_restore_checkpoint(self):
        """Restore back to a specific checkpoint."""
        self._validate_checkpoint()
        self.log(f"Restoring checkpoint: {self.sequence_name}",
                 colors=LogColors.INFO, timestamp=True, log_type="INFO")
        logger.debug(f"[Restore] Starting checkpoint restoration...")
        
        # IO for writing to source directory
        _io = IO(path=self.source_dir, mode="a",
                 ignore_dirs=self.ignore_dirs)
        # Read encryption key from destination
        _key = os.path.join(self.dest_dir, '.checkpoint')
        crypt = Crypt(key='crypt.key', key_path=_key)

        # Read checkpoint data from destination
        checkpoint_path = os.path.join(self.dest_dir, '.checkpoint',
                                       self.sequence_name, f'{self.sequence_name}.json')

        config_path = os.path.join(self.dest_dir, '.checkpoint', '.config')

        with open(checkpoint_path, 'r') as checkpoint_file:
            checkpoint_dict = json.load(checkpoint_file)

        with open(config_path, 'r') as config_file:
            checkpoint_config = json.load(config_file)
            checkpoint_config['current_checkpoint'] = self.sequence_name

        with open(config_path, 'w+') as config_file:
            json.dump(checkpoint_config, config_file, indent=4)

        # Handle both legacy and new checkpoint formats
        if is_legacy_checkpoint(checkpoint_dict):
            # Legacy format: direct path→encrypted_content mapping
            files_data = checkpoint_dict
            logger.debug(f"[Restore] Checkpoint format: legacy")
        else:
            # New format: files are nested under 'files' key
            files_data = checkpoint_dict.get('files', {})
            logger.debug(f"[Restore] Checkpoint format: new (v{checkpoint_dict.get('version', 'unknown')})")

        logger.debug(f"[Restore] Files to restore: {len(files_data)}")

        # Restore files to source_dir
        for file, file_info in files_data.items():
            # In new format, file_info is a dict with 'content', 'hash', etc.
            # In legacy format, file_info is just the encrypted content string
            if isinstance(file_info, dict):
                encrypted_content = file_info['content']
            else:
                encrypted_content = file_info
            
            logger.debug(f"[Restore] Decrypting: {file}")
            content = crypt.decrypt(encrypted_content)
            logger.debug(f"[Restore] Writing: {file}")
            _io.write(file, 'wb+', content)

        logger.debug(f"[Restore] Completed restoration of {len(files_data)} files")
        self.log(f"Successfully restored to checkpoint {self.sequence_name}!",
                 colors=LogColors.SUCCESS, timestamp=True, log_type="SUCCESS")

    def seq_version(self):
        """Print the version of the sequence."""
        _msg = f'Running version {version}'
        self.log(_msg, timestamp=True, log_type="INFO")


class CLISequence(Sequence):
    """Sequence for the CLI environment."""

    def __init__(self, sequence_name='CLI_Sequence', order_dict=None,
                 arg_parser=None, args=None, terminal_log=False, env='UI'):
        """Initialize the CLISequence class.

        Default execution sequence is:

        1. Parse the arguments.
        2. Determine the action to perform from the arguments.
        3. Perform the action.

        Parameters
        ----------
        sequence_name: str
            Name of the sequence.
        order_dict: dict
            Dictionary of the order of the functions in the sequence.
        arg_parser: ArgumentParser
            Argument parser for the CLI.
        """
        self.default_order_dict = {
            'seq_parse_args': 2,
            'seq_determine_action': 1,
            'seq_perform_action': 0,
        }
        self.args = args
        self.arg_parser = arg_parser
        super(CLISequence, self).__init__(sequence_name=sequence_name,
                                          order_dict=order_dict or self.default_order_dict,
                                          terminal_log=terminal_log, env=env)

    def seq_parse_args(self):
        """Parse the arguments from the CLI."""
        if self.args is None:
            args = self.arg_parser.parse_args()
        elif isinstance(self.args, Namespace):
            # Already parsed (passed from __main__.py)
            args = self.args
        else:
            args = self.arg_parser.parse_args(self.args)
        return args

    def seq_determine_action(self, args):
        """Determine the action to be performed.

        Parameters
        ----------
        args: ArgumentParser
            Parsed arguments from the CLI.
        """
        if args.action == 'create':
            action = 'seq_create_checkpoint'
        elif args.action == 'restore':
            action = 'seq_restore_checkpoint'
        elif args.action == 'delete':
            action = 'seq_delete_checkpoint'
        elif args.action == 'init':
            action = 'seq_init_checkpoint'
        elif args.action == 'version':
            action = 'seq_version'
        else:
            raise ValueError('Invalid action.')

        return [action, args]

    def seq_perform_action(self, action_args):
        """Perform the action.

        Parameters
        ----------
        action_args: list
            List containing action and args NameSpace.
        """
        action, args = action_args
        _name = args.name
        _ignore_dirs = args.ignore_dirs or []
        _checkpoint_type = getattr(args, 'type', None)
        _subtype = getattr(args, 'subtype', None)
        _force = getattr(args, 'force', False)
        _exclusion_config = getattr(args, 'exclusion_config', None)
        _helper_actions = ['seq_init_checkpoint', 'seq_version']

        # Resolve source_dir and dest_dir from args
        # Priority: source/destination > path (deprecated) > cwd
        if hasattr(args, 'source') and args.source:
            _source = os.path.abspath(args.source)
            _dest = os.path.abspath(args.destination) if hasattr(args, 'destination') and args.destination else _source
        elif hasattr(args, 'path') and args.path:
            # Backward compatibility with deprecated --path argument
            _source = os.path.abspath(args.path)
            _dest = _source
        else:
            _source = os.getcwd()
            _dest = _source

        if not (_name and _source) and action not in _helper_actions:
            raise ValueError(f'{args.action} requires a valid name and a source path')

        order_dict = {action: 0}
        _checkpoint_sequence = CheckpointSequence(
            _name, order_dict, _source, _dest, _ignore_dirs,
            terminal_log=self.terminal_log, env=self.env,
            checkpoint_type=_checkpoint_type, subtype=_subtype, force=_force,
            exclusion_config=_exclusion_config)
        action_function = getattr(_checkpoint_sequence, action)
        action_function()
