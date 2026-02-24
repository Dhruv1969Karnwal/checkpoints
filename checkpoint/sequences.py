import json
import os
from argparse import Namespace
from collections import OrderedDict
from itertools import count
from multiprocessing import cpu_count
from tempfile import TemporaryDirectory as InTemporaryDirectory
from types import MethodType

from joblib import Parallel, delayed

from rich.progress import Progress, SpinnerColumn

from checkpoint import __version__ as version
from checkpoint.crypt import Crypt, generate_key
from checkpoint.io import IO
from checkpoint.readers import get_all_readers
from checkpoint.trace import TraceGenerator
from checkpoint.utils import LogColors, get_reader_by_extension, Logger

_logger = Logger()


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
        SpinnerColumn(), *Progress.get_default_columns(), transient=False)

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

            if self._progress.finished:
                self.log(_finish_msgs["success"], [
                    LogColors.SUCCESS, LogColors.BOLD], timestamp=True, log_type="SUCCESS")

            else:
                self.log(_finish_msgs["error"], [
                    LogColors.SUCCESS, LogColors.BOLD], timestamp=True, log_type="ERROR")

            self._stop_progress_bars()
            self._progress.console.clear_live()
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
        if self.env == 'UI':
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
                 num_cores=None, terminal_log=False, env='UI'):
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

        self.io = IO(self.source_dir, ignore_dirs=self.ignore_dirs)
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

        contents = \
            Parallel(self.num_cores)(delayed(readers_dict[ext].read)(files,
                                     validate=False) for (ext, files) in
                                     extension_dict.items())
        return contents

    def seq_encrypt_files(self, contents):
        """Encrypt the read files.

        Parameters
        ----------
        contents: dict
            Dictionary of file paths and their content.

        Returns
        -------
        dict
            Dictionary of file paths and their encrypted content.
        """
        # TODO: Parallelize this
        path2content = {}
        # Use dest_dir for .checkpoint path (where the key is stored)
        crypt_obj = Crypt(key='crypt.key', key_path=os.path.join(
            self.dest_dir, '.checkpoint'))

        for content in contents:
            for obj in content:
                path = list(obj.keys())[0]
                path2content[path] = crypt_obj.encrypt(path)

        return path2content


class CheckpointSequence(Sequence):
    """Sequence to perform checkpoint operations."""

    def __init__(self, sequence_name, order_dict, source_dir, dest_dir, ignore_dirs,
                 terminal_log=False, env='UI', checkpoint_type=None):
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
            Type of checkpoint ('human' or 'ai'). If provided, generates trace.json.
        """
        self.sequence_name = sequence_name
        self.order_dict = order_dict
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        # Keep root_dir as an alias for source_dir for backward compatibility
        self.root_dir = source_dir
        self.ignore_dirs = ignore_dirs
        self.checkpoint_type = checkpoint_type
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
        path = _io.make_dir('.checkpoint')
        generate_key('crypt.key', path)

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
        # Check if checkpoint exists in destination
        checkpoint_path = os.path.join(
            self.dest_dir, '.checkpoint', self.sequence_name)
        if os.path.isdir(checkpoint_path):
            raise ValueError(f'Checkpoint {self.sequence_name} already exists')

        # IO for destination (write checkpoint data)
        _io = IO(path=self.dest_dir, mode="a",
                 ignore_dirs=self.ignore_dirs)

        # IOSequence reads from source, encrypts, and we store in destination
        _io_sequence = IOSequence(source_dir=self.source_dir,
                                  dest_dir=self.dest_dir,
                                  ignore_dirs=self.ignore_dirs,
                                  terminal_log=self.terminal_log, env=self.env)

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
        source_io = IO(path=self.source_dir, ignore_dirs=self.ignore_dirs)
        for root, file in source_io.walk_directory():
            if root in root2file:
                root2file[root].append(os.path.join(root, file))
            else:
                root2file[root] = [os.path.join(root, file)]

        with open(os.path.join(checkpoint_path, '.metadata'), 'w+') as metadata_file:
            json.dump(root2file, metadata_file, indent=4)

        # Generate trace.json if checkpoint_type is provided
        if self.checkpoint_type:
            self._generate_trace(enc_files, checkpoint_path)

    def _generate_trace(self, enc_files, checkpoint_path):
        """Generate trace.json for the checkpoint.

        Parameters
        ----------
        enc_files: dict
            Dictionary of encrypted file paths and their content.
        checkpoint_path: str
            Path to the checkpoint directory.
        """
        # Read key from destination
        _key = os.path.join(self.dest_dir, '.checkpoint')
        crypt = Crypt(key='crypt.key', key_path=_key)

        current_files = {}
        for file_path, encrypted_content in enc_files.items():
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

                previous_files = {}
                for file_path, encrypted_content in prev_enc_files.items():
                    content = crypt.decrypt(encrypted_content)
                    previous_files[file_path] = content

        # Generate and save trace
        trace_generator = TraceGenerator(
            checkpoint_name=self.sequence_name,
            checkpoint_type=self.checkpoint_type,
            source_dir=self.source_dir,
            dest_dir=self.dest_dir
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

        # Restore files to source_dir
        for file, content in checkpoint_dict.items():
            content = crypt.decrypt(content)
            _io.write(file, 'wb+', content)

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
            checkpoint_type=_checkpoint_type)
        action_function = getattr(_checkpoint_sequence, action)
        action_function()
