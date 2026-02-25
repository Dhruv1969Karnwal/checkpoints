import os
from os.path import isdir
from os.path import join as pjoin
from shutil import rmtree
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from checkpoint.exclusion import ExclusionConfig


class IO:
    """Class to perform Input/Output opreations.

    Provides methods to perform various IO operations
    in the target directory.

    Attributes
    ----------
    path: str
        Path to the target directory
    mode: str
        Mode of operations, valid values are
        `a`: IO has all permissions (R/W/X/A)
        `m`: IO has moderate permissions (R/W/A)
        `s`: IO has limited permissions (R/A)
    """

    def __init__(
        self,
        path=None,
        mode="a",
        ignore_dirs=None,
        lazy=True,
        exclusion_config: "ExclusionConfig" = None,
    ):
        """Initialize the IO class.

        Parameters
        ----------
        path: str, optional
            Path to perform operations on
        mode: str, optional
            Mode of operations, valid values are
            `a`: IO has all permissions (R/W/X/A)
            `m`: IO has moderate permissions (R/W/A)
            `s`: IO has limited permissions (R/A)
        ignore_dirs: list
            List of directories to ignore (merged with exclusion config)
        lazy: bool, optional
            If True, the IO class will not update sub_dirs and files
        exclusion_config: ExclusionConfig, optional
            Configuration for the three-tier exclusion system.
            If provided, the ExclusionManager will be used for filtering.
        """
        self._path = str()
        self._mode = str()
        self.ignore_dirs = ignore_dirs or []
        self.files = []
        self.sub_dirs = []
        self._exclusion_config = exclusion_config
        self._exclusion_manager = None

        self.lazy = lazy

        self.path = path or os.getcwd()
        self.mode = mode

        if not isdir(self.path):
            raise IOError(
                f'{self.path} is not a valid directory'
            )

        if self.mode not in ['a', 'm', 's']:
            raise ValueError(
                f'{self.mode} is not a valid IO operation mode'
            )

        self.setup()

    def setup(self):
        """Setup the IO class."""
        self.mode_mappings = {'a': [*'rwxa', 'wb+', 'w+', 'rb+', 'rb'],
                              'm': [*'rwa', 'wb', 'rb'],
                              's': [*'ra', 'rb']}

        # Initialize exclusion manager if config is provided
        if self._exclusion_config is not None:
            from checkpoint.exclusion import ExclusionManager
            # Merge ignore_dirs into the exclusion config
            self._exclusion_config.custom_dirs.extend(self.ignore_dirs)
            self._exclusion_manager = ExclusionManager(self._path, self._exclusion_config)

        if self.lazy:
            self.files = []
            self.sub_dirs = []
        else:
            self.update_paths(self._path)

    def update_paths(self, path):
        """Update the paths of files, sub_dirs w.r.t the path.

        Parameters
        ----------
        path: str
            Path to the target directory
        """
        self.files.clear()
        self.sub_dirs.clear()

        # Use ExclusionManager if available
        if self._exclusion_manager is not None:
            for root, dirs, files in os.walk(path):
                # Filter directories in-place
                dirs[:] = [
                    d for d in dirs
                    if not self._exclusion_manager.should_exclude(
                        pjoin(root, d), is_directory=True
                    ).excluded
                ]
                
                for file in files:
                    if not self._exclusion_manager.should_exclude(
                        pjoin(root, file), is_directory=False
                    ).excluded:
                        self.files.append(pjoin(root, file))

                for subdir in dirs:
                    self.sub_dirs.append(pjoin(root, subdir))
        else:
            # Fallback to simple ignore_dirs filtering
            for path, subdirs, files in os.walk(path):
                if all(dir not in path for dir in self.ignore_dirs):
                    for file in files:
                        self.files.append(pjoin(path, file))

                    for subdir in subdirs:
                        self.sub_dirs.append(pjoin(path, subdir))

    def walk_directory(self):
        """Walk through the root directory.
        
        Uses the ExclusionManager if available, otherwise falls back
        to simple ignore_dirs filtering.
        """
        # Use ExclusionManager if available
        if self._exclusion_manager is not None:
            for root, dirs, files in os.walk(self.path):
                # Filter directories in-place
                dirs[:] = [
                    d for d in dirs
                    if not self._exclusion_manager.should_exclude(
                        pjoin(root, d), is_directory=True
                    ).excluded
                ]
                
                for file in files:
                    if not self._exclusion_manager.should_exclude(
                        pjoin(root, file), is_directory=False
                    ).excluded:
                        yield [root, file]
        else:
            # Fallback to simple ignore_dirs filtering
            for root, _, files in os.walk(self.path):
                if all(dir not in root for dir in self.ignore_dirs):
                    for file in files:
                        yield [root, file]

    def _validate_mode(self, mode):
        if mode not in self.mode_mappings[self.mode]:
            raise IOError(
                f'Mode {mode} not allowed with IO mode {self.mode}'
            )

    def read(self, file, mode='r', encoding='utf-8', errors='replace'):
        """Read the content of a file

        Parameters
        ----------
        file: str
            Name of the file
        mode: str, optional
            Mode of operation
        encoding: str, optional
            File encoding (default: 'utf-8')
        errors: str, optional
            How to handle encoding errors (default: 'replace')
        """
        self._validate_mode(mode)
        if 'b' in mode:
            with open(file, mode) as f:
                content = f.read()
        else:
            with open(file, mode, encoding=encoding, errors=errors) as f:
                content = f.read()

        return content

    def write(self, file, mode, content):
        """Write some content into a file.

        Parameters
        ----------
        file: str
            Name of the file
        mode: str
            Mode of operation
        content: str
            Content to write in the file
        """
        self._validate_mode(mode)
        with open(file, mode) as f:
            f.write(content)

    def open(self, file, mode):
        """Open a file in a given mode.

        Parameters
        ----------
        file: str
            Name of the file
        mode: str
            Mode of operation
        """
        self._validate_mode(mode)
        return open(file, mode)

    def get_file_extension(self, file_path):
        """Get the extension from the file.

        Parameters
        ----------
        file_path: str
            Path to the file
        """
        _file = os.path.basename(file_path)
        return _file.split('.')[-1].lower()

    def make_dir(self, dir_name):
        """Make a sub directory in the root directory.

        Parameters
        ----------
        dir_name: str
            Name of the sub directory
        """
        if os.path.join(self.path, dir_name) in self.sub_dirs:
            raise IOError(
                f'{dir_name} already exists'
            )

        try:
            os.mkdir(pjoin(self.path, dir_name))
        except FileExistsError:
            rmtree(pjoin(self.path, dir_name))
            os.mkdir(pjoin(self.path, dir_name))

        return pjoin(self.path, dir_name)

    def delete_dir(self, dir_name):
        """Delete a sub directory in the root directory.

        Parameters
        ----------
        dir_name: str
            Name of the sub directory
        """

        if not os.path.isdir(pjoin(self.path, dir_name)):
            raise IOError(
                f'{dir_name} does not exist'
            )

        rmtree(pjoin(self.path, dir_name))

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        """Set the value of the current path.

        Parameters
        ----------
        path: str
            New path
        """
        self._path = path
        if not self.lazy:
            self.update_paths(self._path)

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, mode):
        """Set the value of the IO mode.

        Parameters
        ----------
        mode: str
            Mode of operations, valid values are
            `a`: IO has all permissions (R/W/X/A)
            `m`: IO has moderate permissions (R/W/A)
            `s`: IO has limited permissions (R/A)
        """
        self._mode = mode

        if self._mode not in [*'ams']:
            raise ValueError(
                f'{self._mode} is not a valid IO operation mode'
            )

    @property
    def mode_mapping(self):
        return self.mode_mappings

    @mode_mapping.setter
    def mode_mapping(self, io_mode):
        """Add a IO permission to a specific mode

        Parameters
        ----------
        io_mode: Iterable
            Iterable that packs the mode, IO permission
            Valid values are
            `a`: IO has all permissions (R/W/X/A)
            `m`: IO has moderate permissions (R/W/A)
            `s`: IO has limited permissions (R/A)
        """
        if len(io_mode) != 2:
            raise ValueError(
                f'Iterable {io_mode} can have max two values packed'
            )

        mode, io_permission = io_mode
        self.mode_mappings[mode].append(io_permission)
