import builtins
import os
import warnings
from argparse import ArgumentParser
from typing import Tuple

from rich import print as rich_print

from checkpoint import __version__ as version
from checkpoint.sequences import CLISequence
from checkpoint.utils import execute_command


builtins.print = rich_print


def resolve_paths(args) -> Tuple[str, str]:
    """Resolve source and destination paths from arguments.

    Priority:
    1. If --source and --destination provided: use both
    2. If only --source provided: destination = source
    3. If only --path provided: source = destination = path (backward compat)
    4. If neither provided: source = destination = os.getcwd()

    Parameters
    ----------
    args : Namespace
        Parsed command line arguments.

    Returns
    -------
    Tuple[str, str]
        A tuple of (source_dir, dest_dir) as absolute paths.
    """
    source = getattr(args, 'source', None)
    destination = getattr(args, 'destination', None)
    path = getattr(args, 'path', None)

    if source:
        source_dir = os.path.abspath(os.path.expanduser(source))
        dest_dir = os.path.abspath(os.path.expanduser(destination)) if destination else source_dir
    elif path:
        # Backward compatibility: --path sets both source and destination
        source_dir = os.path.abspath(os.path.expanduser(path))
        dest_dir = source_dir
    else:
        source_dir = os.getcwd()
        dest_dir = source_dir

    return source_dir, dest_dir


def validate_source_dir(source_dir: str) -> None:
    """Validate that source directory exists and is accessible.

    Parameters
    ----------
    source_dir : str
        Path to the source directory.

    Raises
    ------
    ValueError
        If source directory does not exist or is not a directory.
    PermissionError
        If source directory is not readable.
    """
    if not os.path.exists(source_dir):
        raise ValueError(f"Source directory does not exist: {source_dir}")
    if not os.path.isdir(source_dir):
        raise ValueError(f"Source path is not a directory: {source_dir}")
    if not os.access(source_dir, os.R_OK):
        raise PermissionError(f"Cannot read from source directory: {source_dir}")


def ensure_destination_dir(dest_dir: str) -> None:
    """Ensure destination directory exists, create if needed.

    Parameters
    ----------
    dest_dir : str
        Path to the destination directory.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        print(f"[yellow]Created destination directory: {dest_dir}[/yellow]")


def run(args=None):
    checkpoint_arg_parser = ArgumentParser(
        description=f"Create restore points in your projects. Version: {version}",
        prog="checkpoint",
    )

    checkpoint_arg_parser.add_argument(
        "--run-ui",
        action="store_true",
        help="Start checkpoint in UI environment",
        default=False,
    )

    checkpoint_arg_parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="Name of the restore point.",
    )

    checkpoint_arg_parser.add_argument(
        "-s",
        "--source",
        type=str,
        default=None,
        help="Path to the source directory to track.",
    )

    checkpoint_arg_parser.add_argument(
        "-d",
        "--destination",
        type=str,
        default=None,
        help="Path where .checkpoint folder will be created (defaults to source).",
    )

    checkpoint_arg_parser.add_argument(
        "-p",
        "--path",
        type=str,
        default=None,
        help="[DEPRECATED] Use --source and --destination instead. Path to the project.",
    )

    checkpoint_arg_parser.add_argument(
        "-a",
        "--action",
        type=str,
        help="Action to perform.",
        choices=["create", "restore", "version", "delete", "init"],
    )

    checkpoint_arg_parser.add_argument(
        "--ignore-dirs",
        "-i",
        nargs="+",
        default=[".git", ".idea", ".vscode",
                 ".venv", "node_modules", "__pycache__" , "venv"],
        help="Ignore directories."
    )

    checkpoint_arg_parser.add_argument(
        "--type",
        "-t",
        type=str,
        help="Type of checkpoint: human, ai, or codebase (default: codebase)",
        choices=["human", "ai", "codebase"],
        default="codebase"
    )

    checkpoint_arg_parser.add_argument(
        "--subtype",
        "-st",
        type=str,
        default=None,
        help="Optional subtype for the checkpoint (saved to trace.json)"
    )

    checkpoint_arg_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        default=False,
        help="Force checkpoint creation even if no changes detected."
    )

    if args is not None:
        run_ui = args.run_ui
    else:
        run_ui = checkpoint_arg_parser.parse_args().run_ui

    if run_ui:
        _dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(os.path.join(_dir, "ui"))
        for line in execute_command('yarn start'):
            print(line, end='')
            if "exited" in line:
                exit(0)
    else:
        # Parse arguments and resolve paths
        parsed_args = checkpoint_arg_parser.parse_args() if args is None else args

        # Show deprecation warning if --path is used
        if parsed_args.path:
            warnings.warn(
                "The '--path' argument is deprecated. Use '--source' and '--destination' instead.",
                DeprecationWarning,
                stacklevel=2
            )

        # Resolve and validate paths
        source_dir, dest_dir = resolve_paths(parsed_args)

        # Validate source directory exists (except for 'init' and 'version' actions)
        action = getattr(parsed_args, 'action', None)
        if action not in ['init', 'version', None]:
            validate_source_dir(source_dir)

        # Ensure destination directory exists
        ensure_destination_dir(dest_dir)

        # Store resolved paths on args for CLISequence to access
        parsed_args.source_dir = source_dir
        parsed_args.dest_dir = dest_dir

        cli_sequence = CLISequence(
            arg_parser=checkpoint_arg_parser, args=parsed_args, terminal_log=True, env='CLI')
        cli_sequence.execute_sequence(pass_args=True)


if __name__ == "__main__":
    run()
