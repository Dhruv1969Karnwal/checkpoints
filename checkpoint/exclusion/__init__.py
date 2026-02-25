"""Three-Tier File/Folder Exclusion System.

This package provides a robust, hierarchical exclusion system for filtering
files and directories during checkpoint operations. The system uses three
tiers of filtering:

1. **Tier 1 - Explicit Rules**: Fast, deterministic exclusion of well-known
   OS, IDE, and version control artifacts using O(1) set lookups.

2. **Tier 2 - Gitignore Parsing**: Dynamic parsing of .gitignore files with
   full support for git wildcard syntax, negation patterns, and nested files.

3. **Tier 3 - Heuristic Analysis**: Content-based detection of binary files,
   oversized files, and minified/generated content.

Usage:
    >>> from checkpoint.exclusion import ExclusionManager, ExclusionConfig
    >>> 
    >>> # Create with default configuration
    >>> manager = ExclusionManager('/path/to/project')
    >>> 
    >>> # Check if a file should be excluded
    >>> result = manager.should_exclude('node_modules/package.json', is_directory=False)
    >>> print(result.excluded)  # True
    >>> print(result.reason)    # "Explicit directory rule: node_modules"
    >>> 
    >>> # Create with custom configuration
    >>> config = ExclusionConfig(
    ...     enable_explicit=True,
    ...     enable_gitignore=True,
    ...     enable_heuristics=True,
    ...     max_file_size=5 * 1024 * 1024,  # 5MB
    ... )
    >>> manager = ExclusionManager('/path/to/project', config=config)
    >>> 
    >>> # Walk directory with exclusions
    >>> from checkpoint.exclusion import walk_with_exclusions
    >>> for root, dirs, files in walk_with_exclusions('/path/to/project'):
    ...     for file in files:
    ...         print(os.path.join(root, file))

Classes:
    ExclusionManager: Central coordinator for the three-tier system
    ExclusionConfig: Configuration dataclass for exclusion settings
    ExclusionResult: Result of an exclusion check
    ExclusionTier: Enum representing which tier made the decision

Functions:
    walk_with_exclusions: Generator for walking directories with exclusions
"""

from .config import ExclusionConfig
from .explicit import ExplicitRulesFilter
from .gitignore import GitignoreFilter, GitignorePattern
from .heuristics import HeuristicsFilter
from .manager import ExclusionManager, walk_with_exclusions
from .types import ExclusionFilterProtocol, ExclusionResult, ExclusionTier

__all__ = [
    # Main classes
    'ExclusionManager',
    'ExclusionConfig',
    'ExclusionResult',
    'ExclusionTier',
    # Tier filters
    'ExplicitRulesFilter',
    'GitignoreFilter',
    'GitignorePattern',
    'HeuristicsFilter',
    # Protocols
    'ExclusionFilterProtocol',
    # Functions
    'walk_with_exclusions',
]

__version__ = '1.0.0'
