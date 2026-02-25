"""Tier 1: Explicit Rules Filter.

This module provides fast, deterministic exclusion of well-known OS, IDE,
and version control artifacts that should never be included in checkpoints.
Uses O(1) set lookups for optimal performance.
"""

import os
import re
from pathlib import Path
from typing import FrozenSet, List, Set

from .config import ExclusionConfig
from .types import ExclusionResult, ExclusionTier


class ExplicitRulesFilter:
    """Tier 1: Fast explicit rule matching for well-known artifacts.
    
    This filter provides O(1) exclusion checks for:
    - Version control directories (.git, .svn, .hg, etc.)
    - IDE configuration directories (.idea, .vscode, etc.)
    - OS-specific artifacts (.DS_Store, Thumbs.db, etc.)
    - Language-specific artifacts (__pycache__, node_modules, etc.)
    
    Attributes:
        DIRECTORY_PATTERNS: Frozen set of directory names to exclude
        FILE_PATTERNS: Frozen set of exact file names to exclude
        FILE_GLOBS: List of compiled regex patterns for file matching
    """
    
    # Version control systems
    VCS_DIRS: FrozenSet[str] = frozenset({
        '.git', '.svn', '.hg', '.bzr', '_darcs', '.gitignore', '.gitmodules',
    })
    
    # IDE and editor configurations
    IDE_DIRS: FrozenSet[str] = frozenset({
        '.idea', '.vscode', '.vs', '.settings', '.eclipse', '.sublime-project',
        '.sublime-workspace', '.atom', '.brackets', '.vscode-test',
    })
    
    # Build and distribution directories
    BUILD_DIRS: FrozenSet[str] = frozenset({
        'dist', 'build', 'target', 'out', 'bin', 'obj', '.eggs', 'eggs',
        'wheels', '*.egg-info', 'htmlcov', '.tox', '.nox', '.hypothesis',
    })
    
    # Language/package manager directories
    PACKAGE_DIRS: FrozenSet[str] = frozenset({
        'node_modules', '.npm', '.yarn', '.pnpm-store', 'vendor', 'venv',
        '.venv', 'env', '.env', '__pycache__', '.mypy_cache', '.pytest_cache',
        '.ruff_cache', 'site-packages', 'pip-cache',
    })
    
    # OS-specific directories
    OS_DIRS: FrozenSet[str] = frozenset({
        '.Spotlight-V100', '.Trashes', '.fseventsd', '.TemporaryItems',
        'System Volume Information', '$RECYCLE.BIN',
    })
    
    # Combined directory patterns
    DIRECTORY_PATTERNS: FrozenSet[str] = VCS_DIRS | IDE_DIRS | BUILD_DIRS | PACKAGE_DIRS | OS_DIRS
    
    # OS-specific files
    OS_FILES: FrozenSet[str] = frozenset({
        '.DS_Store', 'Thumbs.db', 'desktop.ini', 'Desktop.ini', 'ehthumbs.db',
        '.Trashes', '.fseventsd', '.Spotlight-V100',
    })
    
    # Python artifacts
    PYTHON_FILES: FrozenSet[str] = frozenset({
        '.Python', 'pip-log.txt', 'pip-delete-this-directory.txt',
    })
    
    # Combined exact file patterns
    FILE_PATTERNS: FrozenSet[str] = OS_FILES | PYTHON_FILES
    
    # Compiled regex patterns for glob-like matching
    FILE_GLOBS: List[re.Pattern] = [
        # Python artifacts
        re.compile(r'.*\.pyc$'),
        re.compile(r'.*\.pyo$'),
        re.compile(r'.*\.pyd$'),
        re.compile(r'.*\.egg$'),
        re.compile(r'.*\.spec$'),
        re.compile(r'.*\.manifest$'),
        # Editor/IDE files
        re.compile(r'.*\.swp$'),
        re.compile(r'.*\.swo$'),
        re.compile(r'.*~$'),
        re.compile(r'.*\.sublime-project$'),
        re.compile(r'.*\.sublime-workspace$'),
        # Lock files (often large and generated)
        re.compile(r'.*-lock\.json$'),
        re.compile(r'.*\.lock$'),
        # Compiled/generated files
        re.compile(r'.*\.so$'),
        re.compile(r'.*\.dll$'),
        re.compile(r'.*\.dylib$'),
        re.compile(r'.*\.exe$'),
        re.compile(r'.*\.bin$'),
        # Archive files
        re.compile(r'.*\.zip$'),
        re.compile(r'.*\.tar$'),
        re.compile(r'.*\.tar\.gz$'),
        re.compile(r'.*\.tgz$'),
        re.compile(r'.*\.rar$'),
        re.compile(r'.*\.7z$'),
        # Image files
        re.compile(r'.*\.png$'),
        re.compile(r'.*\.jpg$'),
        re.compile(r'.*\.jpeg$'),
        re.compile(r'.*\.gif$'),
        re.compile(r'.*\.ico$'),
        re.compile(r'.*\.svg$'),
        re.compile(r'.*\.webp$'),
        # Font files
        re.compile(r'.*\.ttf$'),
        re.compile(r'.*\.otf$'),
        re.compile(r'.*\.woff$'),
        re.compile(r'.*\.woff2$'),
        re.compile(r'.*\.eot$'),
        # Database files
        re.compile(r'.*\.db$'),
        re.compile(r'.*\.sqlite$'),
        re.compile(r'.*\.sqlite3$'),
        # Log files
        re.compile(r'.*\.log$'),
        # Certificate and key files
        re.compile(r'.*\.pem$'),
        re.compile(r'.*\.key$'),
        re.compile(r'.*\.crt$'),
        re.compile(r'.*\.p12$'),
        re.compile(r'.*\.pfx$'),
    ]
    
    def __init__(self, config: ExclusionConfig = None) -> None:
        """Initialize the explicit rules filter.
        
        Args:
            config: Optional exclusion configuration for custom patterns
        """
        self.config = config or ExclusionConfig()
        
        # Initialize mutable sets for custom patterns
        self._custom_dirs: Set[str] = set()
        self._custom_file_patterns: Set[str] = set()
        self._custom_file_globs: List[re.Pattern] = []
        self._override_patterns: Set[str] = set()
        
        # Add custom patterns from config
        if self.config.custom_dirs:
            self._custom_dirs.update(self.config.custom_dirs)
        if self.config.custom_patterns:
            for pattern in self.config.custom_patterns:
                # Treat as glob pattern
                try:
                    regex = self._glob_to_regex(pattern)
                    self._custom_file_globs.append(re.compile(regex))
                except re.error:
                    # If regex compilation fails, treat as exact match
                    self._custom_file_patterns.add(pattern)
        if self.config.override_patterns:
            self._override_patterns.update(self.config.override_patterns)
    
    def _glob_to_regex(self, pattern: str) -> str:
        """Convert a glob pattern to a regex pattern.
        
        Args:
            pattern: Glob pattern (e.g., *.pyc, test_*.py)
            
        Returns:
            Regex pattern string
        """
        # Escape special regex characters except glob wildcards
        result = ''
        i = 0
        while i < len(pattern):
            c = pattern[i]
            if c == '*':
                result += '.*'
            elif c == '?':
                result += '.'
            elif c in '.^$+{}[]|()\\':
                result += '\\' + c
            else:
                result += c
            i += 1
        return result + '$'
    
    def add_directory_pattern(self, pattern: str) -> None:
        """Add a directory pattern to exclude.
        
        Args:
            pattern: Directory name to exclude
        """
        self._custom_dirs.add(pattern)
    
    def add_file_pattern(self, pattern: str) -> None:
        """Add a file pattern to exclude.
        
        Args:
            pattern: File pattern (glob syntax)
        """
        try:
            regex = self._glob_to_regex(pattern)
            self._custom_file_globs.append(re.compile(regex))
        except re.error:
            self._custom_file_patterns.add(pattern)
    
    def add_override_pattern(self, pattern: str) -> None:
        """Add an override pattern to never exclude.
        
        Args:
            pattern: Pattern to never exclude
        """
        self._override_patterns.add(pattern)
    
    def should_exclude(self, path: str, is_directory: bool) -> ExclusionResult:
        """Check if a path should be excluded based on explicit rules.
        
        This method performs O(1) set lookups for optimal performance.
        
        Args:
            path: Path to check (absolute or relative)
            is_directory: True if path is a directory
            
        Returns:
            ExclusionResult with exclusion decision
        """
        # Get the basename for matching
        name = os.path.basename(path)
        
        # Check override patterns first (never exclude these)
        if self._is_override(path, name):
            return ExclusionResult(
                excluded=False,
                tier=ExclusionTier.EXPLICIT,
                reason="Override pattern match",
            )
        
        # Directory matching
        if is_directory:
            # Check built-in directory patterns
            if name in self.DIRECTORY_PATTERNS:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason=f"Explicit directory rule: {name}",
                )
            
            # Check custom directory patterns
            if name in self._custom_dirs:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason=f"Custom directory rule: {name}",
                )
        else:
            # File matching - exact match
            if name in self.FILE_PATTERNS:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason=f"Explicit file rule: {name}",
                )
            
            # Check custom exact file patterns
            if name in self._custom_file_patterns:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason=f"Custom file rule: {name}",
                )
            
            # Pattern matching for glob-like rules
            for pattern in self.FILE_GLOBS:
                if pattern.match(name):
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.EXPLICIT,
                        reason=f"Explicit pattern match: {pattern.pattern}",
                    )
            
            # Check custom glob patterns
            for pattern in self._custom_file_globs:
                if pattern.match(name):
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.EXPLICIT,
                        reason=f"Custom pattern match: {pattern.pattern}",
                    )
        
        # Check if any parent directory matches
        parts = Path(path).parts
        for part in parts[:-1]:  # Exclude the last part (self)
            if part in self.DIRECTORY_PATTERNS or part in self._custom_dirs:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason=f"Parent directory excluded: {part}",
                )
        
        return ExclusionResult(
            excluded=False,
            tier=ExclusionTier.EXPLICIT,
        )
    
    def _is_override(self, path: str, name: str) -> bool:
        """Check if path matches an override pattern.
        
        Args:
            path: Full path to check
            name: Basename of the path
            
        Returns:
            True if path should never be excluded
        """
        # Check exact name match
        if name in self._override_patterns:
            return True
        
        # Check path patterns
        for pattern in self._override_patterns:
            if pattern in path or pattern == name:
                return True
        
        return False
    
    def reset(self) -> None:
        """Reset any cached state for a new traversal.
        
        Note: Explicit rules filter has no state to reset,
        but the method is provided for protocol compliance.
        """
        pass
