"""Tier 2: Gitignore Filter.

This module provides dynamic .gitignore parsing and matching following
standard git wildcard syntax. Supports nested .gitignore files, negation
patterns, and directory-specific rules.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .types import ExclusionResult, ExclusionTier


@dataclass
class GitignorePattern:
    """Represents a single parsed gitignore pattern.
    
    Attributes:
        original: The original pattern string from .gitignore
        base_path: The directory containing the .gitignore file
        negation: Whether this is a negation pattern (starts with !)
        directory_only: Whether this pattern only matches directories
        root_only: Whether this pattern is anchored to the gitignore root
        regex: The compiled regex pattern for matching
    """
    original: str
    base_path: str
    negation: bool = False
    directory_only: bool = False
    root_only: bool = False
    regex: Optional[re.Pattern] = None
    
    def __post_init__(self) -> None:
        """Parse and compile the pattern after initialization."""
        pattern = self.original
        
        # Handle negation
        if pattern.startswith('!'):
            self.negation = True
            pattern = pattern[1:]
        
        # Handle root-only patterns (anchored to gitignore directory)
        if pattern.startswith('/'):
            self.root_only = True
            pattern = pattern[1:]
        
        # Handle directory-only patterns
        if pattern.endswith('/'):
            self.directory_only = True
            pattern = pattern[:-1]
        
        # Handle trailing spaces (unless escaped)
        pattern = pattern.rstrip()
        if pattern.endswith('\\'):
            pattern = pattern[:-1] + ' '
        
        # Empty pattern after processing
        if not pattern:
            self.regex = None
            return
        
        # Convert gitignore pattern to regex
        regex_pattern = self._gitignore_to_regex(pattern)
        
        try:
            self.regex = re.compile(regex_pattern)
        except re.error:
            self.regex = None
    
    def _gitignore_to_regex(self, pattern: str) -> str:
        """Convert a gitignore pattern to a regex pattern.
        
        Args:
            pattern: The gitignore pattern (after preprocessing)
            
        Returns:
            A regex pattern string
        """
        result = []
        i = 0
        n = len(pattern)
        
        # If pattern starts with **, it can match anywhere
        if pattern.startswith('**'):
            # ** at start means match any prefix
            pass
        
        while i < n:
            c = pattern[i]
            
            if c == '\\' and i + 1 < n:
                # Escaped character
                next_char = pattern[i + 1]
                if next_char in '*?[]!':
                    result.append('\\' + next_char)
                else:
                    result.append(re.escape(next_char))
                i += 2
                continue
            
            if c == '*':
                if i + 1 < n and pattern[i + 1] == '*':
                    # Double star **
                    if i + 2 < n and pattern[i + 2] == '/':
                        # **/ - matches any directory prefix
                        if i == 0:
                            # At start: can match nothing or any prefix
                            result.append('(?:.*/)?')
                        else:
                            result.append('.*')
                        i += 3
                        continue
                    elif i > 0 and pattern[i - 1] == '/':
                        # /** - matches any suffix
                        result.append('.*')
                        i += 2
                        continue
                    else:
                        # ** in middle or standalone
                        result.append('.*')
                        i += 2
                        continue
                else:
                    # Single star - matches anything except /
                    result.append('[^/]*')
                    i += 1
                    continue
            
            if c == '?':
                # Single character wildcard (not /)
                result.append('[^/]')
                i += 1
                continue
            
            if c == '[':
                # Character class
                j = i + 1
                if j < n and pattern[j] == '!':
                    result.append('[^')
                    j += 1
                elif j < n and pattern[j] == ']':
                    # Empty class or ] as first char
                    result.append('\\[')
                    i += 1
                    continue
                else:
                    result.append('[')
                
                # Find closing bracket
                while j < n and pattern[j] != ']':
                    if pattern[j] == '\\' and j + 1 < n:
                        result.append('\\' + pattern[j + 1])
                        j += 2
                    else:
                        result.append(pattern[j])
                        j += 1
                
                if j < n:
                    result.append(']')
                    i = j + 1
                else:
                    # No closing bracket, treat [ as literal
                    result.pop()  # Remove the '[' we added
                    result.append('\\[')
                    i += 1
                continue
            
            # Escape regex special characters
            if c in '.^$+{}|()':
                result.append('\\' + c)
            else:
                result.append(c)
            
            i += 1
        
        # Build final pattern
        regex = ''.join(result)
        
        # Handle root-only patterns
        if self.root_only:
            regex = '^' + regex
        else:
            # Can match at any directory level
            regex = '(?:^|/)' + regex if not regex.startswith('(?:') else regex
        
        # Handle directory matching
        if self.directory_only:
            regex = regex + '(?:/|$)'
        else:
            regex = regex + '(?:/|$)?'
        
        return regex
    
    def matches(self, path: str, is_directory: bool) -> bool:
        """Check if this pattern matches the given path.
        
        Args:
            path: The path to check (absolute or relative)
            is_directory: Whether the path is a directory
            
        Returns:
            True if the pattern matches
        """
        if self.regex is None:
            return False
        
        # Directory-only patterns only match directories
        if self.directory_only and not is_directory:
            return False
        
        # Compute relative path from base
        try:
            rel_path = os.path.relpath(path, self.base_path)
            # Normalize path separators
            rel_path = rel_path.replace(os.sep, '/')
        except ValueError:
            # Different drives on Windows
            return False
        
        # For root-only patterns, match from the beginning
        if self.root_only:
            return bool(self.regex.match(rel_path))
        
        # For non-root patterns, can match anywhere
        # Try matching the full path
        if self.regex.search(rel_path):
            return True
        
        # Also try matching individual path components
        parts = rel_path.split('/')
        for i in range(len(parts)):
            subpath = '/'.join(parts[i:])
            if self.regex.search(subpath):
                return True
        
        return False


class GitignoreFilter:
    """Tier 2: Dynamic .gitignore parsing and matching.
    
    This filter parses .gitignore files and applies their patterns
    to determine if files should be excluded. Supports:
    - Nested .gitignore files in subdirectories
    - Negation patterns (!)
    - Directory-specific patterns (ending with /)
    - Root-only patterns (starting with /)
    - Glob patterns (*, **, ?, [])
    
    Attributes:
        root_path: The root directory for the project
        _gitignore_cache: Cache of parsed gitignore patterns
        _gitignore_locations: List of discovered .gitignore files
    """
    
    def __init__(self, root_path: str) -> None:
        """Initialize the gitignore filter.
        
        Args:
            root_path: The root directory for the project
        """
        self.root_path = os.path.abspath(root_path)
        self._gitignore_cache: Dict[str, List[GitignorePattern]] = {}
        self._gitignore_locations: List[str] = []
        self._result_cache: Dict[Tuple[str, bool], ExclusionResult] = {}
        self._initialized = False
    
    def _initialize(self) -> None:
        """Lazy initialization - discover gitignore files."""
        if self._initialized:
            return
        self._discover_gitignores()
        self._initialized = True
    
    def _discover_gitignores(self) -> None:
        """Find all .gitignore files in the project.
        
        Walks the directory tree to find all .gitignore files.
        This is done lazily on first use.
        """
        self._gitignore_locations = []
        
        try:
            for root, dirs, files in os.walk(self.root_path):
                # Skip common excluded directories for efficiency
                dirs[:] = [d for d in dirs if d not in {
                    '.git', '.svn', '.hg', 'node_modules', '__pycache__',
                    '.venv', 'venv', 'env', '.tox', '.nox', '.eggs',
                }]
                
                if '.gitignore' in files:
                    self._gitignore_locations.append(
                        os.path.join(root, '.gitignore')
                    )
        except (OSError, PermissionError):
            pass  # Handle permission errors gracefully
    
    def _parse_gitignore(self, gitignore_path: str) -> List[GitignorePattern]:
        """Parse a .gitignore file and return list of patterns.
        
        Args:
            gitignore_path: Path to the .gitignore file
            
        Returns:
            List of GitignorePattern objects
        """
        patterns = []
        base_path = os.path.dirname(gitignore_path)
        
        try:
            with open(gitignore_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.rstrip('\n\r')
                    
                    # Skip empty lines
                    if not line.strip():
                        continue
                    
                    # Skip comments (unless escaped)
                    if line.startswith('#'):
                        continue
                    
                    # Handle escaped comments
                    if line.startswith('\\#'):
                        line = '#' + line[1:]
                    
                    patterns.append(GitignorePattern(
                        original=line,
                        base_path=base_path,
                    ))
        except (IOError, OSError, UnicodeDecodeError):
            pass  # Skip unreadable gitignore files
        
        return patterns
    
    def _get_applicable_patterns(self, path: str) -> List[GitignorePattern]:
        """Get all gitignore patterns applicable to a path.
        
        Patterns are sorted by directory depth, with parent patterns
        applied before child patterns (so child patterns can override).
        
        Args:
            path: The path to get patterns for
            
        Returns:
            List of applicable GitignorePattern objects
        """
        self._initialize()
        
        applicable = []
        abs_path = os.path.abspath(path)
        
        for gitignore_path in self._gitignore_locations:
            gitignore_dir = os.path.dirname(gitignore_path)
            
            # Pattern applies if path is under gitignore directory
            try:
                rel = os.path.relpath(abs_path, gitignore_dir)
                # Path is under gitignore directory if it doesn't start with ..
                if not rel.startswith('..'):
                    if gitignore_path not in self._gitignore_cache:
                        self._gitignore_cache[gitignore_path] = self._parse_gitignore(gitignore_path)
                    applicable.extend(self._gitignore_cache[gitignore_path])
            except ValueError:
                # Different drives on Windows
                continue
        
        # Sort by directory depth (shallower = more general, applied first)
        applicable.sort(key=lambda p: p.base_path.count(os.sep))
        
        return applicable
    
    def should_exclude(self, path: str, is_directory: bool) -> ExclusionResult:
        """Check if path should be excluded based on gitignore rules.
        
        Args:
            path: Path to check
            is_directory: Whether the path is a directory
            
        Returns:
            ExclusionResult with exclusion decision
        """
        # Check result cache
        cache_key = (path, is_directory)
        if cache_key in self._result_cache:
            return self._result_cache[cache_key]
        
        patterns = self._get_applicable_patterns(path)
        
        excluded = False
        exclude_reason = None
        matched_pattern = None
        
        for pattern in patterns:
            if pattern.matches(path, is_directory):
                if pattern.negation:
                    # Negation pattern - re-include the file
                    excluded = False
                    exclude_reason = None
                    matched_pattern = None
                else:
                    excluded = True
                    exclude_reason = f"Gitignore pattern: {pattern.original}"
                    matched_pattern = pattern.original
        
        result = ExclusionResult(
            excluded=excluded,
            tier=ExclusionTier.GITIGNORE,
            reason=exclude_reason,
            metadata={'pattern': matched_pattern} if matched_pattern else {},
        )
        
        # Cache result
        self._result_cache[cache_key] = result
        
        return result
    
    def reset(self) -> None:
        """Clear cache for new traversal."""
        self._gitignore_cache.clear()
        self._result_cache.clear()
        self._gitignore_locations.clear()
        self._initialized = False
    
    def add_gitignore_path(self, path: str) -> None:
        """Add an additional gitignore file to parse.
        
        Args:
            path: Path to a .gitignore file
        """
        if os.path.isfile(path):
            self._gitignore_locations.append(path)
