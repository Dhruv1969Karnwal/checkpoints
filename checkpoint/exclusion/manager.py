"""Exclusion Manager - Central coordinator for the three-tier exclusion system.

This module provides the ExclusionManager class that orchestrates all three
tiers of the exclusion system in priority order, stopping at the first tier
that excludes a file (cascade pattern).
"""

import fnmatch
import os
from typing import Any, Dict, Optional

from .config import ExclusionConfig
from .explicit import ExplicitRulesFilter
from .gitignore import GitignoreFilter
from .heuristics import HeuristicsFilter
from .types import ExclusionResult, ExclusionTier


class ExclusionManager:
    """Central coordinator for the three-tier exclusion system.
    
    The manager orchestrates exclusion checks through three tiers:
    1. Explicit Rules - Fast, deterministic exclusion of well-known artifacts
    2. Gitignore Parsing - Project-specific patterns from .gitignore files
    3. Heuristic Analysis - Content-based detection of binary/large/minified files
    
    Tiers are executed in order, and the first tier that excludes a file
    determines the result (cascade pattern).
    
    Attributes:
        root_path: The root directory for the project
        config: Exclusion configuration
        stats: Statistics about exclusions during traversal
    """
    
    def __init__(
        self,
        root_path: str,
        config: ExclusionConfig = None,
    ) -> None:
        """Initialize the exclusion manager.
        
        Args:
            root_path: The root directory for the project
            config: Optional exclusion configuration
        """
        self.root_path = os.path.abspath(root_path)
        self.config = config or ExclusionConfig()
        
        # Initialize tier filters
        self._explicit_filter = ExplicitRulesFilter(self.config)
        self._gitignore_filter: Optional[GitignoreFilter] = None
        self._heuristics_filter: Optional[HeuristicsFilter] = None
        
        # Lazy initialization of gitignore filter
        if self.config.enable_gitignore:
            self._gitignore_filter = GitignoreFilter(self.root_path)
        
        # Initialize heuristics filter
        if self.config.enable_heuristics:
            self._heuristics_filter = HeuristicsFilter(self.config)
        
        # Statistics tracking
        self._stats: Dict[str, int] = {
            'total_checked': 0,
            'tier1_exclusions': 0,
            'tier2_exclusions': 0,
            'tier3_exclusions': 0,
            'included': 0,
            'errors': 0,
        }
    
    def should_exclude(self, path: str, is_directory: bool) -> ExclusionResult:
        """Check if a path should be excluded through all tiers.
        
        Executes tiers in priority order and stops at the first tier
        that excludes the file.
        
        Args:
            path: Path to check (absolute or relative to root)
            is_directory: Whether the path is a directory
            
        Returns:
            ExclusionResult with exclusion decision
        """
        self._stats['total_checked'] += 1
        
        # Normalize path
        if not os.path.isabs(path):
            path = os.path.join(self.root_path, path)
        
        # Check for symlink pointing outside root
        if os.path.islink(path):
            try:
                target = os.path.realpath(path)
                # Use commonpath to avoid prefix confusion (e.g., /repo vs /repo2)
                if os.path.commonpath([self.root_path, target]) != self.root_path:
                    self._stats['tier1_exclusions'] += 1
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.EXPLICIT,
                        reason="Symlink points outside project",
                    )
            except OSError:
                # Broken symlink
                self._stats['errors'] += 1
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.EXPLICIT,
                    reason="Broken symlink",
                )
        
        # Check override patterns first (never exclude these)
        if self._is_override(path):
            self._stats['included'] += 1
            return ExclusionResult(
                excluded=False,
                tier=None,
                reason="Override pattern match",
            )
        
        # Tier 1: Explicit Rules
        if self.config.enable_explicit:
            result = self._explicit_filter.should_exclude(path, is_directory)
            if result.excluded:
                self._stats['tier1_exclusions'] += 1
                return result
        
        # Tier 2: Gitignore Parsing
        if self._gitignore_filter is not None:
            result = self._gitignore_filter.should_exclude(path, is_directory)
            if result.excluded:
                self._stats['tier2_exclusions'] += 1
                return result
        
        # Tier 3: Heuristic Analysis
        if self._heuristics_filter is not None:
            try:
                result = self._heuristics_filter.should_exclude(path, is_directory)
                if result.excluded:
                    self._stats['tier3_exclusions'] += 1
                    return result
            except Exception:
                # Handle any unexpected errors in heuristic analysis
                self._stats['errors'] += 1
        
        self._stats['included'] += 1
        return ExclusionResult(
            excluded=False,
            tier=None,
        )
    
    def _is_override(self, path: str) -> bool:
        """Check if path matches an override pattern (should never be excluded).
        
        Args:
            path: Path to check
            
        Returns:
            True if path matches an override pattern
        """
        name = os.path.basename(path)
        
        for pattern in self.config.override_patterns:
            # Check exact match
            if pattern == name:
                return True
            
            # Check glob pattern
            if fnmatch.fnmatch(name, pattern):
                return True
            
            # Check if pattern is in path
            if pattern in path:
                return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about exclusions during traversal.
        
        Returns:
            Dictionary with exclusion statistics
        """
        return self._stats.copy()
    
    def reset(self) -> None:
        """Reset for a new traversal.
        
        Clears all cached state and statistics.
        """
        self._explicit_filter.reset()
        
        if self._gitignore_filter is not None:
            self._gitignore_filter.reset()
        
        if self._heuristics_filter is not None:
            self._heuristics_filter.reset()
        
        # Reset statistics
        self._stats = {
            'total_checked': 0,
            'tier1_exclusions': 0,
            'tier2_exclusions': 0,
            'tier3_exclusions': 0,
            'included': 0,
            'errors': 0,
        }
    
    def add_explicit_directory(self, pattern: str) -> None:
        """Add a directory pattern to explicit exclusions.
        
        Args:
            pattern: Directory name to exclude
        """
        self._explicit_filter.add_directory_pattern(pattern)
    
    def add_explicit_file(self, pattern: str) -> None:
        """Add a file pattern to explicit exclusions.
        
        Args:
            pattern: File pattern (glob syntax) to exclude
        """
        self._explicit_filter.add_file_pattern(pattern)
    
    def add_override(self, pattern: str) -> None:
        """Add an override pattern (never exclude).
        
        Args:
            pattern: Pattern to never exclude
        """
        self._explicit_filter.add_override_pattern(pattern)


def walk_with_exclusions(
    root_path: str,
    config: ExclusionConfig = None,
) -> "Generator[tuple[str, list[str], list[str]], None, None]":
    """Walk directory tree with three-tier exclusion filtering.
    
    This is a generator function that yields tuples of (root, directories, files)
    similar to os.walk, but with excluded items filtered out.
    
    Args:
        root_path: The root directory to walk
        config: Optional exclusion configuration
        
    Yields:
        Tuples of (root, directories, files) with excluded items filtered
    """
    manager = ExclusionManager(root_path, config)
    
    for root, dirs, files in os.walk(root_path):
        # Filter directories (modifies in-place for os.walk)
        dirs[:] = [
            d for d in dirs
            if not manager.should_exclude(os.path.join(root, d), is_directory=True).excluded
        ]
        
        # Filter files
        filtered_files = [
            f for f in files
            if not manager.should_exclude(os.path.join(root, f), is_directory=False).excluded
        ]
        
        yield root, dirs, filtered_files
    
    # Log statistics if needed
    stats = manager.get_stats()
    if stats['total_checked'] > 0:
        pass  # Statistics available via get_stats()