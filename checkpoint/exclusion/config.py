"""Configuration dataclasses for the exclusion system.

This module defines configuration options for each tier of the exclusion
system and the overall ExclusionConfig that combines them.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ExclusionConfig:
    """Configuration for the three-tier exclusion system.
    
    This dataclass holds all configuration options for controlling
    the behavior of each exclusion tier.
    
    Attributes:
        enable_explicit: Whether Tier 1 (explicit rules) is enabled
        enable_gitignore: Whether Tier 2 (gitignore parsing) is enabled
        enable_heuristics: Whether Tier 3 (heuristic analysis) is enabled
        max_file_size: Maximum file size in bytes (default 10MB)
        custom_patterns: Additional file patterns to exclude (glob syntax)
        custom_dirs: Additional directory names to exclude
        override_patterns: Patterns to never exclude (negation patterns)
        detect_binary: Whether to detect and exclude binary files
        detect_minified: Whether to detect and exclude minified files
        max_avg_line_length: Threshold for minified file detection
    """
    # Tier toggles
    enable_explicit: bool = True
    enable_gitignore: bool = True
    enable_heuristics: bool = True
    
    # Tier 3: File size threshold (default 10MB)
    max_file_size: int = 10 * 1024 * 1024
    
    # Custom exclusion patterns
    custom_patterns: List[str] = field(default_factory=list)
    custom_dirs: List[str] = field(default_factory=list)
    
    # Override patterns (never exclude these)
    override_patterns: List[str] = field(default_factory=list)
    
    # Tier 3: Heuristic options
    detect_binary: bool = True
    detect_minified: bool = True
    max_avg_line_length: int = 500
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.max_file_size <= 0:
            raise ValueError("max_file_size must be positive")
        if self.max_avg_line_length <= 0:
            raise ValueError("max_avg_line_length must be positive")
    
    @classmethod
    def from_cli_args(
        cls,
        no_explicit: bool = False,
        no_gitignore: bool = False,
        no_heuristics: bool = False,
        max_file_size_mb: int = 10,
        ignore_dirs: List[str] = None,
    ) -> "ExclusionConfig":
        """Create configuration from CLI arguments.
        
        Args:
            no_explicit: Disable Tier 1 explicit rules
            no_gitignore: Disable Tier 2 gitignore parsing
            no_heuristics: Disable Tier 3 heuristic analysis
            max_file_size_mb: Maximum file size in megabytes
            ignore_dirs: Additional directories to ignore
            
        Returns:
            Configured ExclusionConfig instance
        """
        return cls(
            enable_explicit=not no_explicit,
            enable_gitignore=not no_gitignore,
            enable_heuristics=not no_heuristics,
            max_file_size=max_file_size_mb * 1024 * 1024,
            custom_dirs=ignore_dirs or [],
        )
