"""Type definitions and protocols for the exclusion system.

This module defines the core types, enums, and protocol interfaces used
throughout the three-tier exclusion system.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Protocol


class ExclusionTier(Enum):
    """Enumeration of exclusion tiers in the three-tier system.
    
    Attributes:
        EXPLICIT: Tier 1 - Explicit rules for well-known artifacts
        GITIGNORE: Tier 2 - Dynamic .gitignore parsing
        HEURISTICS: Tier 3 - Heuristic content analysis
    """
    EXPLICIT = 1
    GITIGNORE = 2
    HEURISTICS = 3


@dataclass
class ExclusionResult:
    """Result of an exclusion check.
    
    Attributes:
        excluded: Whether the path should be excluded
        tier: Which tier made the exclusion decision (if any)
        reason: Human-readable explanation for the decision
        metadata: Additional information about the exclusion
    """
    excluded: bool
    tier: Optional[ExclusionTier] = None
    reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __bool__(self) -> bool:
        """Allow using ExclusionResult in boolean context."""
        return self.excluded


class ExclusionFilterProtocol(Protocol):
    """Protocol defining the interface for exclusion filter implementations.
    
    All tier filters must implement this protocol to ensure consistent
    behavior across the exclusion system.
    """
    
    def should_exclude(self, path: str, is_directory: bool) -> ExclusionResult:
        """Check if a path should be excluded.
        
        Args:
            path: Absolute or relative path to check
            is_directory: True if path is a directory
            
        Returns:
            ExclusionResult with exclusion decision and metadata
        """
        ...
    
    def reset(self) -> None:
        """Reset any cached state for a new traversal.
        
        This method should clear any internal caches or state that
        should not persist between different directory traversals.
        """
        ...
