"""Tier 3: Heuristic Analysis Filter.

This module provides intelligent content-based analysis to detect files
that should be excluded based on their characteristics rather than
explicit rules. Includes file size checks, binary detection, and
minified file detection.



"""

import os
from pathlib import Path
from typing import FrozenSet, Optional

from .config import ExclusionConfig
from .types import ExclusionResult, ExclusionTier


# Common text file extensions for heuristic analysis
TEXT_EXTENSIONS: FrozenSet[str] = frozenset({
    # Programming languages
    '.py', '.pyw', '.pyx', '.pxd', '.pyi',
    '.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs',
    '.java', '.kt', '.kts', '.scala', '.groovy',
    '.c', '.cpp', '.cc', '.cxx', '.h', '.hpp', '.hxx',
    '.cs', '.vb', '.fs', '.fsi', '.fsx',
    '.go', '.rs', '.dart', '.swift', '.kt',
    '.rb', '.rake', '.gemspec',
    '.php', '.phtml', '.php3', '.php4', '.php5',
    '.pl', '.pm', '.t', '.pod',
    '.lua', '.r', '.rmd', '.jl',
    '.sh', '.bash', '.zsh', '.fish', '.ksh',
    '.ps1', '.psm1', '.psd1',
    '.bat', '.cmd',
    # Markup and data
    '.html', '.htm', '.xhtml', '.xml', '.xsl', '.xslt',
    '.css', '.scss', '.sass', '.less', '.styl',
    '.json', '.json5', '.jsonc', '.yaml', '.yml', '.toml',
    '.md', '.markdown', '.mdown', '.mkd',
    '.rst', '.adoc', '.asciidoc', '.tex', '.latex',
    '.svg', '.mml',
    # Config files
    '.ini', '.cfg', '.conf', '.config', '.properties',
    '.env', '.editorconfig', '.gitattributes',
    '.dockerfile', '.containerfile',
    # Documentation
    '.txt', '.text', '.log', '.readme', '.license',
    # Shell scripts
    '.bash', '.zsh', '.fish',
})

# JSON/XML extensions for size thresholds
JSON_XML_EXTENSIONS: FrozenSet[str] = frozenset({'.json', '.json5', '.jsonc', '.xml'})


class HeuristicsFilter:
    """Tier 3: Composite heuristic analysis for content-based exclusion.
    
    This filter applies multiple heuristics in order of computational cost:
    1. File size check (cheapest - just a stat call)
    2. Binary detection (reads file header)
    3. Line density analysis (reads and parses content)
    
    Attributes:
        config: Exclusion configuration
    """
    
    # Known binary file signatures (magic bytes)
    BINARY_SIGNATURES = {
        b'\x89PNG\r\n\x1a\n': 'PNG image',
        b'\x89PNG': 'PNG image',
        b'GIF87a': 'GIF image',
        b'GIF89a': 'GIF image',
        b'\xff\xd8\xff': 'JPEG image',
        b'PK\x03\x04': 'ZIP archive',
        b'PK\x05\x06': 'ZIP archive (empty)',
        b'PK\x07\x08': 'ZIP archive (spanned)',
        b'Rar!\x1a\x07': 'RAR archive',
        b'\x1f\x8b': 'GZIP archive',
        b'BZh': 'BZIP2 archive',
        b'\x00\x00\x01\x00': 'ICO image',
        b'MZ': 'Windows executable',
        b'\x7fELF': 'Linux executable',
        b'%PDF': 'PDF document',
        b'SQLite': 'SQLite database',
        b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1': 'MS Office document',
        b'PK': 'Office Open XML',
        b'\x25\x50\x44\x46': 'PDF document',
        b'\x00\x00\x00': 'MP4/MOV video',
        b'ftyp': 'MP4/MOV video',
        b'OggS': 'OGG audio',
        b'ID3': 'MP3 audio',
        b'\xff\xfb': 'MP3 audio',
        b'\xff\xfa': 'MP3 audio',
        b'fLaC': 'FLAC audio',
        b'RIFF': 'WAV audio',
        b'WAVE': 'WAV audio',
    }
    
    # Sample size for binary detection
    SAMPLE_SIZE = 8192
    
    # Null byte threshold for binary detection
    NULL_BYTE_THRESHOLD = 0.001  # 0.1%
    
    def __init__(self, config: ExclusionConfig = None) -> None:
        """Initialize the heuristics filter.
        
        Args:
            config: Optional exclusion configuration
        """
        self.config = config or ExclusionConfig()
    
    def should_exclude(self, path: str, is_directory: bool) -> ExclusionResult:
        """Apply all heuristics in order of cost.
        
        Args:
            path: Path to check
            is_directory: Whether the path is a directory
            
        Returns:
            ExclusionResult with exclusion decision
        """
        # Directories are not subject to heuristic analysis
        if is_directory:
            return ExclusionResult(
                excluded=False,
                tier=ExclusionTier.HEURISTICS,
            )
        
        # Check if file exists
        if not os.path.exists(path):
            return ExclusionResult(
                excluded=True,
                tier=ExclusionTier.HEURISTICS,
                reason="File no longer exists",
            )
        
        # 1. Size check (cheapest)
        result = self._check_file_size(path)
        if result.excluded:
            return result
        
        # 2. Binary detection (reads file header)
        if self.config.detect_binary:
            result = self._check_binary(path)
            if result.excluded:
                return result
        
        # 3. Line density analysis (most expensive)
        if self.config.detect_minified:
            result = self._check_line_density(path)
            if result.excluded:
                return result
        
        return ExclusionResult(
            excluded=False,
            tier=ExclusionTier.HEURISTICS,
        )
    
    def _check_file_size(self, path: str) -> ExclusionResult:
        """Check if file exceeds size threshold.
        
        Args:
            path: Path to check
            
        Returns:
            ExclusionResult with size check result
        """
        try:
            size = os.path.getsize(path)
            extension = Path(path).suffix.lower()
            
            # Determine threshold based on file type
            if extension in JSON_XML_EXTENSIONS:
                max_size = min(2 * 1024 * 1024, self.config.max_file_size)
            elif extension in TEXT_EXTENSIONS:
                max_size = min(5 * 1024 * 1024, self.config.max_file_size)
            else:
                max_size = self.config.max_file_size
            
            if size > max_size:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.HEURISTICS,
                    reason=f"File size {self._format_size(size)} exceeds threshold {self._format_size(max_size)}",
                    metadata={'size': size, 'threshold': max_size},
                )
        except OSError:
            pass  # Skip files we can't stat
        
        return ExclusionResult(
            excluded=False,
            tier=ExclusionTier.HEURISTICS,
        )
    
    def _check_binary(self, path: str) -> ExclusionResult:
        """Check if file is binary using multiple detection methods.
        
        Detection methods:
        1. Magic bytes detection (file signatures)
        2. Null byte detection
        3. Encoding detection
        
        Args:
            path: Path to check
            
        Returns:
            ExclusionResult with binary check result
        """
        try:
            with open(path, 'rb') as f:
                sample = f.read(self.SAMPLE_SIZE)
            
            if not sample:
                # Empty file - not binary
                return ExclusionResult(
                    excluded=False,
                    tier=ExclusionTier.HEURISTICS,
                )
            
            # Check for magic bytes
            for signature, file_type in self.BINARY_SIGNATURES.items():
                if sample.startswith(signature):
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.HEURISTICS,
                        reason=f"Binary file detected: {file_type}",
                        metadata={'type': file_type, 'method': 'magic_bytes'},
                    )
            
            # Check for null bytes (strong indicator of binary)
            null_count = sample.count(b'\x00')
            null_ratio = null_count / len(sample)
            if null_ratio > self.NULL_BYTE_THRESHOLD:
                return ExclusionResult(
                    excluded=True,
                    tier=ExclusionTier.HEURISTICS,
                    reason=f"Binary file detected: null bytes found ({null_ratio:.2%})",
                    metadata={'method': 'null_bytes', 'null_ratio': null_ratio},
                )
            
            # Try to decode as text
            try:
                sample.decode('utf-8')
            except UnicodeDecodeError:
                # Try other common encodings
                for encoding in ['latin-1', 'utf-16', 'utf-16-le', 'utf-16-be', 'cp1252']:
                    try:
                        sample.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.HEURISTICS,
                        reason="Binary file detected: not decodable as text",
                        metadata={'method': 'encoding_check'},
                    )
        
        except (IOError, OSError, PermissionError):
            # Can't read file - include by default (conservative)
            return ExclusionResult(
                excluded=False,
                tier=ExclusionTier.HEURISTICS,
                reason="Permission denied - included by default",
                metadata={'error': 'PermissionError'},
            )
        
        return ExclusionResult(
            excluded=False,
            tier=ExclusionTier.HEURISTICS,
        )
    
    def _check_line_density(self, path: str) -> ExclusionResult:
        """Check if file appears to be minified based on line density.
        
        Detection criteria:
        - Average line length > threshold (default 500 chars)
        - Very high non-whitespace ratio
        
        Args:
            path: Path to check
            
        Returns:
            ExclusionResult with line density check result
        """
        extension = Path(path).suffix.lower()
        
        # Only check text files
        if extension not in TEXT_EXTENSIONS:
            return ExclusionResult(
                excluded=False,
                tier=ExclusionTier.HEURISTICS,
            )
        
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = []
                total_chars = 0
                max_line_length = 0
                total_whitespace = 0
                sample_size = 100  # Number of lines to sample
                
                for i, line in enumerate(f):
                    if i >= sample_size:
                        break
                    
                    lines.append(line)
                    line_len = len(line)
                    total_chars += line_len
                    max_line_length = max(max_line_length, line_len)
                    total_whitespace += sum(1 for c in line if c.isspace())
                
                if not lines:
                    return ExclusionResult(
                        excluded=False,
                        tier=ExclusionTier.HEURISTICS,
                    )
                
                # Calculate metrics
                avg_line_length = total_chars / len(lines)
                non_ws_ratio = 1 - (total_whitespace / total_chars) if total_chars > 0 else 0
                
                # Check thresholds
                reasons = []
                
                if avg_line_length > self.config.max_avg_line_length:
                    reasons.append(
                        f"Average line length {avg_line_length:.0f} > {self.config.max_avg_line_length}"
                    )
                
                if max_line_length > 10000:
                    reasons.append(f"Max line length {max_line_length} > 10000")
                
                if non_ws_ratio > 0.95 and total_chars > 1000:
                    reasons.append(f"Non-whitespace ratio {non_ws_ratio:.2%} > 95%")
                
                if reasons:
                    return ExclusionResult(
                        excluded=True,
                        tier=ExclusionTier.HEURISTICS,
                        reason=f"Minified file detected: {'; '.join(reasons)}",
                        metadata={
                            'avg_line_length': avg_line_length,
                            'max_line_length': max_line_length,
                            'non_ws_ratio': non_ws_ratio,
                        },
                    )
        
        except (IOError, OSError, PermissionError):
            pass  # Skip files we can't read
        
        return ExclusionResult(
            excluded=False,
            tier=ExclusionTier.HEURISTICS,
        )
    
    def _format_size(self, size: int) -> str:
        """Format file size in human-readable format.
        
        Args:
            size: Size in bytes
            
        Returns:
            Human-readable size string
        """
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"
    
    def reset(self) -> None:
        """Reset any cached state for a new traversal.
        
        Note: Heuristics filter has no state to reset,
        but the method is provided for protocol compliance.
        """
        pass
