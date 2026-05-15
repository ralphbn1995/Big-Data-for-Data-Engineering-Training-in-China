#!/usr/bin/env python3
"""Shared utilities for Session 4 data lake scripts."""


def fmt_bytes(n: int) -> str:
    """Format a byte count as a human-readable string (B, KB, MB, GB, TB)."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
