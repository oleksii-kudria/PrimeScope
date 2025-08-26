"""Definitions of processing flows for PrimeScope."""

# Default pipeline sequence.
DEFAULT_FLOW = [
    "collect",
    "validate",
    "normalize",
    "interim",
    "checks",
    "report",
]

# Placeholder for future custom flows.
EXAMPLE_FLOW = DEFAULT_FLOW

