"""Definitions of processing flows for PrimeScope."""

# Canonical list of processing steps used across the project.
STEPS = [
    "validate",
    "collect",
    "normalize",
    "interim",
    "checks",
    "report",
]

# Default pipeline sequence simply includes all known steps.
DEFAULT_FLOW = list(STEPS)

# Placeholder for future custom flows.
EXAMPLE_FLOW = DEFAULT_FLOW

