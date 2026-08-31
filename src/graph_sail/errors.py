"""Domain-specific exceptions with actionable messages."""


class GraphSailError(Exception):
    """Base exception for expected Graph Sail failures."""


class ValidationError(GraphSailError):
    """Raised when a graph document violates the input contract."""


class PlanningError(GraphSailError):
    """Raised when no feasible placement exists."""


class OutputError(GraphSailError):
    """Raised when report artifacts cannot be written."""
