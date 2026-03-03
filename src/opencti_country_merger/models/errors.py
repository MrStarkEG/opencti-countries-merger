"""Error hierarchy for the OpenCTI Country Merger."""


class MergeError(Exception):
    """Base exception for all merge-related errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class EntityNotFoundError(MergeError):
    """Raised when an expected entity cannot be found in Elasticsearch."""

    def __init__(self, entity_id: str, index: str | None = None) -> None:
        self.entity_id = entity_id
        self.index = index
        location = f" in index {index}" if index else ""
        super().__init__(f"Entity {entity_id!r} not found{location}")


class TypeMismatchError(MergeError):
    """Raised when entities being merged have incompatible types."""

    def __init__(self, source_type: str, target_type: str) -> None:
        self.source_type = source_type
        self.target_type = target_type
        super().__init__(
            f"Type mismatch: cannot merge {source_type!r} into {target_type!r}"
        )


class PhaseFailedError(MergeError):
    """Raised when a specific merge phase fails."""

    def __init__(self, phase: int, description: str, cause: Exception | None = None) -> None:
        self.phase = phase
        self.description = description
        self.cause = cause
        super().__init__(f"Phase {phase} ({description}) failed: {cause or 'unknown'}")


class ElasticsearchError(MergeError):
    """Raised when an Elasticsearch operation fails unexpectedly."""

    def __init__(self, operation: str, cause: Exception | None = None) -> None:
        self.operation = operation
        self.cause = cause
        super().__init__(f"Elasticsearch error during {operation}: {cause or 'unknown'}")


class DiscoveryError(MergeError):
    """Raised when country entity discovery fails."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Discovery failed: {message}")
