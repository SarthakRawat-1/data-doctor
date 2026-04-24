"""Global exceptions for Data Doctor."""
from fastapi import HTTPException, status


class DataDoctorException(Exception):
    """Base exception for Data Doctor."""
    pass


class OpenMetadataConnectionError(DataDoctorException):
    """Raised when connection to OpenMetadata fails."""
    pass


class EntityNotFoundError(DataDoctorException):
    """Raised when an entity is not found in OpenMetadata."""
    pass


class LineageNotFoundError(DataDoctorException):
    """Raised when lineage data is not available."""
    pass


class InvalidFQNError(DataDoctorException):
    """Raised when an invalid FQN is provided."""
    pass


# HTTP Exceptions for FastAPI
def entity_not_found_http(fqn: str) -> HTTPException:
    """Return HTTP 404 exception for entity not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Entity with FQN '{fqn}' not found in OpenMetadata"
    )


def openmetadata_connection_error_http() -> HTTPException:
    """Return HTTP 503 exception for OpenMetadata connection error."""
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Unable to connect to OpenMetadata. Please check configuration."
    )


def invalid_fqn_http(fqn: str) -> HTTPException:
    """Return HTTP 400 exception for invalid FQN."""
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid Fully Qualified Name: '{fqn}'"
    )
