from typing import Optional


class ProjectError(Exception):
    """
    Base exception for all project-specific errors.
    """

    def __init__(
        self,
        message: str,
        *,
        cause: Optional[Exception] = None,
    ) -> None:
        self.message = message
        self.cause = cause
        super().__init__(message)

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} | Caused by: {repr(self.cause)}"
        return self.message


# Configuration errors
class ConfigError(ProjectError):
    """Raised when configuration loading or validation fails."""


# API / Ingestion errors

class APIError(ProjectError):
    """Raised when an API request fails."""


class APITimeoutError(APIError):
    """Raised when an API request times out."""


class APIResponseError(APIError):
    """Raised when an API returns an invalid or unexpected response."""


# Parsing / Transformation errors

class ParsingError(ProjectError):
    """Raised when parsing input data fails."""


class XMLParsingError(ParsingError):
    """Raised when XML parsing fails."""


# Spark-related errors

class SparkJobError(ProjectError):
    """Raised when a Spark job fails."""


class SparkSessionError(SparkJobError):
    """Raised when Spark session cannot be created."""


class DataWriteError(SparkJobError):
    """Raised when writing data fails."""
