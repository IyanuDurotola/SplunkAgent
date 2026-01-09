"""Custom exceptions for SplunkProcessor."""

class SplunkProcessorException(Exception):
    """Base exception for Splunk Processor."""
    pass

class AuthenticationError(SplunkProcessorException):
    """Authentication failed."""
    pass

class AuthorizationError(SplunkProcessorException):
    """Authorization failed."""
    pass

class SplunkQueryError(SplunkProcessorException):
    """Splunk query execution failed."""
    pass

class LLMGenerationError(SplunkProcessorException):
    """LLM query generation failed."""
    pass

class ValidationError(SplunkProcessorException):
    """Validation failed."""
    pass

