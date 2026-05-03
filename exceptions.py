class RevenueProcessingError(Exception):
    """Base exception for revenue file processing."""


class FileValidationError(RevenueProcessingError):
    """File validation failure."""


class CsvReadError(RevenueProcessingError):
    """CSV read / schema validation failure."""


class DatabaseError(RevenueProcessingError):
    """Database operation failure."""


class CsvWriteError(RevenueProcessingError):
    """CSV write failure."""


class FileUploadError(RevenueProcessingError):
    """File replacement / upload failure."""


class CifMaskingError(RevenueProcessingError):
    """CIF masking or unmasking failure."""
