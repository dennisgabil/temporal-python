import logging
from cryptography.fernet import Fernet, InvalidToken
from exceptions import CifMaskingError

logger = logging.getLogger(__name__)


class CifMasker:
    """
    Reversible CIF masking using symmetric encryption (Fernet).
    """

    def __init__(self, secret_key: str):
        if not secret_key:
            raise CifMaskingError("CIF_ENCRYPTION_KEY is not configured")
        self._fernet = Fernet(secret_key.encode())

    def mask(self, cif_code: str) -> str:
        try:
            return self._fernet.encrypt(cif_code.encode()).decode()
        except Exception as exc:
            logger.exception("Failed to mask CIF")
            raise CifMaskingError("CIF masking failed") from exc

    def unmask(self, masked_cif: str) -> str:
        try:
            return self._fernet.decrypt(masked_cif.encode()).decode()
        except InvalidToken as exc:
            raise CifMaskingError("Invalid CIF token") from exc
        except Exception as exc:
            logger.exception("Failed to unmask CIF")
            raise CifMaskingError("CIF unmasking failed") from exc
