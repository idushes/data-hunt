from google.auth.transport import requests as google_requests
from google.auth.exceptions import TransportError
from google.oauth2 import id_token

from config import GOOGLE_CLIENT_ID


class GoogleIdentityNotConfiguredError(RuntimeError):
    pass


class GoogleIdentityVerificationError(ValueError):
    pass


class GoogleIdentityUnavailableError(RuntimeError):
    pass


def verify_google_identity_token(credential: str) -> str:
    """Verify a Google ID token and return its stable account subject."""
    if not GOOGLE_CLIENT_ID:
        raise GoogleIdentityNotConfiguredError

    try:
        claims = id_token.verify_oauth2_token(
            credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except TransportError as exc:
        raise GoogleIdentityUnavailableError from exc
    except ValueError as exc:
        raise GoogleIdentityVerificationError from exc

    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise GoogleIdentityVerificationError
    return subject
