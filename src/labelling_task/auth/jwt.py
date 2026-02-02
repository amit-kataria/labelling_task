from __future__ import annotations

from typing import Any

from jose import JWTError, jwt

from labelling_task.configs.settings import Settings
from labelling_task.errors import AuthError
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)


def decode_token(token: str, settings: Settings) -> dict[str, Any]:
    """
    Decode and validate JWT.

    Notes:
    - For this scaffold we support HS256 via shared secret.
    - If you need RS256 + JWKS, implement key fetch/caching from settings.jwt_jwks_url.
    """
    try:
        options = {"verify_aud": settings.jwt_audience is not None}
        return jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_alg],
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options=options,
        )
    except JWTError as e:
        log.info("JWT decode failed: %s", str(e))
        raise AuthError("invalid token") from e
