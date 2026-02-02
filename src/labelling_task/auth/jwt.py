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
        log.info("jwt.decode start alg=%s iss=%s aud=%s", settings.jwt_alg, settings.jwt_issuer, settings.jwt_audience)
        options = {"verify_aud": settings.jwt_audience is not None}
        claims = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_alg],
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options=options,
        )
        log.info("jwt.decode ok sub=%s tenantId=%s role=%s", claims.get("sub"), claims.get("tenantId"), claims.get("role"))
        return claims
    except JWTError as e:
        log.info("JWT decode failed: %s", str(e))
        raise AuthError("invalid token") from e
