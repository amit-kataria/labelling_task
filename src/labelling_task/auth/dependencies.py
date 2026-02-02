from __future__ import annotations

from fastapi import Depends

from labelling_task.auth.models import Principal
from labelling_task.auth.security import oauth2_scheme, validator
from labelling_task.errors import AuthError
from labelling_task.configs.logging_config import get_logger

log = get_logger(__name__)


async def get_principal(token: str = Depends(oauth2_scheme)) -> Principal:
    """
    Resolve the authenticated principal using the shared JWKS-based validator,
    mirroring the pattern used in user_management.
    """
    if not token:
        log.info("auth.missing_bearer_token")
        raise AuthError("missing authorization header")

    claims = await validator.verify_token(token)

    tenant_id = claims.get("tenantId")
    role = claims.get("role")
    permissions = claims.get("permissions") or []
    user_id = claims.get("sub")

    if not tenant_id or not role or not user_id:
        log.info(
            "auth.token_missing_claims has_tenant=%s has_role=%s has_sub=%s",
            bool(tenant_id),
            bool(role),
            bool(user_id),
        )
        raise AuthError("token missing required claims")
    if not isinstance(permissions, list):
        log.info("auth.invalid_permissions_claim type=%s", type(permissions).__name__)
        raise AuthError("invalid permissions claim")

    log.info(
        "auth.principal tenant_id=%s user_id=%s role=%s",
        str(tenant_id),
        str(user_id),
        str(role),
    )
    return Principal(
        user_id=str(user_id),
        tenant_id=str(tenant_id),
        role=str(role),
        permissions=tuple(str(p) for p in permissions),
    )
