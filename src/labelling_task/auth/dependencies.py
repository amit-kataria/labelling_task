from __future__ import annotations

from fastapi import Depends, Header

from labelling_task.auth.jwt import decode_token
from labelling_task.auth.models import Principal
from labelling_task.configs.settings import Settings, get_settings
from labelling_task.errors import AuthError
from labelling_task.configs.logging_config import get_logger
log = get_logger(__name__)

def _bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise AuthError("missing authorization header")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise AuthError("invalid authorization header")
    return parts[1].strip()


async def get_principal(
    authorization: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> Principal:
    token = _bearer_token(authorization)
    claims = decode_token(token, settings)

    tenant_id = claims.get("tenantId")
    role = claims.get("role")
    permissions = claims.get("permissions") or []
    user_id = claims.get("sub")

    if not tenant_id or not role or not user_id:
        raise AuthError("token missing required claims")
    if not isinstance(permissions, list):
        raise AuthError("invalid permissions claim")

    return Principal(
        user_id=str(user_id),
        tenant_id=str(tenant_id),
        role=str(role),
        permissions=tuple(str(p) for p in permissions),
    )
