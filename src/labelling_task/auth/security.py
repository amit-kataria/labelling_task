from labelling_task.repositories.redis_client import redis_client
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError, ExpiredSignatureError
import time
import threading
import requests
from labelling_task.configs.logging_config import get_logger
from labelling_task.configs.settings import get_settings

log = get_logger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

ALLOWED_ALGORITHMS = ["RS256"]
CLOCK_SKEW_SECONDS = 60  # industry standard (1 min)
JWKS_CACHE_TTL = 300  # 5 minutes


class JWKSCache:
    def __init__(self, jwks_url: str):
        self.jwks_url = jwks_url
        self.keys = {}
        self.last_refresh = 0
        self.lock = threading.Lock()

    def _fetch(self):
        log.info("Refreshing JWKS")
        response = requests.get(self.jwks_url, timeout=5)
        response.raise_for_status()
        keys = response.json().get("keys", [])
        self.keys = {k["kid"]: k for k in keys}
        self.last_refresh = time.time()
        log.info(f"JWKS loaded: {len(self.keys)} keys")

    def get_key(self, kid: str):
        with self.lock:
            now = time.time()
            if now - self.last_refresh > JWKS_CACHE_TTL:
                self._fetch()
            return self.keys.get(kid)

    def refresh_forever(self):
        while True:
            try:
                with self.lock:
                    self._fetch()
            except Exception as e:
                log.error(f"JWKS refresh failed: {e}")
            time.sleep(JWKS_CACHE_TTL)

    def start_jwks_refresh(self):
        thread = threading.Thread(target=self.refresh_forever, name="jwks-refresh", daemon=True)
        thread.start()


class JWTValidator:
    def __init__(self):
        settings = get_settings()
        self.jwks_url = settings.JWKS_URL
        self.jwks_keys = {}

    def fetch_jwks(self):
        try:
            log.info(f"Fetching JWKS from {self.jwks_url}")
            response = requests.get(self.jwks_url, timeout=5)
            response.raise_for_status()
            keys = response.json().get("keys", [])
            self.jwks_keys = {key["kid"]: key for key in keys}
            log.info(f"Loaded {len(self.jwks_keys)} JWKS keys")
        except Exception as e:
            log.error(f"Failed to fetch JWKS: {e}")
            # Do not raise here, allow retry lazily if needed, or keep old keys

    def get_key(self, kid: str):
        if kid not in self.jwks_keys:
            self.fetch_jwks()
        return self.jwks_keys.get(kid)

    async def verify_token(self, token: str):
        log.debug(f"verify_token: {token}")
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            headers = jwt.get_unverified_header(token)
            kid = headers.get("kid")
            if not kid:
                raise HTTPException(401, "Missing kid")

            key = jwks_cache.get_key(kid)
            if not key:
                raise HTTPException(401, "Unknown signing key")

            payload = jwt.decode(
                token,
                key,
                algorithms=ALLOWED_ALGORITHMS,
                options={
                    "require_exp": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_signature": True,
                    "verify_aud": False,
                    "verify_iss": False,
                    "leeway": CLOCK_SKEW_SECONDS,
                },
            )

            # Replay protection
            # await validate_jti(payload)
            log.debug(f"Token validated: {payload}")
            return payload

        except ExpiredSignatureError as e:
            log.warning("JWT expired, %s", str(e))
            raise HTTPException(401, "Token expired")

        except JWTError as e:
            log.warning("JWT invalid, %s", str(e))
            raise HTTPException(401, "Invalid token")

        except Exception as e:
            log.error(f"Token validation error: {e}", exc_info=True)
            raise HTTPException(401, "Token validation failed")

    async def validate_jti(payload: dict):
        jti = payload.get("jti")
        exp = payload.get("exp")

        if not jti:
            raise HTTPException(
                status_code=401,
                detail="Token missing jti",
            )

        if not exp:
            raise HTTPException(
                status_code=401,
                detail="Token missing exp",
            )

        ttl = exp - int(time.time())
        if ttl <= 0:
            raise HTTPException(
                status_code=401,
                detail="Token expired",
            )

        key = f"jwt:jti:{jti}"

        # SETNX = atomic replay protection
        is_new = await redis_client.setnx(key, "1")
        if not is_new:
            raise HTTPException(
                status_code=401,
                detail="Token replay detected",
            )

        await redis_client.client.expire(key, ttl)


_settings = get_settings()
jwks_cache = JWKSCache(_settings.JWKS_URL)
validator = JWTValidator()


async def validate_token(token: str = Depends(oauth2_scheme)):
    log.debug(f"validating token : {token}")
    return await validator.verify_token(token)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    log.debug("get_current_user from token")
    return await validator.verify_token(token)


def require_role(required_role: str):
    def role_checker(token_data=Depends(get_current_user)):
        log.debug(f"role_checker: checking for required role {required_role} in {token_data}")
        roles = token_data.get("roles", [])
        lower_roles = [s.lower() for s in roles]
        log.debug(f"role_checker: roles: {lower_roles}")

        if required_role.lower() not in lower_roles:
            log.warning(f"Insufficient privileges for role {required_role}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient privileges"
            )

        return token_data

    return role_checker
