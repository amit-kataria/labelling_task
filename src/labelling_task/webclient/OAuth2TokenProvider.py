import time
import asyncio
import httpx
from typing import Optional


class OAuth2TokenProvider:
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
        timeout: int = 5,
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.timeout = timeout

        self._lock = asyncio.Lock()
        self._access_token = None
        self._expires_at = 0

    async def get_token(self) -> str:
        now = time.time()

        if self._access_token and now < self._expires_at:
            return self._access_token

        async with self._lock:
            # double-check inside lock
            if self._access_token and now < self._expires_at:
                return self._access_token

            await self._fetch_token()
            return self._access_token

    async def _fetch_token(self):
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                self.token_url,
                data=data,
                auth=(self.client_id, self.client_secret),
            )
            resp.raise_for_status()

            payload = resp.json()

            self._access_token = payload["access_token"]
            expires_in = payload.get("expires_in", 300)

            # refresh slightly early
            self._expires_at = time.time() + expires_in - 30
