from labelling_task.webclient.OAuth2TokenProvider import OAuth2TokenProvider
import httpx
from contextlib import asynccontextmanager


class OAuth2HttpClient:
    def __init__(self, token_provider: OAuth2TokenProvider, client: httpx.AsyncClient = None):
        self.token_provider = token_provider
        self.session = client or httpx.AsyncClient()

    async def request(self, method: str, url: str, **kwargs):
        token = await self.token_provider.get_token()

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"

        return await self.session.request(
            method,
            url,
            headers=headers,
            **kwargs,
        )

    @asynccontextmanager
    async def stream(self, method: str, url: str, **kwargs):
        token = await self.token_provider.get_token()

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"

        async with self.session.stream(method, url, headers=headers, **kwargs) as resp:
            yield resp

    async def get(self, url: str, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs):
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs):
        return await self.request("DELETE", url, **kwargs)
