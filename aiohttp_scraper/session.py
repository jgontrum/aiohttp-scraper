import asyncio
import math
import random
from typing import List, Optional

from aiohttp import ClientSession, ClientResponse
from aiohttp.hdrs import METH_GET

from aiohttp_scraper.exceptions import Unsuccessful, AllRetriesFailed
from aiohttp_scraper.proxies import Proxies
from aiohttp_scraper.user_agents import USER_AGENTS


class ScraperSession(ClientSession):
    def __init__(
        self,
        *args,
        proxies: Optional[Proxies] = None,
        user_agents: Optional[List[str]] = None,
        use_random_user_agent: bool = True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.proxies = proxies
        self.use_random_user_agent = use_random_user_agent or bool(user_agents)
        self.user_agents = user_agents or USER_AGENTS

    async def get_json(self, url: str, **kwargs) -> dict:
        return await (
            await self._request(
                METH_GET,
                url,
                expect_json=True,
                expected_mime_type="application/json",
                **kwargs,
            )
        ).json()

    async def get_html(self, url: str, **kwargs) -> str:
        return await (
            await self._request(METH_GET, url, expected_mime_type="text/html", **kwargs)
        ).text()

    async def _request(
        self,
        *args,
        retries: int = 5,
        exponential_backoff: bool = True,
        max_backoff_delay: float = 300.0,
        start_backoff_delay: float = 15.0,
        expect_json: bool = False,
        expected_mime_type: Optional[str] = None,
        **kwargs,
    ) -> ClientResponse:
        num_tries = 0
        stats = []

        while retries > 0:
            if self.use_random_user_agent:
                kwargs.get("headers", dict())["user-agent"] = random.choice(
                    self.user_agents
                )

            if self.proxies:
                kwargs["proxy"] = await self.proxies.select_proxy(url=args[1])

            try:
                response = await super()._request(*args, **kwargs)

                if self.proxies:
                    await self.proxies.register_status_code(
                        url=args[1],
                        status_code=response.status,
                        proxy_url=kwargs["proxy"],
                    )

                if not 200 <= response.status < 300:
                    raise Unsuccessful(f"Status code is {response.status}")

                if expected_mime_type:
                    content_type = response.headers.get("content-type", "")
                    success = expected_mime_type.lower() in content_type.lower()

                    if not success:
                        raise Unsuccessful(
                            f"MIME type does not match. (Expected '{expected_mime_type}', got {content_type})."
                        )

                if expect_json:
                    try:
                        await response.json()
                    except Exception as e:
                        raise Unsuccessful(f"Cannot parse JSON: {e}")

                if not expect_json:
                    if not response or not (await response.text()):
                        raise Unsuccessful(f"Empty response.")

                return response

            except Exception as e:
                stats.append(e)

                retries -= 1

                if retries > 0:
                    if not num_tries:
                        delay = start_backoff_delay
                    else:
                        delay = min(2 ** num_tries, max_backoff_delay)

                    rnd = math.floor(delay * 0.2)
                    delay += random.randint(-rnd, rnd)

                    await asyncio.sleep(delay)

                    num_tries += 1

        raise AllRetriesFailed(f"Tries: [{', '.join([str(e) for e in stats])}]")
