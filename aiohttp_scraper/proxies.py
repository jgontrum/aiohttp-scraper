import asyncio
import datetime
import json
from random import shuffle
from typing import List, Optional, Tuple

import aioredis
from aioredis import Redis
from tldextract import tldextract


class Proxy:
    def __init__(
        self,
        url: str,
        window_size_in_minutes: int = 5,
        max_requests_per_window: int = 100,
    ):
        if url.startswith("http"):
            self._url = url
        else:
            self._url = "http://" + url

        self.window_size_in_minutes = window_size_in_minutes
        self.max_requests_per_window = max_requests_per_window

        self.requests: List[datetime.datetime] = []
        self.status_codes: List[Tuple[int, datetime.datetime]] = []
        self.wait_until: Optional[datetime.datetime] = None

    @property
    def _clean_url(self) -> str:
        return self._url.replace("http://", "").replace(":", "_")

    @staticmethod
    def _decode_status(status: str) -> Tuple[int, datetime.datetime]:
        obj = json.loads(status)
        return int(obj["status"]), datetime.datetime.fromisoformat(obj["created_at"])

    @staticmethod
    def _encode_status(status: Tuple[int, datetime.datetime]) -> str:
        return json.dumps({"status": status[0], "created_at": status[1].isoformat()})

    async def get_number_of_free_slots(self, domain: str, redis: Redis) -> int:
        recent_requests, status_code = await asyncio.gather(
            redis.keys(f"scrape_proxy:{domain}:{self._clean_url}:requests:*"),
            redis.lindex(
                key=f"scrape_proxy:{domain}:{self._clean_url}:status_codes", index=0
            ),
        )

        num_free_slots = self.max_requests_per_window - len(recent_requests)

        if status_code:
            last_status, last_timestamp = self._decode_status(status_code)
            window_end = datetime.datetime.utcnow() - datetime.timedelta(
                minutes=self.window_size_in_minutes
            )

            if last_status == 429 and last_timestamp > window_end:
                num_free_slots = -1

        return num_free_slots

    async def get_url(self, domain: str, redis: Redis) -> str:
        now = datetime.datetime.utcnow().isoformat().replace(":", "-")
        await redis.set(
            key=f"scrape_proxy:{domain}:{self._clean_url}:requests:{now}",
            value=datetime.datetime.utcnow().isoformat(),
        )
        await redis.pexpire(
            key=f"scrape_proxy:{domain}:{self._clean_url}:requests:{now}",
            timeout=self.window_size_in_minutes * 60 * 1000,
        )

        return self._url

    async def register_status_code(self, status_code: int, domain: str, redis: Redis):
        await redis.lpush(
            key=f"scrape_proxy:{domain}:{self._clean_url}:status_codes",
            value=self._encode_status((status_code, datetime.datetime.utcnow())),
        )


class Proxies:
    def __init__(
        self,
        proxies: List[str],
        redis_uri: str,
        window_size_in_minutes: int = 5,
        max_requests_per_window: int = 100,
        redis_kwargs: Optional[dict] = None,
    ):
        self._proxy_urls = proxies
        self._redis_client = None

        self._proxies = [
            Proxy(url, window_size_in_minutes, max_requests_per_window)
            for url in self._proxy_urls
        ]

        self.redis_uri = redis_uri
        self.redis_kwargs = redis_kwargs

        self.window_size_in_minutes = window_size_in_minutes
        self.max_requests_per_window = max_requests_per_window

    async def setup(self):
        self._redis_client = await aioredis.create_redis_pool(
            address=self.redis_uri, **(self.redis_kwargs or {})
        )
        await self.cleanup()

    async def select_proxy(self, url: str) -> str:
        if not self._redis_client:
            await self.setup()
        await self.cleanup()

        domain = tldextract.extract(url).domain

        while True:
            free_slots = await asyncio.gather(
                *[
                    proxy.get_number_of_free_slots(domain, self._redis_client)
                    for proxy in self._proxies
                ]
            )

            slots_and_proxies = list(zip(free_slots, self._proxies))
            shuffle(slots_and_proxies)

            proxies = sorted(slots_and_proxies, key=lambda d: d[0], reverse=True)

            if proxies[0][0] > 0:
                return await proxies[0][1].get_url(domain, self._redis_client)
            else:
                # No proxy available right now. Wait.
                await asyncio.sleep(5)

    async def register_status_code(self, url: str, status_code: int, proxy_url: str):
        domain = tldextract.extract(url).domain

        for proxy in self._proxies:
            if proxy._url == proxy_url:
                await proxy.register_status_code(
                    status_code, domain, self._redis_client
                )
                break

    async def cleanup(self):
        keys = await self._redis_client.keys(f"scrape_proxy:*requests*")
        timestamps = [
            datetime.datetime.strptime(
                k.decode().split(":")[-1], "%Y-%m-%dT%H-%M-%S.%f"
            )
            for k in keys
        ]

        window_end = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=self.window_size_in_minutes
        )

        invalid_timestamps = [
            key for key, ts in zip(keys, timestamps) if ts < window_end
        ]

        if invalid_timestamps:
            await self._redis_client.delete(*invalid_timestamps)
