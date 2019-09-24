# AIOHTTP Scraper
### A robust asynchronous web scraping client

Because scraping is messy.

---

## Features

### ScraperSession
- Drop-in replacement for AIOHTTP's `ClientSession`
- Catches Exceptions caused by a request
- Retries a given number of times
- Exponential backoff to wait between repeated requests
- Validates the MIME-type
- Helper functions to receive HTML and JSON

### Proxies
- Manage a pool of rotating proxies
- LeastConn-style proxy selection
- Uses a moving time window to throttle requests per domain and proxy
- Handles 429 to deactivate a proxy for a few minutes
- Redis backend for persistence

---

## Installation
Requires Python 3.7.
```bash
pip install aiohttp-scraper
```

## Usage
```python
from aiohttp_scraper import ScraperSession

async with ScraperSession() as session:
    resp = await session.get_json(some_url)
```

```python
from aiohttp_scraper import ScraperSession
from aiohttp_scraper import Proxies

proxies = Proxies(
    proxies=[
        "123.456.789.12:1234",
        "123.456.789.12:1237",
        # ...
    ],
    redis_uri="redis://localhost:6379",
    window_size_in_minutes=5,
    max_requests_per_window=300,
    redis_kwargs={},  # E.g for setting authentication
)

async with ScraperSession(proxies=proxies) as session:
    resp = await session.get_json(some_url)
```
