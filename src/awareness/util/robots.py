"""robots.txt cache.

We use the stdlib ``urllib.robotparser`` (RFC 9309-aligned) and add async
fetching with a short TTL. Per-domain crawl-delay is honored where reported.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from awareness.obs.logging import get_logger

logger = get_logger("util.robots")


@dataclass
class RobotsEntry:
    parser: RobotFileParser | None
    expires_at: float
    crawl_delay: float | None


class RobotsCache:
    """In-memory robots cache. Single-process.

    Use ``await is_allowed(url, user_agent)`` from async code.
    """

    def __init__(self, ttl: int = 3600, timeout: float = 10.0) -> None:
        self._ttl = ttl
        self._timeout = timeout
        self._entries: dict[str, RobotsEntry] = {}
        # We hold a shared client; not strictly necessary but cheaper.
        self._client: httpx.AsyncClient | None = None

    async def _client_lazy(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=True,
                headers={"Accept": "text/plain, */*;q=0.1"},
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def _site_key(url: str) -> str:
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return ""
        return f"{parts.scheme.lower()}://{parts.netloc.lower()}"

    async def _load(self, site: str, user_agent: str) -> RobotsEntry:
        url = f"{site}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(url)
        crawl_delay: float | None = None
        try:
            client = await self._client_lazy()
            resp = await client.get(url, headers={"User-Agent": user_agent})
            if resp.status_code == 200 and resp.text:
                rp.parse(resp.text.splitlines())
                # crawl-delay isn't first-class in RobotFileParser; emulate.
                cd = rp.crawl_delay(user_agent)
                if cd is not None:
                    try:
                        crawl_delay = float(cd)
                    except (TypeError, ValueError):
                        crawl_delay = None
            elif resp.status_code in (401, 403):
                # Treat as DISALLOWED for everything.
                rp.parse(["User-agent: *", "Disallow: /"])
            elif resp.status_code == 404:
                rp.parse([])  # implicit allow-all
            else:
                rp.parse([])  # be permissive on transient errors
            return RobotsEntry(parser=rp, expires_at=time.time() + self._ttl, crawl_delay=crawl_delay)
        except (httpx.HTTPError, ValueError, OSError) as e:
            logger.warning("robots_fetch_failed", site=site, err=str(e))
            # Be cautious on failure: cache empty/permissive entry briefly.
            rp.parse([])
            return RobotsEntry(parser=rp, expires_at=time.time() + min(self._ttl, 300), crawl_delay=None)

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        site = self._site_key(url)
        if not site:
            return False
        entry = self._entries.get(site)
        if entry is None or entry.expires_at < time.time():
            entry = await self._load(site, user_agent)
            self._entries[site] = entry
        if entry.parser is None:
            return False
        try:
            return entry.parser.can_fetch(user_agent, url)
        except (ValueError, AttributeError):
            return False

    def crawl_delay(self, url: str) -> float | None:
        site = self._site_key(url)
        e = self._entries.get(site)
        return e.crawl_delay if e else None
