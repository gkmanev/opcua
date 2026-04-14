import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any

import aiohttp
from aiohttp import ClientTimeout, ClientError
from zoneinfo import ZoneInfo

CET_TZ = ZoneInfo("CET")


class PriceProcessor:
    BASE_URL = "https://api.visualize.energy/api/prices/range/"

    def __init__(self, timeout_seconds: int = 10):
        self.timeout = ClientTimeout(total=timeout_seconds)

    @staticmethod
    def _parse_api_datetime_utc(value: str) -> datetime:
        """Parse API timestamps like 2026-03-26T00:00:00Z into aware UTC datetimes."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _floor_15min(dt: datetime) -> datetime:
        """Floor to the previous 15-minute boundary (keeps tz)."""
        minute = (dt.minute // 15) * 15
        return dt.replace(minute=minute, second=0, microsecond=0)

    async def fetch_prices_period(
        self,
        country: str = "BG",
        period: str = "today",
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch price points for a predefined API period such as today or yesterday."""
        try:
            params = {
                "country": country,
                "period": period,
            }

            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                url = str(aiohttp.client.URL(self.BASE_URL).with_query(params))
                print(f"Fetching: {url}")

                async with session.get(self.BASE_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            items = data.get("items", [])
            if not items:
                print("No data returned.")
                return None

            print(f"Fetched {len(items)} price points successfully for period={period}.")
            return items

        except ClientError as e:
            print(f"HTTP error occurred: {e}")
        except asyncio.TimeoutError:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}")
        return None

    def _find_item_for_local_slot(
        self,
        items: List[Dict[str, Any]],
        target_local: datetime,
    ) -> Optional[Dict[str, Any]]:
        for item in items:
            timestamp_utc = item.get("datetime_utc")
            if not timestamp_utc:
                continue

            item_local = self._parse_api_datetime_utc(timestamp_utc).astimezone(CET_TZ)
            item_local = item_local.replace(second=0, microsecond=0)
            if item_local == target_local:
                return item
        return None

    async def get_price_prev_quarter_shifted(self, country: str = "BG", contract: str = "A01") -> Optional[float]:
        """
        Return the price for the current CET 15-minute slot.
        The API timestamps are UTC, so items are converted to CET before matching.
        The contract argument is kept for caller compatibility but is not used by this endpoint.
        """
        _ = contract

        now_local = datetime.now(CET_TZ)
        target_local = self._floor_15min(now_local)
        print(
            f"Local now: {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')} -> "
            f"Local floored: {target_local.strftime('%Y-%m-%d %H:%M %Z')}"
        )

        items = await self.fetch_prices_period(country=country, period="today")
        match = self._find_item_for_local_slot(items or [], target_local)

        if not match:
            items = await self.fetch_prices_period(country=country, period="yesterday")
            match = self._find_item_for_local_slot(items or [], target_local)

        if not match:
            print(f"No item found for CET local slot {target_local.isoformat()}.")
            return None

        matched_utc = self._parse_api_datetime_utc(match["datetime_utc"])
        matched_local = matched_utc.astimezone(CET_TZ)
        print(
            f"Matched local {matched_local.strftime('%Y-%m-%d %H:%M %Z')} "
            f"<- UTC {matched_utc.strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return match.get("price")


async def main():
    processor = PriceProcessor()
    value = await processor.get_price_prev_quarter_shifted(country="BG", contract="A01")
    if value is not None:
        print(f"Price (current CET slot): {value}")
    else:
        print("Failed to fetch price for current CET slot")


if __name__ == "__main__":
    asyncio.run(main())
