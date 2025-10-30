import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import aiohttp
from aiohttp import ClientTimeout, ClientError
from zoneinfo import ZoneInfo  # Python 3.9+

CET_TZ = ZoneInfo("Europe/Sofia")   # CET/CEST with DST handling
UTC_SHIFT_HOURS = 1                  # Shift the UTC slot by +1h vs local floored time


class PriceProcessor:
    BASE_URL = "http://85.14.6.37:16601/api/prices/range/"

    def __init__(self, timeout_seconds: int = 10):
        self.timeout = ClientTimeout(total=timeout_seconds)

    @staticmethod
    def _fmt_z(dt: datetime) -> str:
        """Format tz-aware datetime as ISO 8601 with 'Z' suffix (UTC)."""
        dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _floor_15min(dt: datetime) -> datetime:
        """Floor to the previous 15-minute boundary (keeps tz)."""
        minute = (dt.minute // 15) * 15
        return dt.replace(minute=minute, second=0, microsecond=0)

    @staticmethod
    def _tomorrow_midnight_utc_from_local(now_local: datetime) -> datetime:
        """UTC timestamp for local midnight tomorrow."""
        tomorrow = (now_local + timedelta(days=1)).date()
        midnight_local = datetime.combine(tomorrow, datetime.min.time(), tzinfo=CET_TZ)
        return midnight_local.astimezone(timezone.utc)

    async def fetch_prices_range(
        self,
        country: str = "BG",
        contract: str = "A01",
        start_dt: Optional[datetime] = None,  # tz-aware, any tz
        end_dt: Optional[datetime] = None,    # tz-aware, any tz
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch price points for [start_dt, end_dt). Converts to UTC for the API."""
        try:
            now_local = datetime.now(CET_TZ)
            start_dt = start_dt or now_local
            end_dt = end_dt or self._tomorrow_midnight_utc_from_local(now_local)

            # Convert to UTC for the API query
            start_utc = start_dt.astimezone(timezone.utc)
            end_utc = end_dt.astimezone(timezone.utc)

            params = {
                "country": country,
                "contract": contract,
                "start": self._fmt_z(start_utc),
                "end": self._fmt_z(end_utc),
            }

            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                url = str(aiohttp.client.URL(self.BASE_URL).with_query(params))
                print(f"Fetching: {url}")
                print(f"Local (CET/CEST) range: {start_dt} → {end_dt}")

                async with session.get(self.BASE_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            items = data.get("items", [])
            if not items:
                print("No data returned.")
                return None

            print(f"Fetched {len(items)} price points successfully.")
            return items

        except ClientError as e:
            print(f"HTTP error occurred: {e}")
        except asyncio.TimeoutError:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}")
        return None

    async def get_price_prev_quarter_shifted(self, country: str = "BG", contract: str = "A01") -> Optional[float]:
        """
        Floor NOW in CET/CEST to previous 15-min slot, then shift the UTC target by +UTC_SHIFT_HOURS.
        Example: now=13:27 CET -> local target=13:15 CET -> desired UTC = 13:15Z (i.e., +1h vs 12:15Z).
        Also prints both UTC and CET/CEST labels for the matched slot.
        """
        now_local = datetime.now(CET_TZ)
        target_local = self._floor_15min(now_local)

        # Convert local target to UTC, then apply the requested shift (+1h)
        base_utc = target_local.astimezone(timezone.utc)
        target_utc = base_utc + timedelta(hours=UTC_SHIFT_HOURS)
        target_cet = target_utc.astimezone(CET_TZ)

        end_utc = self._tomorrow_midnight_utc_from_local(now_local)

        print(f"Local now: {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')} → Local floored: {target_local.strftime('%H:%M %Z')}")
        print(f"Desired target: {target_utc.strftime('%H:%M')} UTC ↔ {target_cet.strftime('%H:%M %Z')}")

        # Fetch including this desired UTC target
        items = await self.fetch_prices_range(
            country=country,
            contract=contract,
            start_dt=target_utc,    # start at the shifted UTC moment
            end_dt=end_utc,         # run until local midnight tomorrow (in UTC)
        )
        if not items:
            return None

        target_utc_str = self._fmt_z(target_utc)
        match = next((it for it in items if it.get("datetime_utc") == target_utc_str), None)
        if not match:
            print(f"No item found for target UTC slot {target_utc_str}.")
            return None

        print(f"Matched {target_utc.strftime('%H:%M')} UTC ↔ {target_cet.strftime('%H:%M %Z')} ({target_utc_str})")
        return match.get("price")


# Example usage
async def main():
    processor = PriceProcessor()
    value = await processor.get_price_prev_quarter_shifted(country="BG", contract="A01")
    if value is not None:
        print(f"Price (shifted target): {value}")
    else:
        print("Failed to fetch price for shifted target")

if __name__ == "__main__":
    asyncio.run(main())
