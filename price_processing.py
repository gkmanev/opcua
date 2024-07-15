import aiohttp
import asyncio

from datetime import datetime
from aiohttp import ClientTimeout
from aiohttp.client_exceptions import ClientError

# Configure logging


class PriceProcessor:
    async def ibex_price(self):
        try:
            now = datetime.now()
            now = now.replace(minute=0, second=0, microsecond=0)
            formatted_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')
            url_price = f"http://85.14.6.37:16455/api/price/?timestamp=&start_date={formatted_time}&end_date={formatted_time}"
            
            timeout = ClientTimeout(total=10)  # 10 seconds timeout
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url_price) as response:
                    response.raise_for_status()  # Raise an exception for HTTP errors
                    data = await response.json()
                    print("Successfully fetched price data")
                    price = data[0].get("value", None)
                    return price

        except ClientError as e:
            print(f"HTTP error occurred: {e}")
        except asyncio.TimeoutError:
            print("Request timed out")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        # Return None in case of any error
        return None

# Example usage
async def main():
    processor = PriceProcessor()
    price_data = await processor.get_price()
    if price_data is not None:
        print(price_data)
    else:
        print("Failed to fetch price data")

if __name__ == "__main__":
    asyncio.run(main())
