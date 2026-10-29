"""Read and print Aris power and wind speed without writing to SCADA."""

import asyncio
from pathlib import Path

from asyncua import Client


SERVER_URL = "opc.tcp://10.126.252.1:62550/DataAccessServer"
APPLICATION_URI = "urn:your-org:aris:opcua-client:pi2"
WIND_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd"
POWER_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W"
READ_INTERVAL_SECONDS = 60


async def main():
    base_dir = Path(__file__).parent
    certificate = base_dir / "aris_pi2_cert.pem"
    private_key = base_dir / "aris_pi2_private_key.pem"

    client = Client(url=SERVER_URL)
    client.application_uri = APPLICATION_URI
    await client.set_security_string(
        f"Basic256,SignAndEncrypt,{certificate},{private_key}"
    )
    await client.load_client_certificate(certificate)
    await client.load_private_key(private_key)

    async with client:
        wind_node = client.get_node(WIND_NODE_ID)
        power_node = client.get_node(POWER_NODE_ID)

        while True:
            # read_value() is read-only; this script never calls set_value() or a method.
            wind = await wind_node.read_value()
            power = await power_node.read_value()
            print(f"Power: {power} kW | Wind: {wind} m/s", flush=True)
            await asyncio.sleep(READ_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
