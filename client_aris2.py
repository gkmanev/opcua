"""Read Aris telemetry and serve it over Modbus TCP without writing to SCADA."""

import asyncio
import struct
from pathlib import Path

from asyncua import Client
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusSlaveContext,
    ModbusServerContext,
)
from pymodbus.server import ModbusTcpServer


SERVER_URL = "opc.tcp://100.127.1.220:62550/DataAccessServer"
APPLICATION_URI = "urn:your-org:aris:opcua-client:pi2"
WIND_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd"
POWER_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W"
REACTIVE_POWER_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.Var"
FREQUENCY_NODE_ID = "ns=2;s=DA.Rakovo Aris.WTG01.WCNV01.GriHz"
READ_INTERVAL_SECONDS = 60
MODBUS_ADDRESS = ("0.0.0.0", 16599)
# Modbus holding register addresses; each float32 uses two registers.
REG_P_WT = 6    # P_WT: active power in MW (registers 6-7)
REG_Q_WT = 8    # Q_WT: reactive power in MVAr (registers 8-9)
REG_F_WT = 12   # F_WT: frequency in Hz (registers 12-13)


def float32_to_registers(value):
    """Encode float32 as big-endian registers, high word first."""
    return list(struct.unpack(">HH", struct.pack(">f", value)))


async def publish_data(client, context):
    wind_node = client.get_node(WIND_NODE_ID)
    power_node = client.get_node(POWER_NODE_ID)
    reactive_power_node = client.get_node(REACTIVE_POWER_NODE_ID)
    frequency_node = client.get_node(FREQUENCY_NODE_ID)

    while True:
        # Only read from SCADA; setValues updates the local Modbus datastore.
        wind = await wind_node.read_value()
        power = await power_node.read_value()
        reactive_power = await reactive_power_node.read_value()
        frequency = await frequency_node.read_value()
        power_registers = float32_to_registers(float(power) / 1000)
        # SCADA reactive power is assumed to be in kVAr; convert to MVAr.
        reactive_power_registers = float32_to_registers(float(reactive_power) / 1000)
        frequency_registers = float32_to_registers(float(frequency))
        context[0x00].setValues(3, REG_P_WT, power_registers)
        context[0x00].setValues(3, REG_Q_WT, reactive_power_registers)
        context[0x00].setValues(3, REG_F_WT, frequency_registers)
        print(
            f"Power: {power} kW | Reactive power: {reactive_power} kVAr | "
            f"Wind: {wind} m/s | Frequency: {frequency} Hz",
            flush=True,
        )
        await asyncio.sleep(READ_INTERVAL_SECONDS)


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
        store = ModbusSlaveContext(
            di=ModbusSequentialDataBlock(0, [0] * 100),
            co=ModbusSequentialDataBlock(0, [0] * 100),
            hr=ModbusSequentialDataBlock(0, [0] * 100),
            ir=ModbusSequentialDataBlock(0, [0] * 100),
        )
        context = ModbusServerContext(slaves={0x00: store}, single=False)
        server = ModbusTcpServer(context, address=MODBUS_ADDRESS)
        tasks = [
            asyncio.create_task(server.serve_forever()),
            asyncio.create_task(publish_data(client, context)),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            await server.shutdown()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
