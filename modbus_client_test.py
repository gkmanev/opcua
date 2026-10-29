from pymodbus.client import ModbusTcpClient

client = ModbusTcpClient('85.14.6.37', port=16599)
if not client.connect():
    print("Could not connect")
else:
    result = client.read_holding_registers(address=0, count=3, slave=0)
    if result.isError():
        print(f"Error reading registers: {result}")
    else:
        power, wind, freq = result.registers
        print(f"Power: {power}, Wind: {wind}, Freq: {freq}")
    client.close()
