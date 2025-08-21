from pymodbus.client import ModbusTcpClient

client = ModbusTcpClient('85.14.6.37', port=16599)
client.connect()
result = client.read_holding_registers(address=0, count=2, slave=0)
if result.isError():
    print(f"Error reading registers: {result}")
else:
    print(result.registers)
client.close()

