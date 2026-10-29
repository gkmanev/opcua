import struct
from pymodbus.client import ModbusTcpClient

REG_P_WT = 6    # WT active power (MW)
REG_F_WT = 12   # WT frequency (Hz)


def registers_to_float32(registers):
    # Big-endian, high word first
    return struct.unpack('>f', struct.pack('>HH', *registers))[0]


client = ModbusTcpClient('85.14.6.37', port=16599)
if not client.connect():
    print("Could not connect")
else:
    result = client.read_holding_registers(address=REG_P_WT, count=8, slave=0)
    if result.isError():
        print(f"Error reading registers: {result}")
    else:
        regs = result.registers
        power = registers_to_float32(regs[0:2])
        freq = registers_to_float32(regs[6:8])
        print(f"P_WT: {power:.3f} MW, F_WT: {freq:.2f} Hz")
    client.close()
