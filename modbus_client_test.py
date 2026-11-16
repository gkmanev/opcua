import struct
from pymodbus.client import ModbusTcpClient

REG_P_WT = 6    # WT active power (MW)
REG_Q_WT = 8    # WT reactive power (MVAr)
REG_F_WT = 12   # WT frequency (Hz)


def registers_to_float32(registers):
    # Big-endian, high word first
    return struct.unpack('>f', struct.pack('>HH', *registers))[0]


client = ModbusTcpClient('127.0.0.1', port=16599)
if not client.connect():
    print("Could not connect")
else:
    result = client.read_holding_registers(address=REG_P_WT, count=8, slave=0)
    if result.isError():
        print(f"Error reading registers: {result}")
    else:
        regs = result.registers
        power_offset = REG_P_WT - REG_P_WT
        reactive_power_offset = REG_Q_WT - REG_P_WT
        frequency_offset = REG_F_WT - REG_P_WT
        power = registers_to_float32(regs[power_offset:power_offset + 2])
        reactive_power = registers_to_float32(
            regs[reactive_power_offset:reactive_power_offset + 2]
        )
        freq = registers_to_float32(regs[frequency_offset:frequency_offset + 2])
        print(
            f"P_WT: {power:.3f} MW, Q_WT: {reactive_power:.3f} MVAr, "
            f"F_WT: {freq:.2f} Hz"
        )
    client.close()
