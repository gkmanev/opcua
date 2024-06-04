import asyncio
import logging
import sys
from pathlib import Path
import socket
from paho.mqtt import client as mqtt_client

from asyncua import Client, ua
from asyncua.crypto.security_policies import SecurityPolicyBasic256
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.crypto.truststore import TrustStore


broker = "159.89.103.242"
port = 1883
client_id = "skjfa[w899]"
keep_alive_interval = 60
topic_power = 'aris/1mpow'
topic_wind = 'aris/1mwind'


# Set up detailed logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# _logger = logging.getLogger('asyncua')

# OPC UA Server settings
url = "opc.tcp://10.126.252.1:62550/DataAccessServer"
# username = "TakeFromCertificate"
# username = "georgi-VirtualBox"
username = "urn:freeopcua:client"

# Security and certificate settings
cert_base = Path(__file__).parent
cert_path = "my_cert.pem"
private_key_path = cert_base / "my_private_key.pem"
server_cert_path = cert_base / "trusted"  # The server's certificate
client_app_uri = "urn:freeopcua:client" 


async def mqtt_connect_and_publish(broker, port, topic, payload):
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    
    mqtt = mqtt_client.Client()
    mqtt.on_connect = on_connect
    mqtt.connect(broker, port)
    mqtt.loop_start()
    mqtt.publish(topic, payload)
    mqtt.loop_stop()
    mqtt.disconnect()
   

async def task():    
    
    client = Client(url=url)
    client.application_uri = client_app_uri
    await client.set_security_string(f"Basic256,SignAndEncrypt,{cert_path},{private_key_path}")
    # Setup for trust store and validator as before...

    await client.load_client_certificate(cert_path)
    await client.load_private_key(private_key_path)
    try:
        async with client:
            while True:
                try:
                    wind_node = client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd')
                    wind_value = await wind_node.read_data_value()
                    print(f'Wind Speed: {wind_value.Value.Value} m/s')
                    await mqtt_connect_and_publish(broker, port, topic_wind, str(round(wind_value.Value.Value, 2)))

                    power_node = client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W')
                    power_value = await power_node.read_data_value()
                    print(f'Power: {power_value.Value.Value} kW')
                    await mqtt_connect_and_publish(broker, port, topic_power, str(round(power_value.Value.Value, 2)))
                    

                except ua.UaStatusCodeError as e:
                    print(f"OPC UA Error: {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")

                # Sleep for 5 minutes
                await asyncio.sleep(30)

    except Exception as e:
        print(f"Failed to connect or error during operation: {e}")
    finally:
        await client.disconnect()
        print("Disconnected from server")

def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(task())
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        loop.close()
        print("Program terminated")

if __name__ == "__main__":
    main()
