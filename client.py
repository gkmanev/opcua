import asyncio
from pathlib import Path
from opcua_setup import OPCUAClient
from mqtt import MQTTClient
from mail_processing import GmailService, FileManager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import requests


class DataPublisher:
    def __init__(self, opcua_client, mqtt_client, topic_wind, topic_power):
        self.opcua_client = opcua_client
        self.mqtt_client = mqtt_client
        self.topic_wind = topic_wind
        self.topic_power = topic_power

    async def publish_data(self):
        while True:
            try:
                wind_value, power_value = await self.opcua_client.read_data()
                print(f'Wind Speed: {wind_value.Value.Value} m/s')
                print(f'Power: {power_value.Value.Value} kW')
                await self.mqtt_client.connect_and_publish(self.topic_wind, str(round(wind_value.Value.Value, 2)))
                await self.mqtt_client.connect_and_publish(self.topic_power, str(round(power_value.Value.Value, 2)))
            except ua.UaStatusCodeError as e:
                print(f"OPC UA Error: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
            await asyncio.sleep(30)   

class TourbineControl:
    def __init__(self, file_manager, opcua_client) -> None:
        self.file_manager = file_manager
        self.opcua_client = opcua_client
    
    async def scheduler_check(self):
        next_forecast_value = await self.file_manager.process_files()
        current_status = await self.status_check()
        print(f"Turbine Current Status: {current_status} || command:{next_forecast_value}")        
        if next_forecast_value:            
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=1"
            r = requests.get(url)
        else:
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=0"
            r = requests.get(url)
    
    async def status_check(self):        
        status = await self.opcua_client.check_tourbine_status()
        return status.Value.Value

async def main():
    cert_base = Path(__file__).parent
    opcua_client = OPCUAClient(
        url="opc.tcp://10.126.252.1:62550/DataAccessServer",
        client_app_uri="urn:freeopcua:client",
        cert_path=cert_base / "my_cert.pem",
        private_key_path=cert_base / "my_private_key.pem"
    )
    await opcua_client.setup()
    process_the_file = FileManager()
    tourbine_control = TourbineControl(process_the_file, opcua_client)
    scheduler = AsyncIOScheduler()    
    scheduler.add_job(tourbine_control.scheduler_check, IntervalTrigger(minutes=1))
    scheduler.start()
    # Start/Stop the turine    
    #await opcua_client.send_stop_start_command("start")

    mqtt_client = MQTTClient(broker="159.89.103.242", port=1883)    
    publisher = DataPublisher(opcua_client, mqtt_client, topic_wind='aris/1mwind', topic_power='aris/1mpow')

    await publisher.publish_data()
    # Start the scheduler
    
     
if __name__ == "__main__":
    #asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()