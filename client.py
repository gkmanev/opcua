import asyncio
from pathlib import Path
from opcua_setup import OPCUAClient
from mqtt import MQTTClient
from mail_processing import GmailService, FileManager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from asyncua import ua
import requests


class DataPublisher:
    def __init__(self, opcua_client, mqtt_client, topic_wind, topic_power):
        self.opcua_client = opcua_client
        self.mqtt_client = mqtt_client
        self.topic_wind = topic_wind
        self.topic_power = topic_power

    async def test(self):
        print("TEST IS HERE!")

    async def publish_data(self):        
        try:
            wind_value, power_value, turbine_status = await self.opcua_client.read_data()   
            print(f'Turbine status: {turbine_status.Value.Value}')             
            print(f'Wind Speed: {wind_value.Value.Value} m/s')
            url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v5={wind_value.Value.Value}" # Aris
            #url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v11={wind_value.Value.Value}" # Power
                
            r_wind = requests.get(url_wind)
            if r_wind.status_code == 200:
                pass
            print(f'Power: {power_value.Value.Value} kW')
            url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v4={power_value.Value.Value}"  # Aris
            #url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v10={power_value.Value.Value}" # Power
            r_power = requests.get(url_power) 
            if r_power.status_code == 200:
                pass
            await self.mqtt_client.connect_and_publish(self.topic_wind, str(round(wind_value.Value.Value, 2)))
            await self.mqtt_client.connect_and_publish(self.topic_power, str(round(power_value.Value.Value, 2)))
        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
               

# class TourbineControl:
#     def __init__(self, file_manager, opcua_client) -> None:
#         self.file_manager = file_manager
#         self.opcua_client = opcua_client
    
#     async def scheduler_check(self):
#         next_forecast_value = await self.file_manager.process_files()
#         current_status = await self.status_check()
#         print(f"Turbine Current Status: {current_status} || command:{next_forecast_value}")        
#         if next_forecast_value:            
#             url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=1"
#             r = requests.get(url)
#             if r.status_code == 200:
#                 pass
#         else:
#             url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=0"
#             r = requests.get(url)
    
#     async def status_check(self):        
#         status = await self.opcua_client.check_tourbine_status()
#         return status.Value.Value

async def main():
    cert_base = Path(__file__).parent    
    
    url_aris = "opc.tcp://10.126.252.1:62550/DataAccessServer"
    url_power = "opc.tcp://10.126.253.1:62550/DataAccessServer"
    wind_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd'
    wind_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WMET01.HorWdSpd'
    power_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W'
    power_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.W'
    status_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurSt'
    status_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurSt'
    opcua_client = OPCUAClient(        
        url = url_aris, #url_power        
        client_app_uri="urn:freeopcua:client",
        cert_path=cert_base / "my_cert.pem",
        private_key_path=cert_base / "my_private_key.pem",
        wind_node = wind_node_aris,
        power_node = power_node_aris,
        status_node = status_node_aris
    )
    await opcua_client.setup()
    process_the_file = FileManager()
    #tourbine_control = TourbineControl(process_the_file, opcua_client)
    scheduler = AsyncIOScheduler()    
    #scheduler.add_job(tourbine_control.scheduler_check, IntervalTrigger(minutes=1))
    
    # Start/Stop the turine    
    #await opcua_client.send_stop_start_command("start")

    mqtt_client = MQTTClient(broker="159.89.103.242", port=1883)    
    publisher = DataPublisher(opcua_client, mqtt_client, topic_wind='power/1mwind', topic_power='power/1mpow')#power/aris
    scheduler.add_job(publisher.publish_data, IntervalTrigger(seconds=30))
    #await publisher.publish_data()
    # Start the scheduler
    
    scheduler.start()
    try:
        await asyncio.Event().wait()  # Keep the loop running
    finally:
        await opcua_client.close()
    
     
if __name__ == "__main__":
    asyncio.run(main())
    # loop = asyncio.get_event_loop()
    # loop.create_task(main())
    # loop.run_forever()