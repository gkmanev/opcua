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
    def __init__(self, opcua_client, email_processor) -> None:
        self.opcua_client = opcua_client
        self.email_processor = email_processor
        self.turbine_status = None       



    async def publish_data(self):        
        try:
            wind_value, power_value, turbine_status = await self.opcua_client.read_data()   
            self.turbine_status = turbine_status.Value.Value  
            print(f'Wind Speed: {wind_value.Value.Value} m/s')
            url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v5={wind_value.Value.Value}" # Aris           
                
            r_wind = requests.get(url_wind)
            if r_wind.status_code == 200:
                pass
            print(f'Power: {power_value.Value.Value} kW')
            url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v4={power_value.Value.Value}"  # Aris            
            r_power = requests.get(url_power) 
            if r_power.status_code == 200:
                pass            
        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    async def turbine_control(self):
        next_forecast_value = await self.email_processor.process_files()        
        print(f"Turbine Current Status: {self.turbine_status} || command:{next_forecast_value}") 
        if next_forecast_value != "NA":      
            converted_to_kw = float(next_forecast_value)*1000
            url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v2={converted_to_kw}" 
            r_forecast = requests.get(url_forecast)
            if r_forecast.status_code == 200:
                pass
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=1"
            r = requests.get(url)
            if r.status_code == 200:
                pass
        else:
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=0"
            r = requests.get(url)            



async def main():
    cert_base = Path(__file__).parent    
    
    url_aris = "opc.tcp://10.126.252.1:62550/DataAccessServer"
    wind_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd'
    power_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W'
    status_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurSt'   
    opcua_client = OPCUAClient(        
        url = url_aris,     
        client_app_uri="urn:freeopcua:client",
        cert_path=cert_base / "my_cert.pem",
        private_key_path=cert_base / "my_private_key.pem",
        wind_node = wind_node_aris,
        power_node = power_node_aris,
        status_node = status_node_aris,
    )
    await opcua_client.setup()
    email_forecast_processor = FileManager("aris")    
    scheduler = AsyncIOScheduler()         
    publisher = DataPublisher(opcua_client, email_forecast_processor)
    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    scheduler.add_job(publisher.turbine_control, IntervalTrigger(minutes=1))  

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