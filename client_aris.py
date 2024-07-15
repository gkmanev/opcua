import asyncio
import aiohttp
from pathlib import Path
from opcua_setup import OPCUAClient
from mqtt import MQTTClient
from mail_processing import GmailService, FileManager, ForecastProcessor
from price_processing import PriceProcessor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from asyncua import ua
from functools import partial
from datetime import datetime



class DataPublisher:
    def __init__(self, opcua_client, email_processor, dam_price_processor) -> None:
        self.opcua_client = opcua_client
        self.email_processor = email_processor
        self.dam_price_processor = dam_price_processor
        self.turbine_status = None       
        self.accumulate_power = 0



    async def publish_data(self):        
        try:
            wind_value, power_value, turbine_status = await self.opcua_client.read_data()   
            self.turbine_status = turbine_status.Value.Value  
            print(f'Wind Speed: {wind_value.Value.Value} m/s')
            url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v5={wind_value.Value.Value}" # Aris  
            async with aiohttp.ClientSession() as session:
                async with session.get(url_wind) as response:
                    if response.status == 200:
                        pass                         
            
            current_minute = datetime.now().minute
      
            if current_minute % 15 == 0:
                self.accumulate_power = 0
                print("Accumulate power resetting")
            self.accumulate_power += int(power_value.Value.Value)
            url_aris_accumulate = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v1={self.accumulate_power/60}"  # Aris  
            async with aiohttp.ClientSession() as session:
                async with session.get(url_aris_accumulate) as response:
                    if response.status == 200:
                        pass    
       
            print(f'Power: {power_value.Value.Value} kW')            
            
            url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v4={power_value.Value.Value}"  # Aris  
            async with aiohttp.ClientSession() as session:
                async with session.get(url_power) as response:
                    if response.status == 200:
                        pass   
        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    

    
    async def turbine_control(self):
        next_forecast_value = await self.email_processor.process_files()        
        print(f"Turbine Current Status: {self.turbine_status} || command:{next_forecast_value}") 
        if next_forecast_value:
            if next_forecast_value != "NA":      
                converted_to_kw = float(next_forecast_value)*1000
                url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v2={converted_to_kw}" 
                async with aiohttp.ClientSession() as session:
                    async with session.get(url_forecast) as response:
                        if response.status == 200:
                            pass   
               
                url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=1"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            pass   
            else:
                url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=0"
                async with session.get(url) as response:
                    if response.status == 200:
                        pass         

    async def get_price(self):
        price = await self.dam_price_processor.ibex_price()
        url_price = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v3={float(price)}" 
        async with aiohttp.ClientSession() as session:
            async with session.get(url_price) as response:
                if response.status == 200:
                    pass
        


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
    dam_price = PriceProcessor()
    file_forecast_processor = FileManager("aris")    
    gmail_processor = ForecastProcessor()
    scheduler = AsyncIOScheduler()         
    publisher = DataPublisher(opcua_client, file_forecast_processor, dam_price)
    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    scheduler.add_job(publisher.turbine_control, IntervalTrigger(minutes=1))  
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=11, minute=42))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=16, minute=30))

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