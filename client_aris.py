import asyncio
import aiohttp
import os
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
from datetime import datetime, timedelta



class DataPublisher:
    def __init__(self, opcua_client, gmail_preocessing_service, email_files_processor, dam_price_processor) -> None:
        self.opcua_client = opcua_client
        self.gmail_service = gmail_preocessing_service
        self.email_processor = email_files_processor
        self.dam_price_processor = dam_price_processor        
        self.accumulate_power = 0
        self.next_forecast_value = None
        self.turbine_status_aris = None
        self.power_aris = None
        self.wind_aris = None



    async def publish_data(self):        
        try:            
            self.next_forecast_value = await self.email_processor.process_files()
            print(f"FORECAST PRINT: {self.next_forecast_value}")

            if self.next_forecast_value:
                if self.next_forecast_value == "NA":                    
                    if self.turbine_status_aris == 3:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data(command="stop")
                        self.turbine_status_aris = turbine_status.Value.Value 
                        self.power_aris = power_value.Value.Value
                        self.wind_aris = wind_value.Value.Value

                    else:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data()  
                        self.turbine_status_aris = turbine_status.Value.Value  
                        self.power_aris = power_value.Value.Value
                        self.wind_aris = wind_value.Value.Value                
                    
                else:
                    if self.turbine_status_aris == 2:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data(command="start")
                        self.turbine_status_aris = turbine_status.Value.Value
                        self.power_aris = power_value.Value.Value
                        self.wind_aris = wind_value.Value.Value
                        
                    else:                        
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data()                        
                        self.turbine_status_aris = turbine_status.Value.Value
                        self.power_aris = power_value.Value.Value
                        self.wind_aris = wind_value.Value.Value
                        
                        
                       
            print(f'Turbine Status: {self.turbine_status_aris} ')
            print(f'Power: {self.power_aris} kW')
            await self.blynk_send_power()
            await self.blynk_send_wind()
            await self.blynk_publish_status()
            await self.blynk_publish_accumulate()
            await self.blynk_send_forecast()
             

        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    

    async def get_price(self):
        price = await self.dam_price_processor.ibex_price()
        print(price)
        url_price = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v3={float(price)}" 
        async with aiohttp.ClientSession() as session:
            async with session.get(url_price) as response:
                if response.status == 200:
                    pass

    async def blynk_send_power(self):        
        url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v4={self.power_aris}"  # Aris  
        async with aiohttp.ClientSession() as session:
            async with session.get(url_power) as response:
                if response.status == 200:
                    pass   

    async def blynk_send_wind(self):
        url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v5={self.wind_aris}" # Aris
        async with aiohttp.ClientSession() as session:
            async with session.get(url_wind) as response:
                if response.status == 200:
                    pass    

    async def blynk_send_forecast(self):
        if self.next_forecast_value and self.next_forecast_value != "NA":
            value_published_to_blynk = self.next_forecast_value*1000 
        else:
            value_published_to_blynk = 0
        url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v2={value_published_to_blynk}" #V9 Neykovo V2 Aris
        async with aiohttp.ClientSession() as session:
            async with session.get(url_forecast) as response:
                if response.status == 200:
                    pass      
    
    async def blynk_publish_accumulate(self):
        current_minute = datetime.now().minute      
        if current_minute % 15 == 0:
            self.accumulate_power = 0            
        self.accumulate_power += int(self.power_aris)
        url_aris_accumulate = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v1={self.accumulate_power/60}"  # Aris  
        async with aiohttp.ClientSession() as session:
            async with session.get(url_aris_accumulate) as response:
                if response.status == 200:
                    pass  

    async def blynk_publish_status(self):
        #publish turbine status
        if self.turbine_status_aris == 3:  
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        pass 
        else:
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v0=0"
            async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            pass  

    
async def main():
    cert_base = Path(__file__).parent    
    
    url_aris = "opc.tcp://10.126.252.1:62550/DataAccessServer"
    wind_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd'
    power_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W'
    status_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurSt' 
    #start/stop
    start_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStrOp'
    stop_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp'  
    opcua_client = OPCUAClient(        
        url = url_aris,     
        client_app_uri="urn:freeopcua:client",
        cert_path=cert_base / "my_cert.pem",
        private_key_path=cert_base / "my_private_key.pem",
        wind_node = wind_node_aris,
        power_node = power_node_aris,
        status_node = status_node_aris,
        start_node = start_node_aris,
        stop_node = stop_node_aris
    )
    await opcua_client.setup()
    dam_price = PriceProcessor()
    file_forecast_processor = FileManager("aris")    
    gmail_processor = ForecastProcessor()
    scheduler = AsyncIOScheduler()         
    publisher = DataPublisher(opcua_client, gmail_processor, file_forecast_processor, dam_price)
    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    #scheduler.add_job(publisher.turbine_control, IntervalTrigger(minutes=1))  
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=10, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=11, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=12, minute=15))  

    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=15, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=16, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=17, minute=0))

    scheduler.add_job(publisher.get_price, IntervalTrigger(minutes=1))  


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