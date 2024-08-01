import asyncio
import aiohttp
from pathlib import Path
from opcua_setup import OPCUAClient
from mqtt import MQTTClient
from mail_processing import GmailService, FileManager, ForecastProcessor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from asyncua import ua
from functools import partial
from datetime import datetime

import requests


class DataPublisher:
    def __init__(self, opcua_client, gmail_preocessing_service, email_files_processor) -> None:
        self.opcua_client = opcua_client
        self.gmail_service = gmail_preocessing_service
        self.email_processor = email_files_processor               
        self.accumulate_power = 0
        self.next_forecast_value = None
        self.turbine_status_neykovo = None
        self.power_neykovo = None
        self.wind_neykovo = None



    async def publish_data(self):        
        try:
            self.next_forecast_value = await self.email_processor.process_files()
            print(f"FORECAST PRINT: {self.next_forecast_value}")

            if self.next_forecast_value:
                if self.next_forecast_value == "NA":                    
                    if self.turbine_status_neykovo == 3:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data(command="stop")
                        self.turbine_status_neykovo = turbine_status.Value.Value 
                        self.power_neykovo = power_value.Value.Value
                        self.wind_neykovo = wind_value.Value.Value

                    else:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data()  
                        self.turbine_status_neykovo = turbine_status.Value.Value  
                        self.power_neykovo = power_value.Value.Value
                        self.wind_neykovo = wind_value.Value.Value 
                else:
                    if self.turbine_status_neykovo == 2:
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data(command="start")
                        self.turbine_status_neykovo = turbine_status.Value.Value
                        self.power_neykovo = power_value.Value.Value
                        self.wind_neykovo = wind_value.Value.Value
                        
                    else:                        
                        wind_value, power_value, turbine_status = await self.opcua_client.read_data()                        
                        self.turbine_status_neykovo = turbine_status.Value.Value
                        self.power_neykovo = power_value.Value.Value
                        self.wind_neykovo = wind_value.Value.Value
            
            print(f'Turbine Status: {self.turbine_status_neykovo}')
            print(f'Power: {self.power_neykovo} kW')
            await self.blynk_send_power()
            await self.blynk_send_wind()
            await self.blynk_publish_status()
            await self.blynk_publish_accumulate()
            await self.blynk_send_forecast()
        
        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


    async def blynk_send_power(self):        
        url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v10={self.power_neykovo}"  # Neykovo  
        async with aiohttp.ClientSession() as session:
            async with session.get(url_power) as response:
                if response.status == 200:
                    pass   
    
    async def blynk_send_wind(self):
        url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v11={self.wind_neykovo}" # Aris
        async with aiohttp.ClientSession() as session:
            async with session.get(url_wind) as response:
                if response.status == 200:
                    pass    

    
    async def blynk_send_forecast(self):
        if self.next_forecast_value and self.next_forecast_value != "NA":
            value_published_to_blynk = self.next_forecast_value*1000 
        else:
            value_published_to_blynk = 0
        url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v9={value_published_to_blynk}" #V9 Neykovo V2 Aris
        async with aiohttp.ClientSession() as session:
            async with session.get(url_forecast) as response:
                if response.status == 200:
                    pass

    async def blynk_publish_accumulate(self):
        current_minute = datetime.now().minute      
        if current_minute % 15 == 0:
            self.accumulate_power = 0            
        self.accumulate_power += int(self.power_neykovo)
        url_neykovo_accumulate = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v8={self.accumulate_power/60}"  # Neykovo  
        async with aiohttp.ClientSession() as session:
            async with session.get(url_neykovo_accumulate) as response:
                if response.status == 200:
                    pass  

    async def blynk_publish_status(self):
        #publish turbine status
        if self.turbine_status_neykovo == 3:  
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v17=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        pass 
        else:
            url = "https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v17=0"
            async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            pass  




async def main():
    cert_base = Path(__file__).parent    

    url_power = "opc.tcp://10.126.253.1:62550/DataAccessServer"    
    wind_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WMET01.HorWdSpd'    
    power_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.W'    
    status_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurSt'
    #start/stop
    start_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurStrOp'
    stop_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurStopOp'
    opcua_client = OPCUAClient(        
        url = url_power,     
        client_app_uri="urn:freeopcua:client",
        cert_path=cert_base / "my_cert.pem",
        private_key_path=cert_base / "my_private_key.pem",
        wind_node = wind_node_neykovo,
        power_node = power_node_neykovo,
        status_node = status_node_neykovo,
        start_node = start_node_neykovo,
        stop_node = stop_node_neykovo
    )
    await opcua_client.setup()
    file_forecast_processor = FileManager("neykovo") 
    gmail_processor = ForecastProcessor()   
    scheduler = AsyncIOScheduler()    
    publisher = DataPublisher(opcua_client, gmail_processor, file_forecast_processor)

    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    

    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=10, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=11, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=12, minute=15))  

    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=15, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=16, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=17, minute=0))


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