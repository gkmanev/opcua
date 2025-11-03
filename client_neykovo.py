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
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.server.async_io import ModbusTcpServer
import logging
logging.basicConfig(level=logging.INFO)

class DataPublisher:
    def __init__(self, opcua_client, gmail_preocessing_service, email_files_processor, context, gmail_service) -> None:
        self.opcua_client = opcua_client
        self.gmail_service = gmail_preocessing_service
        self.email_processor = email_files_processor               
        self.accumulate_power = 0
        self.next_forecast_value = None
        self.turbine_status_neykovo = None
        self.power_neykovo = None
        self.wind_neykovo = None
        self.context = context
        self.gmail_service = gmail_service
        self.is_email_send = False


    async def init_modbus_server(self):
        server = ModbusTcpServer(self.context, address=("0.0.0.0", 5020))
        await server.serve_forever()



    async def publish_data(self):   
        
        def to_num(x):
            try:
                # handle strings like "5.2" too
                return float(x)
            except (TypeError, ValueError):
                return None      
                
        try:            
            self.next_forecast_value = await self.email_processor.process_files()            
            wind_value, power_value, turbine_status = await self.opcua_client.read_data()   
            self.turbine_status_neykovo = turbine_status.Value.Value 
            self.power_neykovo = power_value.Value.Value
            self.wind_neykovo = wind_value.Value.Value
            logging.info(f"FORECAST PRINT: {self.next_forecast_value}")
            logging.info(f'Turbine Status: {self.turbine_status_neykovo}')
            logging.info(f'Power Neykovo: {self.power_neykovo} kW')
            logging.info(f'Wind Neykovo: {self.wind_neykovo} m/s')

            if self.next_forecast_value is None:
                if self.is_email_send == False:
                    await self.send_warning_email()    
                    self.is_email_send = True 
                
            else:
                if self.next_forecast_value == "NA" or self.next_forecast_value == "N/A":                    
                    if self.turbine_status_neykovo == 3:
                        await self.opcua_client.read_data(command="stop")                   
                else:                    
                    if self.turbine_status_neykovo == 3:                        
                        if to_num(self.wind_neykovo) >= 5 and to_num(self.power_neykovo) > 1:                                                                
                            self.is_email_send = False
                        
                        if (self.wind_neykovo is not None and int(self.wind_neykovo) > 5 and self.power_neykovo is not None and int(self.power_neykovo) <= 1) or (
                            self.wind_neykovo is not None and int(self.wind_neykovo) > 5 and self.power_neykovo is None
                        ):                         
                            if self.is_email_send == False:
                                await self.send_warning_email()
                                self.is_email_send = True
                        if self.wind_neykovo is None or self.power_neykovo is None:
                            if self.is_email_send == False:
                                await self.send_warning_email()
                                self.is_email_send = True

                    elif self.turbine_status_neykovo == 2:
                        await self.opcua_client.read_data(command="start")

                    elif self.turbine_status_neykovo == 1:
                        if self.is_email_send == False:
                            await self.send_warning_email()
                            self.is_email_send = True
                    
                    else:                       
                        if self.is_email_send == False:
                            await self.send_warning_email()
                            self.is_email_send = True                                   
            


            # Handle negative values - set to 0 if negative, ensure within 16-bit range
            power_safe = max(0, min(65535, int(self.power_neykovo))) if self.power_neykovo is not None else 0
            wind_safe = max(0, min(65535, int(self.wind_neykovo))) if self.wind_neykovo is not None else 0
            
            # Optional: Log when values are clamped
            if self.power_neykovo is not None and int(self.power_neykovo) != power_safe:
                print(f"Power value adjusted: {int(self.power_neykovo)} -> {power_safe}")
            if self.wind_neykovo is not None and int(self.wind_neykovo) != wind_safe:
                print(f"Wind value adjusted: {int(self.wind_neykovo)} -> {wind_safe}")

            # Write safe values to Modbus registers
            self.context[0x00].setValues(3, 0, [power_safe, wind_safe])

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
        if not self.power_neykovo:
            return  
        print(f"power neykovo: {self.power_neykovo}")
        url_power = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v10={self.power_neykovo}"  # Neykovo  
        async with aiohttp.ClientSession() as session:
            async with session.get(url_power) as response:
                if response.status == 200:
                    pass   
    
    async def blynk_send_wind(self):
        if not self.wind_neykovo:
            return
        url_wind = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v11={self.wind_neykovo}" # Neykovo
        async with aiohttp.ClientSession() as session:
            async with session.get(url_wind) as response:
                if response.status == 200:
                    pass    

    
    async def blynk_send_forecast(self):
        
        if self.next_forecast_value != "NA" or self.next_forecast_value != "N/A":

            value_published_to_blynk = self.next_forecast_value*1000 
        else:
            value_published_to_blynk = 0
        url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v9={value_published_to_blynk}" #V9 Neykovo V2 Neykovo
        async with aiohttp.ClientSession() as session:
            async with session.get(url_forecast) as response:
                if response.status == 200:
                    pass

    async def blynk_publish_accumulate(self):
        if not self.power_neykovo:
            return
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
    
    async def send_warning_email(self):
        await self.gmail_service.email_georgi(
            subject="Warning Power Tourbine Error",
            body_text="Power Tourbine Problem !!!!!!",
        )
        await self.gmail_service.email_rali(
            subject="Warning Power Tourbine Error",
            body_text="Power Tourbine Problem !!!!!!",
        )




async def main():
    cert_base = Path(__file__).parent   

    #Modbus TCP:
    store = ModbusSlaveContext(
        di=ModbusSequentialDataBlock(0, [0]*100),
        co=ModbusSequentialDataBlock(0, [0]*100),
        hr=ModbusSequentialDataBlock(0, [0]*100),
        ir=ModbusSequentialDataBlock(0, [0]*100),
        zero_mode=True
    )
    context = ModbusServerContext(slaves={0x00: store}, single=False) 

    url_power = "opc.tcp://10.126.253.1:62550/DataAccessServer"    
    wind_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WMET01.HorWdSpd'    
    power_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.W'    
    status_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurSt'
    #start/stop
    start_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurStrOp'
    stop_node_neykovo = 'ns=2;s=DA.Neykovo.WTG01.WTUR01.TurStopOp'
    opcua_client = OPCUAClient(        
        url = url_power,     
        client_app_uri="urn:example.org:FreeOpcUa:python-opcua", #now match the cert on neykovo
        cert_path=cert_base / "my_cert_last.pem",
        private_key_path=cert_base / "my_private_key_last.pem",
        wind_node = wind_node_neykovo,
        power_node = power_node_neykovo,
        status_node = status_node_neykovo,
        start_node = start_node_neykovo,
        stop_node = stop_node_neykovo
    )
    await opcua_client.setup()
    gmail_service = GmailService()
    file_forecast_processor = FileManager("neykovo") 
    gmail_processor = ForecastProcessor()   
    scheduler = AsyncIOScheduler()    
    publisher = DataPublisher(opcua_client, gmail_processor, file_forecast_processor, context, gmail_service)
    asyncio.create_task(publisher.init_modbus_server())

    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    

    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=10, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=11, minute=15))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=13, minute=7))  

    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=15, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=16, minute=50))
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