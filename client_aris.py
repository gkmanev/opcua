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
# modbus tcp:
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.server.async_io import ModbusTcpServer

import logging
logging.basicConfig(level=logging.INFO)


class DataPublisher:
    def __init__(self, opcua_client, gmail_preocessing_service, email_files_processor, dam_price_processor, context, gmail_service) -> None:
        self.opcua_client = opcua_client
        self.gmail_service = gmail_preocessing_service
        self.email_processor = email_files_processor
        self.dam_price_processor = dam_price_processor        
        self.accumulate_power = 0
        self.next_forecast_value = None
        self.turbine_status_aris = None
        self.power_aris = None
        self.wind_aris = None
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
            self.turbine_status_aris = turbine_status.Value.Value 
            self.power_aris = power_value.Value.Value
            self.wind_aris = wind_value.Value.Value
            logging.info(f"FORECAST PRINT: {self.next_forecast_value}")
            logging.info(f'Turbine Status: {self.turbine_status_aris} ')
            logging.info(f'Power Aris: {to_num(self.power_aris)} kW')
            logging.info(f'Wind Aris: {to_num(self.wind_aris)} m/s') 

            if self.next_forecast_value is None:
                if self.is_email_send == False:
                    await self.send_warning_email()    
                    self.is_email_send = True                    
                    
            else: 
                if self.next_forecast_value == "NA" or self.next_forecast_value == "N/A":                    
                    if self.turbine_status_aris == 3:
                       
                        await self.opcua_client.read_data(command="stop")    

                else:                   
                    if self.turbine_status_aris == 3:
                                
                        if to_num(self.wind_aris) >= 5 and to_num(self.power_aris) > 1:                                            
                            self.is_email_send = False

                        if (self.wind_aris is not None and int(self.wind_aris) > 5 and self.power_aris is not None and int(self.power_aris) <= 1) or (
                            self.wind_aris is not None and int(self.wind_aris) > 5 and self.power_aris is None
                        ):                         
                            if self.is_email_send == False:
                                await self.send_warning_email()
                            self.is_email_send = True
                        if self.wind_aris is None or self.power_aris is None:
                            if self.is_email_send == False:
                                await self.send_warning_email()
                                self.is_email_send = True

                    elif self.turbine_status_aris == 2:
                        
                        await self.opcua_client.read_data(command="start")
                    
                    elif self.turbine_status_aris == 1:
                        if self.is_email_send == False:
                            await self.send_warning_email()
                            self.is_email_send = True
                    else:
                        if self.is_email_send == False:
                            await self.send_warning_email()
                            self.is_email_send = True   

            # Handle negative values - set to 0 if negative, ensure within 16-bit range
            power_safe = max(0, min(65535, int(self.power_aris))) if self.power_aris is not None else 0
            wind_safe = max(0, min(65535, int(self.wind_aris))) if self.wind_aris is not None else 0
            
            # Optional: Log when values are clamped
            if self.power_aris is not None and int(self.power_aris) != power_safe:
                print(f"Power value adjusted: {int(self.power_aris)} -> {power_safe}")
            if self.wind_aris is not None and int(self.wind_aris) != wind_safe:
                print(f"Wind value adjusted: {int(self.wind_aris)} -> {wind_safe}")

            # Write safe values to Modbus registers
            self.context[0x00].setValues(3, 0, [power_safe, wind_safe])

            await self.blynk_send_power()
            await self.blynk_send_wind()
            await self.blynk_publish_status()
            #await self.blynk_publish_accumulate()
            await self.blynk_send_forecast()
            
             

        except ua.UaStatusCodeError as e:
            print(f"OPC UA Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    

    async def get_price(self):
        price = await self.dam_price_processor.get_price_prev_quarter_shifted(country="BG", contract="A01")
        if not price:
            logging.info("There is no price fetched!!!")
            return
        logging.info(f"Energy Price is: {price}")
        
        async with aiohttp.ClientSession() as session:
            url_price = "https://api.datacake.co/integrations/api/e823ba1a-e4df-4b2e-b0eb-545a30b47e3f/"
            payload = {
                "device": "c4e5dfae-76b2-4863-96af-e45eef38f9b8",
                "Price": price
            }
            # Set appropriate headers
            headers = {
                "Content-Type": "application/json"
            }
            try:
                async with session.post(url_price, headers=headers, json=payload) as response:
                    if response.status == 200:
                        logging.info("Price sent successfully to Datacake")
                    else:
                        logging.error(f"Failed to send price")
            except Exception as e:
                logging.exception(f"Datacake request error: {e}")
        
            # Blynk API
            url_price_blynk = "https://fra1.blynk.cloud/external/api/update"
            params = {
                "token": "RDng9bL06n9TotZY9sNvssAYxIoFPik8",
                "pin": "v3",
                "value": price,
            }            
            try:
                async with session.get(url_price_blynk, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logging.error(f"Blynk update failed: {resp.status} {body}")
            except Exception as e:
                logging.exception(f"Blynk request error: {e}")
                

    async def blynk_send_power(self):

        async with aiohttp.ClientSession() as session:
            # Datacake API
            url_power = "https://api.datacake.co/integrations/api/e823ba1a-e4df-4b2e-b0eb-545a30b47e3f/"
            payload = {
                "device": "c4e5dfae-76b2-4863-96af-e45eef38f9b8",
                "Power": float(self.power_aris),
            }
            headers = {"Content-Type": "application/json"}
            
            try:
                async with session.post(url_power, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        logging.info("Power sent successfully to Datacake")
                    else:
                        body = await resp.text()
                        logging.error(f"Datacake send failed: {resp.status} {body}")
            except Exception as e:
                logging.exception(f"Datacake request error: {e}")
            
            # Blynk API
            url_power_blynk = "https://fra1.blynk.cloud/external/api/update"
            params = {
                "token": "RDng9bL06n9TotZY9sNvssAYxIoFPik8",
                "pin": "v4",
                "value": self.power_aris,
            }            
            try:
                async with session.get(url_power_blynk, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logging.error(f"Blynk update failed: {resp.status} {body}")
                    else:
                        logging.info("Power sent successfully to Blynk")
            except Exception as e:
                logging.exception(f"Blynk request error: {e}")


    async def blynk_send_wind(self):
        """Send wind data to Datacake and Blynk"""
        if not self.wind_aris:
            return
        
        logging.info(f"Wind is: {self.wind_aris}")
        
        async with aiohttp.ClientSession() as session:
            # Datacake API
            url_wind = "https://api.datacake.co/integrations/api/e823ba1a-e4df-4b2e-b0eb-545a30b47e3f/"
            payload = {
                "device": "c4e5dfae-76b2-4863-96af-e45eef38f9b8",
                "Wind": float(self.wind_aris),
            }
            headers = {"Content-Type": "application/json"}
            
            try:
                async with session.post(url_wind, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        logging.info("Wind sent successfully to Datacake")
                    else:
                        body = await resp.text()
                        logging.error(f"Datacake send failed: {resp.status} {body}")
            except Exception as e:
                logging.exception(f"Datacake request error: {e}")
            
            # Blynk API
            url_wind_blynk = "https://fra1.blynk.cloud/external/api/update"
            params = {
                "token": "RDng9bL06n9TotZY9sNvssAYxIoFPik8",
                "pin": "v5",
                "value": self.wind_aris,
            }
            
            try:
                async with session.get(url_wind_blynk, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logging.error(f"Blynk update failed: {resp.status} {body}")
                    else:
                        logging.info("Wind sent successfully to Blynk")
            except Exception as e:
                logging.exception(f"Blynk request error: {e}")

    async def blynk_send_forecast(self):
        
        if self.next_forecast_value != "NA" or self.next_forecast_value != "N/A":

            value_published_to_blynk = self.next_forecast_value*1000 
        else:
            value_published_to_blynk = 0
        url_forecast = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v2={value_published_to_blynk}" #V9 Neykovo V2 Aris
        async with aiohttp.ClientSession() as session:
            async with session.get(url_forecast) as response:
                if response.status == 200:
                    pass      
    
    # async def blynk_publish_accumulate(self):
    #     if not self.power_aris:
    #         return
    #     current_minute = datetime.now().minute      
    #     if current_minute % 15 == 0:
    #         self.accumulate_power = 0            
    #     self.accumulate_power += int(self.power_aris)
    #     url_aris_accumulate = f"https://fra1.blynk.cloud/external/api/batch/update?token=RDng9bL06n9TotZY9sNvssAYxIoFPik8&v1={self.accumulate_power/60}"  # Aris  
    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(url_aris_accumulate) as response:
    #             if response.status == 200:
    #                 pass  

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
    
    async def send_warning_email(self):
        await self.gmail_service.email_georgi(
            subject="Warning Aris Error",
            body_text="Aris Problem !!!!!!",
        )
        await self.gmail_service.email_rali(
            subject="Warning Aris Error",
            body_text="Aris Problem !!!!!!",
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
    

    url_aris = "opc.tcp://10.126.252.1:62550/DataAccessServer"
    wind_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd'
    power_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W'
    status_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurSt' 
    #start/stop
    start_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStrOp'
    stop_node_aris = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp'  
    opcua_client = OPCUAClient(        
        url = url_aris,     
        client_app_uri="urn:example.org:FreeOpcUa:python-opcua", # Aris
        cert_path=cert_base / "my_cert_last.pem",
        private_key_path=cert_base / "my_private_key_last.pem",
        wind_node = wind_node_aris,
        power_node = power_node_aris,
        status_node = status_node_aris,
        start_node = start_node_aris,
        stop_node = stop_node_aris
    )
    await opcua_client.setup()
    gmail_service = GmailService()
    dam_price = PriceProcessor()
    file_forecast_processor = FileManager("aris")    
    gmail_processor = ForecastProcessor()
    scheduler = AsyncIOScheduler()         
    publisher = DataPublisher(opcua_client, gmail_processor, file_forecast_processor, dam_price, context, gmail_service)
    asyncio.create_task(publisher.init_modbus_server())
    scheduler.add_job(publisher.publish_data, IntervalTrigger(minutes=1))
    #scheduler.add_job(publisher.turbine_control, IntervalTrigger(minutes=1))  
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=10, minute=10))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=11, minute=41))
    scheduler.add_job(gmail_processor.proceed_forecast, CronTrigger(hour=13, minute=2))  

    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=15, minute=0))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=16, minute=30))
    scheduler.add_job(partial(gmail_processor.proceed_forecast, clearing=True), CronTrigger(hour=17, minute=8))

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