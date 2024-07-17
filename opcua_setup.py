import asyncio
import logging
import asyncua
from asyncua import Client as UA_Client, ua

# # Setup logging
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')


class OPCUAClient:
    def __init__(self, url, client_app_uri, cert_path, private_key_path, wind_node, power_node, status_node, start_node, stop_node):
        self.client = UA_Client(url=url)
        self.client.application_uri = client_app_uri
        self.cert_path = cert_path
        self.private_key_path = private_key_path
        self.wind_node = wind_node
        self.power_node = power_node
        self.status_node = status_node
        self.start_node = start_node
        self.stop_node = stop_node

    async def setup(self):
        await self.client.set_security_string(f"Basic256,SignAndEncrypt,{self.cert_path},{self.private_key_path}")
        await self.client.load_client_certificate(self.cert_path)
        await self.client.load_private_key(self.private_key_path)

    async def read_data(self):
        try:
            async with self.client:            
                get_wind_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd')
                print(f" PRRRRRRRRINTTTTT WIND: {get_wind_node}")
                wind_value = await get_wind_node.read_data_value()
                
                get_power_node = self.client.get_node(self.power_node)
                power_value = await get_power_node.read_data_value()
                
                get_status_node = self.client.get_node(self.status_node)
                status_value = await get_status_node.read_data_value()

                stop_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp')
                await stop_node.set_value(ua.Variant(True, ua.VariantType.Boolean))
               


                return wind_value, power_value, status_value
        except asyncua.ua.UaStatusCodeError as e:
            _logger.error(f"OPC UA status code error: {e}")
        except asyncio.CancelledError:
            _logger.error("Task was cancelled")
        except Exception as e:
            _logger.error(f"An unexpected error occurred: {e}")
        
    # async def send_stop_start_command(self, command):
            
        
    #     stop_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp')
    #     print(f"SSSSSSSSSSSSSSSSSSSTTTTTTTTTTTOP:{stop_node}")
    #     await stop_node.set_value(ua.Variant(True, ua.VariantType.Boolean))
    #         # await stop_node.set_value(ua.Variant(True, ua.VariantType.Boolean))
    #         # #Start
    #         # start_node = self.client.get_node(self.start_node)
              

            
                  