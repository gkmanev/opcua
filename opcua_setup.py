import asyncio
import logging
import asyncua
from asyncua import Client as UA_Client, ua

# # Setup logging
# logging.basicConfig(level=logging.INFO)
# _logger = logging.getLogger('asyncua')


class OPCUAClient:
    def __init__(self, url, client_app_uri, cert_path, private_key_path):
        self.client = UA_Client(url=url)
        self.client.application_uri = client_app_uri
        self.cert_path = cert_path
        self.private_key_path = private_key_path

    async def setup(self):
        await self.client.set_security_string(f"Basic256,SignAndEncrypt,{self.cert_path},{self.private_key_path}")
        await self.client.load_client_certificate(self.cert_path)
        await self.client.load_private_key(self.private_key_path)

    async def read_data(self):
        async with self.client:
            #wind_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WMET01.HorWdSpd')#Aris
            wind_node = self.client.get_node('ns=2;s=DA.Neykovo.WTG01.WMET01.HorWdSpd')#Power
            wind_value = await wind_node.read_data_value()

            #power_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.W')#Aris
            power_node = self.client.get_node('ns=2;s=DA.Neykovo.WTG01.WTUR01.W')#Power
            power_value = await power_node.read_data_value()

            return wind_value, power_value
        
    async def send_stop_start_command(self,command):
        print("Write to OPC")
        async with self.client:
            pass            
            #Stop
            # stop_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp')
            # await stop_node.set_value(ua.Variant(True, ua.VariantType.Boolean))
            #Start
            #start_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStrOp')
            # tag_node = ''
            # if command == 'start':
            #     tag_node = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStrOp'
            # elif command == 'stop':
            #     tag_node = 'ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurStopOp'
            # command_node = self.client.get_node(tag_node)
            # await command_node.set_value(ua.Variant(True, ua.VariantType.Boolean))           
 
    async def check_tourbine_status(self):
        async with self.client:
            #status_node = self.client.get_node('ns=2;s=DA.Rakovo Aris.WTG01.WTUR01.TurSt')#Aris   
            status_node = self.client.get_node('ns=2;s=DA.Neykovo.WTG01.WTUR01.TurSt') #Power 
            status_value = await status_node.read_data_value()
            return status_value
            
                  