import asyncio
import aiofiles
from datetime import datetime, date, timedelta
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from base64 import urlsafe_b64decode
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from mimetypes import guess_type as guess_mime_type
import os
import xlrd
import pytz

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

class GmailService:
    def __init__(self, token_file="token.json", credentials_file="credentials.json"):
        self.service = self.authenticate(token_file, credentials_file)

    def authenticate(self, token_file, credentials_file):
        creds = None
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_file, "w") as token:
                token.write(creds.to_json())
        return build('gmail', 'v1', credentials=creds)

    async def search_messages(self, query):
        result = self.service.users().messages().list(userId='me', q=query).execute()
        messages = result.get('messages', [])
        while 'nextPageToken' in result:
            page_token = result['nextPageToken']
            result = self.service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
            messages.extend(result.get('messages', []))
        return messages

    async def parse_parts(self, service, parts, folder_name, message):
        if parts:
            for part in parts:
                filename = part.get("filename")
                mimeType = part.get("mimeType")
                body = part.get("body")
                data = body.get("data")
                file_size = body.get("size")
                part_headers = part.get("headers")

                if part.get("parts"):
                    await self.parse_parts(service, part.get("parts"), folder_name, message)
                else:
                    for part_header in part_headers:
                        part_header_name = part_header.get("name")
                        part_header_value = part_header.get("value")
                        if part_header_name == "Content-Disposition":
                            if "attachment" in part_header_value:
                                attachment_id = body.get("attachmentId")
                                attachment = service.users().messages().attachments().get(id=attachment_id, userId='me', messageId=message['id']).execute()
                                data = attachment.get("data")
                                filepath = os.path.join(folder_name, filename)
                                if data:
                                    async with aiofiles.open(filepath, "wb") as f:
                                        await f.write(urlsafe_b64decode(data))

    async def read_message(self, message):
        msg = self.service.users().messages().get(userId='me', id=message['id'], format='full').execute()
        payload = msg['payload']
        headers = payload.get("headers")
        parts = payload.get("parts")
        folder_name = "email"
        mail_hour = None
        if headers:
            for header in headers:
                if header.get("name").lower() == "subject":
                    folder_name = "enProMail"
                elif header.get("name").lower() == "date":
                    date = header.get("value")
                    local_tz = pytz.timezone('Europe/Sofia')
                    date_obj = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z")
                    date_obj = date_obj.astimezone(local_tz)
                    mail_hour = date_obj.hour
               
        print(f"mail_hour:{mail_hour}")
        if mail_hour and mail_hour >=8: #Filter additional mails with clearings from EnPro
            await self.parse_parts(self.service, parts, folder_name, message)
            print("=" * 50)

class FileManager:
    def get_file_name(self, folder):
        tomorrow = date.today() #- timedelta(days=1)
        d1 = tomorrow.strftime("%d.%m.%Y")
        st = folder.split("_")[1].split("xls")[0]
        file_date = st.split('.')
        d = file_date[0]
        m = file_date[1]
        y = file_date[2]
        name_date = d + "." + m + "." + y
        print(f"Name Date: {name_date} || {d1}")
        return name_date == d1

    async def process_files(self):
        fn = "enProMail"
        for root, dirs, files in os.walk(fn):
            xlsfiles = [f for f in files if f.endswith('.xls')]
            for xlsfile in xlsfiles:
                my_file = self.get_file_name(xlsfile)
                if my_file:
                    filepath = os.path.join(fn, xlsfile)
                    excel_workbook = xlrd.open_workbook(filepath)
                    excel_worksheet = excel_workbook.sheet_by_index(0)
                    xl_asset = excel_worksheet.cell_value(4, 0)
                    xl_date = xlrd.xldate_as_datetime(excel_worksheet.cell_value(1, 1), 0).date()
                    xl_date_time = str(xl_date) + "T01:15:00"
                    testlist = []
                    neykovo_list = []
                    period = (24 * 4) + 1
                    i = 1
                    while i < period:
                        i += 1
                        xl_power = excel_worksheet.cell_value(4, 3 + i)
                        xl_neykovo_power = excel_worksheet.cell_value(5, 3 + i)
                        testlist.append(xl_power)
                        neykovo_list.append(xl_neykovo_power)
                    timeIndex = pd.date_range(start=xl_date_time, periods=period - 1, freq="0H15T")
                    dfA = pd.DataFrame(testlist, index=timeIndex)
                    dfNeykovo = pd.DataFrame(neykovo_list, index=timeIndex)
                    dfA.index.name = 'Aris foreacast'
                    dfNeykovo.index.name = 'Power forecast'
                    dfA.columns = ['pow']
                    dfNeykovo.columns = ['power']                    
                    for row in dfA.itertuples():
                        timenow = datetime.now()
                        quarter_min = self.lookup_quarterly(timenow.minute)                           
                        if quarter_min == 0:
                            quarter_hour = timenow.hour + 1
                        else:
                            quarter_hour = timenow.hour                     
                        forecast_hour = row.Index.hour
                        forecast_min = row.Index.minute
                        if quarter_hour == forecast_hour and forecast_min == quarter_min:                            
                            power = row.pow
                            print(f"forecast_hour={forecast_hour}:{forecast_min} || quarter_hour={quarter_hour}:{quarter_min} || Real Time:{timenow.hour}:{timenow.minute} || Power:{power}")
                            return power
                            
    
    def lookup_quarterly(self, minutes):
        
        if 0 <= minutes <= 14:
            return 15
        elif 15 <= minutes <= 29:
            return 30
        elif 30 <= minutes <= 44:
            return 45
        elif 45 <= minutes <= 59:
            return 0
        else:
            raise ValueError("Minutes must be between 0 and 59")
   

class ForecastProcessor:
    def __init__(self):
        self.gmail_service = GmailService()
        self.file_manager = FileManager()

    async def proceed_forecast(self):
        now = datetime.now() 
        after_date = now.strftime("%Y/%m/%d")
        sender_email = "trading@energo-pro.bg"
        query_str = f"from:{sender_email} after:{after_date}"
        print(query_str)
        results = await self.gmail_service.search_messages(query_str)
        print(f"Found {len(results)} results.")
        for msg in results:
            await self.gmail_service.read_message(msg)

    async def prepare_files(self):
        await self.file_manager.process_files()

if __name__ == "__main__":
    processor = ForecastProcessor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(processor.proceed_forecast())
    loop.run_until_complete(processor.prepare_files())
