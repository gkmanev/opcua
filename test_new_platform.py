import requests
import json

# Define the Datacake API endpoint URL
url = "https://api.datacake.co/integrations/api/10d4ce3f-5dd2-4554-8751-b5a139dd3cdf/"

# Compose the JSON payload as per the decoder's expected format
payload = {
    "device": "d7aa3e85-2c65-4da0-a6d9-c6f21d03ff99",
    "fileInUse": "TheNameOfTheFileInUse"
}

# Set appropriate headers
headers = {
    "Content-Type": "application/json"
}

# Send the POST request
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Print response status and content
print(f"Status Code: {response.status_code}")
print("Response Body:", response.text)
