# File: openai_workaround.py

import requests
import os
import json

# Set up OpenAI API key
api_key = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

# Define the assistant ID and URL
assistant_id = 'asst_9fKPDtcx82rtwPVlmLXRks8Q'
url = "https://api.openai.com/v2/assistants/{assistant_id}"

# New parameters for the assistant
updated_instructions = """
You are a helpful assistant that provides concise and informative responses.
Please ensure the responses are friendly and easy to understand.
"""
new_temperature = 0.7
new_top_p = 0.9

# Set up the request headers with the authorization and beta version header
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json",
    "OpenAI-Beta": "assistants=v2"  # Pass the beta header to use the latest API version
}

# Define the updated payload
payload = {
    "instructions": updated_instructions,
    "temperature": new_temperature,
    "top_p": new_top_p
}

# Make the PATCH request to update the assistant
response = requests.patch(url, headers=headers, data=json.dumps(payload))

# Check and print the response
if response.status_code == 200:
    print("Assistant updated successfully!")
    print(response.json())  # Print the response details
else:
    print(f"Failed to update assistant: {response.status_code}")
    print(response.text)