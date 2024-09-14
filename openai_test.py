# File: openai_test

import pandas as pd
import openai
import os

# Set up OpenAI API key
os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

client = openai.OpenAI()

with open('instruction.txt', 'r') as file:
    instruction = file.read()


# Create the assistant with the specified instructions
assistant = client.beta.assistants.create(
    name="CryptoExchangeGuru",
    instructions=instruction,
    tools=[{"type": "code_interpreter"}],
    model="gpt-4o",
    temperature=0
)

# Load the CSV file
data = pd.read_csv('gateio_articles.tsv', sep='\t')


# Iterate through each row and send the 'content' as a message
for index, row in data.iterrows():
    content = row['body']

    # Create a new thread for interaction
    thread = client.beta.threads.create()

    # Send the announcement message
    message = client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=content
    )

    # Create and poll the assistant run
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread.id,
        assistant_id=assistant.id
    )

    print("Run completed with status: " + run.status)

    if run.status == "completed":
        messages = client.beta.threads.messages.list(thread_id=thread.id)

        print("messages: ")
        for message in messages:
            if message.role == "assistant":
                print(message.content[0].text.value)
