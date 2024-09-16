# File: gateio_get_processed.py

import pandas as pd
import openai
import os
from tqdm import tqdm  # Import tqdm for progress bar

# Set up OpenAI API key
os.environ['OPENAI_API_KEY'] = 'sk-proj-vCcWskCtsmbnloDfRC6XT3BlbkFJURTh97LuwNhvGOMqgWgh'

client = openai.OpenAI()

# Load instructions for different tasks
with open('instruction-summary.txt', 'r') as file:
    instruction_summary = file.read()

with open('instruction-category.txt', 'r') as file:
    instruction_category = file.read()

with open('instruction-coin.txt', 'r') as file:
    instruction_coin = file.read()

with open('instruction-pair.txt', 'r') as file:
    instruction_pair = file.read()

with open('instruction-market-type.txt', 'r') as file:
    instruction_market_type = file.read()

with open('instruction-action-datetime.txt', 'r') as file:
    instruction_action_datetime = file.read()

with open('instruction-priority.txt', 'r') as file:
    instruction_priority = file.read()

# Function to interact with the LLM for a specific instruction using an existing thread
def get_llm_response(content, instruction, thread_id):
    # Send the content as a message
    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=content
    )
    
    # Poll the LLM with the instruction
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread_id,
        assistant_id=assistant.id
    )
    
    # Check if the run completed and extract response
    if run.status == "completed":
        messages = client.beta.threads.messages.list(thread_id=thread_id)
        for message in messages:
            if message.role == "assistant":
                response = message.content[0].text.value  # Extract the response
                return response
    return "[WRONG]"

# Load the TSV file
data = pd.read_csv('gateio_articles.tsv', sep='\t')

# Filter the rows where 'llm_processed' is 'No'
unprocessed_data = data[data['llm_processed'] == 'No']

# Prepare for saving processed data
output_file = 'gateio_articles_processed.tsv'
file_exists = os.path.isfile(output_file)

# Initialize a list to store processed records
processed_data = []

# Use tqdm to display a progress bar while iterating through unprocessed data
for index, row in tqdm(unprocessed_data.iterrows(), total=unprocessed_data.shape[0], desc="Processing records"):
    content = row['body']
    
    # Create the assistant once for this record
    assistant = client.beta.assistants.create(
        name="CryptoExchangeInvestigator",
        instructions=instruction_summary,  # Can use summary as base, updated later
        tools=[{"type": "code_interpreter"}],
        model="gpt-4o",
        temperature=0
    )

    # Create a new thread for this record
    thread = client.beta.threads.create()

    # Get LLM responses for each instruction using the same thread
    llm_summary = get_llm_response(content, instruction_summary, thread.id)  # Use summary instruction
    llm_category = get_llm_response(content, instruction_category, thread.id)  # Use category instruction
    llm_coin = get_llm_response(content, instruction_coin, thread.id)  # Use coin instruction
    llm_pair = get_llm_response(content, instruction_pair, thread.id)  # Use pair instruction
    llm_market_type = get_llm_response(content, instruction_market_type, thread.id)  # Use market-type instruction
    llm_action_datetime = get_llm_response(content, instruction_action_datetime, thread.id)  # Use action datetime instruction
    llm_priority = get_llm_response(content, instruction_priority, thread.id)  # Use priority instruction
    
    # Delete the thread after all responses are retrieved
    client.beta.threads.delete(thread_id=thread.id)

    # Mark the record as processed
    data.at[index, 'llm_processed'] = 'Yes'  # Update the original data to mark it as processed
    
    # Add a new row with all existing features + new LLM-generated features
    processed_record = {
        'exchange': row['exchange'],
        'llm_summary': llm_summary,
        'llm_category': llm_category,
        'llm_coin': llm_coin,
        'llm_pair': llm_pair,
        'llm_market_type': llm_market_type,
        'llm_action_datetime': llm_action_datetime,
        'llm_priority': llm_priority,
        'ical_processed': 'No',  # Default 'No'
        'link': row['link']
    }
    
    # Append the processed record to the list
    processed_data.append(processed_record)

# Convert the processed records into a DataFrame
processed_df = pd.DataFrame(processed_data)

# Save the data to the 'gateio_LLM_processed.tsv' file
if not file_exists:
    # If the file does not exist, create it
    processed_df.to_csv(output_file, sep='\t', index=False)
else:
    # If the file exists, append to it
    processed_df.to_csv(output_file, sep='\t', index=False, mode='a', header=False)

# Save the updated original file with 'llm_processed' marked as 'Yes' for processed records
data.to_csv('gateio_articles.tsv', sep='\t', index=False)

print(f"Data processed and saved to {output_file}")
