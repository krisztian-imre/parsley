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

# Use the existing assistant ID
assistant_id = 'asst_QArsWr5MGsrqgy7UJraQlIMW'

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, instruction):
    try:
        # Create a new thread for each interaction
        thread = client.beta.threads.create()

        # Combine the content and instruction to send it as a single user message
        full_message = f"{instruction}\n\n{content}"

        # Send the content as a message
        message = client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=full_message
        )

        # Poll the LLM with the existing assistant
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread.id,
            assistant_id=assistant_id
        )

        # Check the run status
        if run.status == "completed":
            messages = client.beta.threads.messages.list(thread_id=thread.id)
            for message in messages:
                if message.role == "assistant":
                    response = message.content[0].text.value  # Extract the response
                    # Delete the thread after we have the response
                    client.beta.threads.delete(thread_id=thread.id)
                    return response

        # Delete the thread if the run is not completed or no response is found
        client.beta.threads.delete(thread_id=thread.id)
        print("WRONG: No valid response from assistant.")
        return ""

    except Exception as e:
        print(f"Error during interaction with assistant: {e}")
        return ""

# Function to handle list formatting (coins, pairs, market types)
def format_as_list(response):
    if response:
        # Assuming LLM returns comma-separated values, split and format as list
        return [item.strip() for item in response.split(',')]
    return []

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

    # Get LLM responses for each instruction by sending the instruction as part of the message
    llm_summary = get_llm_response(content, instruction_summary)  # Use summary instruction
    llm_category = get_llm_response(content, instruction_category)  # Use category instruction
    llm_coin = get_llm_response(content, instruction_coin)  # Use coin instruction
    llm_pair = get_llm_response(content, instruction_pair)  # Use pair instruction
    llm_market_type = get_llm_response(content, instruction_market_type)  # Use market-type instruction
    llm_action_datetime = get_llm_response(content, instruction_action_datetime)  # Use action datetime instruction
    llm_priority = get_llm_response(content, instruction_priority)  # Use priority instruction

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
        #'in_category': row['in_category'],
        #'parse_datetime': row['parse_datetime'],
        #'publish_datetime': row['publish_datetime'],
        'link': row['link']
        #'title': row['title'],
        #'body': row['body']
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
