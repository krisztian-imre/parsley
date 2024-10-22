#File: gateio_get_json.py

import pandas as pd
import openai
from openai._exceptions import RateLimitError, APIConnectionError, OpenAIError
import os
import shutil
import datetime
from tqdm import tqdm
import json
import logging
import time
import signal  # Import signal for global timeout management
import re

# Timeout handler to catch unresponsive scripts
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException()

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, assistant_id, max_retries=3, backoff_factor=2, timeout=60):
    retries = 0

    logging.error(f"\nAssistant ID: {assistant_id}\n\nInput Content:\n{content}")

    while retries < max_retries:

        # Create a new thread for each interaction
        thread = client.beta.threads.create()
        
        try:
            # Set an alarm for the timeout to prevent script from hanging
            signal.alarm(timeout)
            
            # Send the content as a message
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=[{"type": "text", "text": content}]
            )

            logging.info(f"\nThread ID: {thread.id}\nContent:\n{content}") 

            # Poll the LLM with the existing assistant
            run = client.beta.threads.runs.create_and_poll(
                thread_id=thread.id,
                assistant_id=assistant_id
            )

            # Reset the alarm after successful response
            signal.alarm(0)

            # Check the run status
            if run.status == "completed":
                messages = client.beta.threads.messages.list(thread_id=thread.id)
                logging.info(f"\nAssistant ID: {assistant_id}\n\nMessages:\n{messages}")
                for message in messages:
                    if message.role == "assistant" and hasattr(message, 'content'):
                        response = message.content[0].text.value
                        #response = response.replace('```json', '')  # Remove starting ```json marker
                        #response = response.replace('```', '')      # Remove ending ```
                        if response:
                            #logging.log(f"\nAssistant ID: {assistant_id}\nResponse:\n{response}")
                            return response
            else:
                logging.error(f"Unexpected run status: {run.status}")
                raise Exception("LLM run did not complete successfully")

        except TimeoutException:
            logging.error(f"Timeout occurred after {timeout} seconds")
            return "LLM_TIMEOUT"
        
        except json.JSONDecodeError as e:
            logging.error(f"JSON Decode Error: {e} - Content: {content}")
            return "LLM_ERROR"
        
        except RateLimitError as rate_limit_err:
            logging.error(f"Rate Limit Error: {rate_limit_err} - Content: {content}")
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"Rate limit exceeded, retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        except APIConnectionError as connection_err:
            logging.error(f"API Connection Error: {connection_err} - Content: {content}")
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"Connection issue, retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            continue

        except OpenAIError as openai_err:
            logging.error(f"OpenAI API Error: {openai_err} - Content: {content}")
            return "LLM_ERROR"

        except Exception as e:
            logging.error(f"Unexpected Error: {e} - Content: {content}")
            return "LLM_ERROR"

    # If all retries fail
    logging.error(f"Failed to process content after {max_retries} retries.")
    return "LLM_ERROR"

# Main function to handle the entire process
def get_json():
    # Load the TSV file into a pandas DataFrame
    df = pd.read_csv('gateio_article_collection.tsv', sep='\t')

    # Filter rows where 'llm_processed' is 'No'
    unprocessed_records = df[df['llm_processed'] == 'No']

    # Remove rows where either 'publish_datetime', 'body', etc. is an empty string
    unprocessed_records = unprocessed_records[unprocessed_records['body'].notna()]
    unprocessed_records = unprocessed_records[unprocessed_records['publish_datetime'].notna()]
    unprocessed_records = unprocessed_records[unprocessed_records['title'].notna()]
    unprocessed_records = unprocessed_records[unprocessed_records['link'].notna()]

    # Initialize an empty list to store raw responses and parsed responses
    raw_responses = []
    parsed_responses = []

    # Process each unprocessed record
    for index, row in tqdm(unprocessed_records.iterrows(), total=len(unprocessed_records)):
        
        exchange = row['exchange']
        publish_datetime = row['publish_datetime']
        title = row['title']
        link = row['link']
        body = row['body']
        body = re.sub(r'///+', '\n', body)

        # Construct the content to be sent to the LLM
        content = f"exchange_name: {exchange}\npublish_datetime: {publish_datetime}\narticle_title: {title}\narticle_link: {link}\narticle: {body}"

        if "Bi-Weekly Report" in title:
            # Use 'Crypto Event Info Extractor for Report' assistant ID
            assistant_id = 'asst_Xk1XKciwc63DdjIHIO3ljfmH'
        else:
            # Use 'Crypto Event Datetime Extractor' assistant ID
            assistant_id = 'asst_33sFfSIFStFOd5TPJvOKfy2h'

        # Get the response from the LLM
        response = get_llm_response(content, assistant_id)

        # Try to parse the response as JSON
        try:
            # Strip and load the JSON to validate it
            parsed_response = json.loads(response.strip())

        except json.JSONDecodeError as e:
            # Log the error and the problematic response, but don't mark as processed
            logging.error(f"Failed to parse JSON: {e} - Response: {response}")
            continue  # Move to the next article without marking this one as processed

        # If the article is not a report, send to the third assistant
        if "Bi-Weekly Report" not in title:
            # 'Crypto Event JSON Improver' assistant
            assistant_id = 'asst_CfFXkDtL6wiBKpPpIREesccm'
        
            json_content = json.dumps(parsed_response, indent=4)
            content = f"JSON: {json_content}\nAdditional data: {body}"

            # Get the response from the third assistant
            response = get_llm_response(content, assistant_id)

            logging.error(f"\nChain-of-Thought Response:\n{response}")

            # Try to parse the response as JSON
            try:
                # Strip and load the JSON to validate it
                parsed_response = json.loads(response.strip())
                
                # Append the raw and parsed responses to the respective lists
                raw_responses.append(response)
                parsed_responses.append(parsed_response)
                
                # If the response is valid JSON, mark the article as processed
                df.at[index, 'llm_processed'] = 'Yes'

            except json.JSONDecodeError as e:
                # Log the error and the problematic response, but don't mark as processed
                logging.error(f"Failed to parse JSON: {e} - Response: {response}")
                continue  # Move to the next article without marking this one as processed

        else:
            logging.error(f"\nReport Response:\n{response}")
            
            # Append the raw and parsed responses to the respective lists
            raw_responses.append(response)
            parsed_responses.append(parsed_response)

            # If it's a report, mark it as processed after the first assistant
            df.at[index, 'llm_processed'] = 'Yes'


    # Save the updated DataFrame to the TSV file
    df.to_csv('gateio_article_collection.tsv', sep='\t', index=False)

    # Save the parsed JSON objects as a valid JSON array
    # Get the current datetime in the format YYMMDD_HHMM
    current_datetime = datetime.datetime.now().strftime("%y%m%d_%H%M")

    # Specify the folder where the JSON file should be saved
    destination_folder = os.path.expanduser('~/parsley/Gateio_JSON_Process')

    # Create the filename with the current datetime appended
    filename = f'gateio_{current_datetime}.json'

    # Create the full path for the file
    file_path = os.path.join(destination_folder, filename)

    # Save the parsed JSON objects as a valid JSON array with the new filename
    with open(file_path, 'w') as f:
        json.dump(parsed_responses, f, indent=4)

    print(f"The JSON file has been saved as '{file_path}'.")

# Run the main function only when the script is executed directly
if __name__ == "__main__":

    # Load the TSV file into a pandas DataFrame
    df = pd.read_csv('gateio_article_collection.tsv', sep='\t')

    # Filter rows where 'llm_processed' is 'No'
    unprocessed_records = df[df['llm_processed'] == 'No']

    # Initialize an empty list to store raw responses and parsed responses
    raw_responses = []
    parsed_responses = []

    # Set up the signal for timeouts
    signal.signal(signal.SIGALRM, timeout_handler)

    # Define the source and destination directories
    source_folder = os.path.expanduser('~/parsley/Gateio_JSON_Process')
    destination_folder = os.path.expanduser('~/parsley/Gateio_JSON_Archive')

    # Create the source folder if it doesn't exist
    if not os.path.exists(source_folder):
        os.makedirs(source_folder)

    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Move all files from source to destination
    for filename in os.listdir(source_folder):
        source_path = os.path.join(source_folder, filename)
        destination_path = os.path.join(destination_folder, filename)
    
        if os.path.isfile(source_path):
            shutil.move(source_path, destination_path)

    print("All files moved from 'Process' to 'Archived'.")

    # Set up logging for error tracking
    logging.basicConfig(filename='gateio_errors.log', level=logging.ERROR)
    
    # Retrieve the OpenAI API key from the environment
    openai_api_key = os.getenv('OPENAI_API_KEY')
    
    if not openai_api_key:
        logging.error("OpenAI API key not found in environment variables.")
        raise EnvironmentError("OPENAI_API_KEY environment variable is not set. Please configure it before running the script.")
    
    # Initialize OpenAI client with the retrieved API key
    global client
    client = openai

    # You can now directly call get_json without needing a global variable
    get_json()
