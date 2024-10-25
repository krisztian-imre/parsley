#File: gateio_get_json_refactored.py

import os
import shutil
import datetime
import json
import time
import signal
import re
import logging
import pandas as pd
import openai
from openai._exceptions import RateLimitError, APIConnectionError, OpenAIError
import logger_setup

# Timeout handler to catch unresponsive scripts
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException()

# Function to interact with the LLM for a specific instruction using an existing assistant
def get_llm_response(content, assistant_id, max_retries=3, backoff_factor=2, timeout=60):
    retries = 0
    thread = client.beta.threads.create()
    while retries < max_retries:
        try:
            signal.alarm(timeout)
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=[{"type": "text", "text": content}]
            )
            run = client.beta.threads.runs.create_and_poll(
                thread_id=thread.id,
                assistant_id=assistant_id
            )
            signal.alarm(0)

            if run.status == "completed":
                messages = client.beta.threads.messages.list(thread_id=thread.id)
                for message in messages:
                    if message.role == "assistant" and hasattr(message, 'content'):
                        response = message.content[0].text.value
                        if response:
                            return check_for_nested_events(json.loads(response))
                    else:
                        logging.error(f"Unexpected run status: {run.status}")
                        return "LLM RESPONSE HAS NO CONTENT ERROR"

        except TimeoutException:
            logging.error(f"Timeout occurred after {timeout} seconds")
            return "LLM TIMEOUT ERROR"
        except json.JSONDecodeError as e:
            logging.error(f"JSON Decode Error: {e} - Content: {content}")
            return "INVALID JSON ERROR"
        except (RateLimitError, APIConnectionError) as retry_err:
            retries += 1
            sleep_time = backoff_factor ** retries
            logging.warning(f"{type(retry_err).__name__}: {retry_err}. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        except OpenAIError as openai_err:
            logging.error(f"OpenAI API Error: {openai_err} - Content: {content}")
            return "UNEXPECTED LLM ERROR"
        except Exception as e:
            logging.error(f"Unexpected Error: {e} - Content: {content}")
            return "UNEXPECTED OTHER ERROR"
    
    logging.error(f"Failed to process content after {max_retries} retries.")
    return "LLM_ERROR"

# Function to check for nested 'events' in the LLM response
def check_for_nested_events(parsed_response):
    if isinstance(parsed_response.get('events'), dict):
        if isinstance(parsed_response['events'].get('events'), list):
            parsed_response['events'] = parsed_response['events']['events']
    return parsed_response

# Main function to process articles and save JSON
def get_json():
    df = pd.read_csv('gateio_article_collection.tsv', sep='\t')
    unprocessed_records = df[df['llm_processed'] == 'No'].dropna(subset=['body', 'publish_datetime', 'title', 'link'])
    parsed_responses = []

    for index, row in unprocessed_records.iterrows():
        content = prepare_content(row)
        logging.info(f"Content for LLM:\n{content}")

        assistant_id = determine_assistant(row['title'])
        response = get_llm_response(content, assistant_id)
        logging.info(f"Response 1:\n{json.dumps(response, indent=4)}")

        if " ERROR" not in response:
            if "Bi-Weekly Report" not in row['title']:
                assistant_id = 'asst_CfFXkDtL6wiBKpPpIREesccm'
                response = get_llm_response(f"JSON:\n{json.dumps(response, indent=4)}\nAdditional data:\n{row['body']}", assistant_id)
                logging.info(f"Response 2:\n{json.dumps(response, indent=4)}")

                if " ERROR" in response:
                    logging.error(f"Error in second assistant response: {response}")
                    continue

            df.at[index, 'llm_processed'] = 'Yes'
            parsed_responses.append(response)
        else:
            logging.error(f"Error in LLM response: {response}")

    df.to_csv('gateio_article_collection.tsv', sep='\t', index=False)
    save_json(parsed_responses, source_folder)

# Helper function to prepare content for LLM input
def prepare_content(row):
    body_cleaned = re.sub(r'///+', '\n', row['body'])
    return (f"exchange_name: {row['exchange']}\n"
            f"publish_datetime: {row['publish_datetime']}\n"
            f"article_title: {row['title']}\n"
            f"article_link: {row['link']}\n"
            f"article: {body_cleaned}")

# Helper function to determine the assistant ID
def determine_assistant(title):
    return ('asst_Xk1XKciwc63DdjIHIO3ljfmH' if "Bi-Weekly Report" in title
            else 'asst_33sFfSIFStFOd5TPJvOKfy2h')

# Helper function to save JSON objects
def save_json(parsed_responses, source_folder):
    current_datetime = datetime.datetime.now().strftime("%y%m%d_%H%M")
    os.makedirs(destination_folder, exist_ok=True)
    file_path = os.path.join(source_folder, f'gateio_{current_datetime}.json')

    with open(file_path, 'w') as f:
        json.dump(parsed_responses, f, indent=4)

    logging.info(f"The JSON file has been saved as '{file_path}'.")

# Function to move files between directories
def move_files(source_folder, destination_folder):
    if not os.path.exists(source_folder):
        os.makedirs(source_folder)
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    for filename in os.listdir(source_folder):
        source_path = os.path.join(source_folder, filename)
        destination_path = os.path.join(destination_folder, filename)
        if os.path.isfile(source_path):
            shutil.move(source_path, destination_path)
    logging.info("All files moved from 'Process' to 'Archived'.")

# Run the main function only when the script is executed directly
if __name__ == "__main__":
    signal.signal(signal.SIGALRM, timeout_handler)
    logger_setup.setup_logging()
    logger = logging.getLogger()

    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        logging.error("OpenAI API key not found in environment variables.")
        raise EnvironmentError("OPENAI_API_KEY environment variable is not set. Please configure it before running the script.")

    client = openai

    source_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process')
    destination_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Archive')
    move_files(source_folder, destination_folder)

    get_json()
