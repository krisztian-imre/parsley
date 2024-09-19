import pandas as pd
from ics import Calendar, Event
import os
import shutil
from datetime import datetime, timedelta
import ast

# Define the source and destination directories
source_folder = 'ICS_IN'
destination_folder = 'ICS_OUT'

# Create the destination folder if it doesn't exist
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# Move all files from 'ICS_IN' to 'ICS_OUT'
for filename in os.listdir(source_folder):
    source_path = os.path.join(source_folder, filename)
    destination_path = os.path.join(destination_folder, filename)
    
    if os.path.isfile(source_path):
        shutil.move(source_path, destination_path)

print("All files moved from 'ICS_IN' to 'ICS_OUT'.")

# Load the TSV file
file_path = 'gateio_articles_processed.tsv'
data = pd.read_csv(file_path, sep='\t')

# Strip any leading/trailing spaces in column names (just in case)
data.columns = data.columns.str.strip()

# Create the 'ICS' folder if it doesn't exist
ics_folder = 'ICS_IN'
if not os.path.exists(ics_folder):
    os.makedirs(ics_folder)

# Function to set minutes and seconds to zero (HH:00:00)
def adjust_time_to_hour(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return dt.replace(minute=0, second=0, microsecond=0)

# Function to parse lists formatted as strings and join values with a comma and space
def parse_list(value):
    if value == '[]':
        return ''  # Return empty string if the list is empty
    else:
        try:
            # Convert the string representation of a list to an actual list
            value_list = ast.literal_eval(value)
            if isinstance(value_list, list):
                return ', '.join(value_list)  # Join list items with ", "
        except (ValueError, SyntaxError):
            # In case there's an issue with the format, just return the original value
            return value
    return value

# Filter rows where 'ical_processed' is 'No'
unprocessed_data = data[data['ical_processed'] == 'No']

# Iterate through each unprocessed row to create .ics files
for index, row in unprocessed_data.iterrows():
    try:
        # Create a new calendar event
        cal = Calendar()
        event = Event()

        # Start building the event description
        description = row['llm_summary'] + "\n\n"

        # Parse and format the details to add to the description
        tokens = parse_list(row['llm_token'])  # Corrected to 'llm_token'
        pairs = parse_list(row['llm_pair'])
        contract_types = parse_list(row['llm_trading_contract'])
        products = parse_list(row['llm_earning_product'])
        categories = parse_list(row['llm_category'])  # Now handling 'llm_category' as a list

        if tokens:
            description += f"Tokens: {tokens}\n"
        if pairs:
            description += f"Pairs: {pairs}\n"
        if contract_types:
            description += f"Type: {contract_types}\n"
        if products:
            description += f"Product: {products}\n"

        # Add category (now a list) and priority to the description
        if categories:
            description += f"Category: {categories}\n"
        description += f"Priority: {row['llm_priority']}"

        # Populate the event details from the row data
        event.name = row['title']  # Use the article's title as the event name
        
        # Adjust the action datetime to HH:00:00 format (UTC)
        start_time = adjust_time_to_hour(row['llm_action_datetime'])
        event.begin = start_time

        # Set the event end time to 30 minutes after the start time
        event.end = start_time + timedelta(minutes=30)

        # Set the location as the parsed category value (in case it's needed)
        event.location = categories

        event.description = description
        event.url = row['link']  # Link to the original article
        
        # Add the event to the calendar
        cal.events.add(event)

        # Create a unique filename using the exchange and publish date
        exchange = row['exchange'].replace(' ', '_').replace('/', '_').replace('.', '_')  # Clean up exchange name for filename
        publish_date = row['publish_datetime'].replace(' ', '_').replace(':', '-')  # Clean up datetime for filename
        filename = f"{exchange}_{publish_date}.ics"
        
        # Save the .ics file to the 'ICS' folder
        ics_path = os.path.join(ics_folder, filename)
        with open(ics_path, 'w') as ics_file:
            ics_file.writelines(cal)

        # Mark the record as processed in the DataFrame
        data.at[index, 'ical_processed'] = 'Yes'
    
    except Exception as e:
        print(f"Error processing row {index}: {e}")

# Save the updated data back to the TSV file
data.to_csv(file_path, sep='\t', index=False)

print("ICS files created and data updated.")
