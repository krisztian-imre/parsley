#File: gateio_get_calendar2.py

import json
import os
from ics import Calendar, Event
from datetime import timedelta
from dateutil import parser

# Function for single-day events
def create_ics_event_single_day(event_data):
    event = Event()

    # Combine tokens and trading pairs into one list
    assets = event_data.get('tokens', []) + event_data.get('trading_pairs', [])

    # Conditional logic for event.name
    if assets:
        if len(assets) > 10:
            # If there are more than 10 tokens/pairs, use a placeholder
            event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])} of Various Assets"
        else:
            # If there are 10 or fewer, list the tokens/pairs
            event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])} of {', '.join(assets)}"
    else:
        event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])}"

    # Construct the event description conditionally
    description_parts = [f"{event_data['event_summary']}"]
    
    if assets:
        description_parts.append(f"\nAssets: {', '.join(assets)}")
    
    if event_data['markets']:
        description_parts.append(f"Markets: {', '.join(event_data['markets'])}")
    
    if event_data['numerical_data']:
        description_parts.append(f"\n{',\n'.join(event_data['numerical_data'])}")
    
    if event_data['user_action_required']:
        description_parts.append(f"\n{event_data['user_action_required']}")
    
    if event_data['separate_event_link']:
        description_parts.append(f"{event_data.get('separate_event_link', '')}")

    event.description = "\n".join(part for part in description_parts if part)

    event.location = ""
    event.url = event_data['article_link']

    # Use parser.parse for datetime parsing
    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])

    # Round down 'event.begin' to HH:00:00
    event.begin = start_datetime.replace(minute=0, second=0, microsecond=0)

    # Round up 'event.end' to HH:30:00
    event.end = end_datetime.replace(minute=30, second=0, microsecond=0)

    # Set UID using existing UID from JSON
    event.uid = event_data['UID']

    return event

# Function for multi-day events
def create_ics_event_multi_day(event_data):
    events = []

    # Combine tokens and trading pairs into one list
    assets = event_data.get('tokens', []) + event_data.get('trading_pairs', [])

    # Function to generate an event
    def create_event(event_data, location, begin_time, end_time, uid_suffix):
        event = Event()

        # Conditional logic for event.name with large token handling
        if assets:
            if len(assets) > 10:
                event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])} of Various Assets"
            else:
                event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])} of {', '.join(assets)}"
        else:
            event.name = f"{event_data['exchange_name']}: {', '.join(event_data['event_type'])}"

        # Construct the event description conditionally
        description_parts = [f"{event_data['event_summary']}"]
        
        if assets:
            description_parts.append(f"\nAssets: {', '.join(assets)}")
        
        if event_data['markets']:
            description_parts.append(f"Markets: {', '.join(event_data['markets'])}")
        
        if event_data['numerical_data']:
            description_parts.append(f"\n{',\n'.join(event_data['numerical_data'])}")
        
        if event_data['user_action_required']:
            description_parts.append(f"\n{event_data['user_action_required']}")
        
        if event_data['separate_event_link']:
            description_parts.append(f"{event_data.get('separate_event_link', '')}")

        # Set the description
        event.description = "\n".join(part for part in description_parts if part)

        # Set the location, URL, and timings
        event.location = location
        event.url = event_data['article_link']
        event.begin = begin_time
        event.end = end_time

        # Set UID with suffix to differentiate start and end events
        event.uid = f"{event_data['UID']}_{uid_suffix}"

        return event

    # Parse the start and end datetimes using parser.parse
    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])

    # Create the start event (location: "Period Starts")
    start_event = create_event(
        event_data=event_data,
        location="Period Starts",
        begin_time=start_datetime,
        end_time=start_datetime + timedelta(minutes=30),
        uid_suffix="start"
    )

    # Create the end event (location: "Period Ends")
    end_event = create_event(
        event_data=event_data,
        location="Period Ends",
        begin_time=end_datetime - timedelta(minutes=30),
        end_time=end_datetime,
        uid_suffix="end"
    )

    # Append both events to the list
    events.append(start_event)
    events.append(end_event)

    return events

# Main function to determine whether the event is single-day or multi-day
def create_ics_event(event_data):
    # Parse the datetime strings using dateutil.parser
    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])
    
    # Check if start_datetime and end_datetime fall on the same day
    if start_datetime.date() == end_datetime.date():
        return [create_ics_event_single_day(event_data)]  # Return a list with a single event
    else:
        return create_ics_event_multi_day(event_data)  # Return a list of two events

# Function to find the latest JSON file in the Gateio_JSON_Process folder
def find_latest_json_file(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    if not files:
        raise FileNotFoundError("No JSON files found in the directory.")
    
    # Sort the files by modification time and get the latest one
    files.sort(key=lambda f: os.path.getmtime(os.path.join(folder_path, f)), reverse=True)
    return files[0]

if __name__ == "__main__":

    # Define the source folder where the JSON file is located
    source_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process')

    # Find the latest JSON file in the source folder
    json_file_name = find_latest_json_file(source_folder)
    json_file_path = os.path.join(source_folder, json_file_name)

    # Load the JSON data from the file
    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    # Initialize or load the existing ICS file
    ics_file_path = 'Gateio_All.ics'

    # If the ICS file exists, load it, else create a new calendar
    if os.path.exists(ics_file_path):
        with open(ics_file_path, 'r') as ics_file:
            calendar = Calendar(ics_file.read())
    else:
        calendar = Calendar()

    # Loop through each group of events in the JSON data
    for event_group in json_data:
        # The 'events' key contains the list of actual events in each group
        for event_data in event_group.get("events", []):  # Use `.get()` to safely access 'events'
            events = create_ics_event(event_data)  # This returns a list of events
            for ics_event in events:
                calendar.events.add(ics_event)

    # Save or append the new events to the ICS file
    with open(ics_file_path, 'w') as ics_file:
        ics_file.writelines(calendar)

    print(f"Events have been saved to '{ics_file_path}'.")
