# Filename: gateio_get_calendar.py

import json
import os
import logging
from datetime import datetime, timedelta, timezone
from ics import Calendar, Event
from ics import DisplayAlarm
from dateutil import parser
import gateio_logger_setup

# Initialize the logger
gateio_logger_setup.setup_logging()
logger = logging.getLogger()

# Constants
JSON_FILE_PATH = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process/gateio_structured.json')
OUTPUT_DIR = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Subscribe')
THRESHOLD_DATE = datetime.now(timezone.utc) - timedelta(days=3)

# Load JSON data
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Create an event name
def generate_event_name(exchange_name, event_types, assets):
    if not assets:
        return f"{exchange_name}: {', '.join(event_types)}"
    if len(assets) > 10:
        return f"{exchange_name}: {', '.join(event_types)} of Various Assets"
    return f"{exchange_name}: {', '.join(event_types)} of {', '.join(assets)}"

# Generate a description for the event
def generate_event_description(event_data, assets):
    description_parts = [event_data.get("event_summary", "")]
    if assets:
        description_parts.append(f"\nAssets: {', '.join(assets)}")
    if event_data.get("markets"):
        description_parts.append(f"Markets: {', '.join(event_data['markets'])}")
    if event_data.get("numerical_data"):
        description_parts.append(",\n".join(event_data['numerical_data']))
    if event_data.get("user_action_required"):
        description_parts.append(f"\n{event_data['user_action_required']}")
    if event_data.get("separate_event_link"):
        description_parts.append(event_data['separate_event_link'])
    return "\n".join(filter(None, description_parts))

# Create a single-day event
def create_single_day_event(event_data):
    event = Event()

    assets = event_data.get('tokens', []) + event_data.get('trading_pairs', [])
    event.name = generate_event_name(event_data['exchange_name'], event_data['event_type'], assets)
    event.description = generate_event_description(event_data, assets)
    event.url = event_data.get('article_link', "")
    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])
    event.begin = start_datetime.replace(minute=0, second=0, microsecond=0)
    event.end = end_datetime.replace(minute=30, second=0, microsecond=0)
    event.uid = event_data['UID']
    event.transparent = True

    # Add alarms with descriptions
    alarm_1_day = DisplayAlarm(trigger=timedelta(days=-1)) #, display_text="Reminder: Event starts in 1 day.")
    alarm_1_hour = DisplayAlarm(trigger=timedelta(hours=-1)) #, display_text="Reminder: Event starts in 1 hour.")
    event.alarms.append(alarm_1_day)
    event.alarms.append(alarm_1_hour)

    return event

# Create multi-day events
def create_multi_day_events(event_data):
    events = []
    assets = event_data.get('tokens', []) + event_data.get('trading_pairs', [])

    def create_event(location, begin, end, uid_suffix):
        event = Event()
        event.name = generate_event_name(event_data['exchange_name'], event_data['event_type'], assets)
        event.description = generate_event_description(event_data, assets)
        event.location = location
        event.url = event_data.get('article_link', "")
        event.begin = begin
        event.end = end
        event.uid = f"{event_data['UID']}_{uid_suffix}"
        event.transparent = True
        
        # Add alarms with descriptions
        alarm_1_day = DisplayAlarm(trigger=timedelta(days=-1)) #, display_text="Reminder: Event starts in 1 day.")
        alarm_1_hour = DisplayAlarm(trigger=timedelta(hours=-1)) #, display_text="Reminder: Event starts in 1 hour.")
        event.alarms.append(alarm_1_day)
        event.alarms.append(alarm_1_hour)

        return event

    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])
    events.append(create_event("Period Starts", start_datetime, start_datetime + timedelta(minutes=30), "start"))
    events.append(create_event("Period Ends", end_datetime - timedelta(minutes=30), end_datetime, "end"))
    return events

# Determine if an event is single or multi-day
def create_ics_events(event_data):
    start_datetime = parser.parse(event_data['start_datetime'])
    end_datetime = parser.parse(event_data['end_datetime'])
    if start_datetime.date() == end_datetime.date():
        return [create_single_day_event(event_data)]
    return create_multi_day_events(event_data)

# Filter events based on criteria
def filter_events(events, request):
    filtered_events = []
    for event_group in events:
        for event in event_group.get("events", []):
            event_start_datetime = parser.parse(event["start_datetime"]).replace(tzinfo=timezone.utc)
            if event_start_datetime < THRESHOLD_DATE:
                continue
            matches = all([
                not request["event_type"] or any(et in event["event_type"] for et in request["event_type"]),
                not request["tokens"] or set(request["tokens"]).issubset(set(event.get("tokens", []))),
                not request["trading_pairs"] or set(request["trading_pairs"]).issubset(set(event.get("trading_pairs", []))),
                not request["markets"] or set(request["markets"]).issubset(set(event.get("markets", []))),
            ])
            if matches:
                filtered_events.append(event)
    return filtered_events

# Save calendar to file
def save_calendar(events, event_type):
    calendar = Calendar()
    for event in events:
        for ics_event in create_ics_events(event):
            calendar.events.add(ics_event)
    filename = f"Gateio_{event_type.replace(' ', '_')}.ics"
    output_path = os.path.join(OUTPUT_DIR, filename)
    with open(output_path, 'w') as f:
        f.writelines(calendar)
    logger.info(f"Saved calendar to {output_path}")

# Main function
def main():
    data = load_json_data(JSON_FILE_PATH)
    requests = {
        "request1": {
            "event_type": ["Listing"],
            "tokens": [],
            "trading_pairs": [],
            "markets": []
        },
        "request2": {
            "event_type": ["Delisting"],
            "tokens": [],
            "trading_pairs": [],
            "markets": []
        }
    }

    for req_name, req in requests.items():
        filtered_events = filter_events(data, req)
        if filtered_events:
            save_calendar(filtered_events, "_".join(req["event_type"]))
        else:
            logger.info(f"No matching events found for {req_name}.")

if __name__ == "__main__":
    main()
