#File: gateio_get_UID.py

import os
import json
import hashlib
from collections import defaultdict

# Function to create a hexadecimal UID based on the article_link
def create_hex_uid(link):
    return hashlib.sha256(link.encode()).hexdigest()[:32]  # Shorten to 16 characters for brevity

def assign_uids(input_file, output_file):
    # Load the JSON data from the input file
    with open(input_file, 'r') as file:
        data = json.load(file)

    # Dictionary to track occurrences of each article_link for uniqueness
    link_counter = defaultdict(int)

    # Iterate through the data to assign UIDs
    for item in data:
        for event in item.get("events", []):
            article_link = event.get("article_link", "")
            if article_link:
                # Generate the base UID
                base_uid = create_hex_uid(article_link)
                
                # Always append '@' and a counter starting from 1
                link_counter[article_link] += 1
                unique_uid = f"{base_uid}@{link_counter[article_link]}"
                
                # Assign the generated UID to the event
                event["UID"] = unique_uid

    # Write the updated data back to the output file
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)

# Input and output file paths
input_file = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process/gateio_241112_2313.json')  # Replace with your input file path
output_file = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process/gateio_241112_2313b.json')  # Replace with your desired output file path

# Run the UID assignment function
assign_uids(input_file, output_file)

print(f"UIDs have been assigned and saved to {output_file}")
