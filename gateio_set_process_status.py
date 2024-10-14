#File: set_older_processed.py

import pandas as pd
from datetime import datetime, timedelta

# Load the TSV file into a DataFrame
filename = 'gateio_article_collection.tsv'
df = pd.read_csv(filename, sep='\t')

# Convert 'publish_datetime' to datetime format
df['publish_datetime'] = pd.to_datetime(df['publish_datetime'], errors='coerce')

# Get the current date
current_date = datetime.now()

# Calculate the threshold date (x days ago)
threshold_date = current_date - timedelta(days=2)

# Update 'llm_processed' to 'Yes' if 'publish_datetime' is older than 7 days
df.loc[df['publish_datetime'] > threshold_date, 'llm_processed'] = 'No'

# Save the updated DataFrame back to the TSV file
df.to_csv(filename, sep='\t', index=False)

print("Updated 'llm_processed' for articles older than 3 days.")
