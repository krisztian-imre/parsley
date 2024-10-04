import pandas as pd
from datetime import datetime

# Step 1: Load the TSV file into a pandas DataFrame
file_path = 'gateio_articles.tsv'
df = pd.read_csv(file_path, sep='\t')

# Step 2: Convert the 'publish_datetime' column to datetime objects
df['publish_datetime'] = pd.to_datetime(df['publish_datetime'], format='%Y-%m-%d %H:%M:%S')

# Step 3: Get today's date (ignoring time)
today = datetime.now().date()

# Step 4: Filter out rows where 'publish_datetime' is older than today
df_filtered = df[df['publish_datetime'].dt.date >= today]

# Step 5: Save the filtered DataFrame back to a TSV file
df_filtered.to_csv(file_path, sep='\t', index=False)

print("Records older than today have been removed.")
