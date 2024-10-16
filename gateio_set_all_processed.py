#File: gateio_set_all_processed.py

import pandas as pd

# Load the TSV file into a DataFrame
filename = 'gateio_article_collection.tsv'
df = pd.read_csv(filename, sep='\t')

# Set all 'llm_processed' values to 'Yes'
df['publish_datetime'] = ''

# Save the updated DataFrame back to the TSV file
df.to_csv(filename, sep='\t', index=False)

print("Updated 'llm_processed' to 'Yes' for all records.")
