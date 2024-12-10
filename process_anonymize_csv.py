import os
import pandas as pd

def process_large_csv(input_csv, output_csv, chunk_size):
    """
    Reads a large CSV file in chunks, anonymizes specific columns, and writes the output.
    """
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    print(f"Processing file: {input_csv}")
    for i, chunk in enumerate(pd.read_csv(input_csv, chunksize=chunk_size)):
        print(f"Processing chunk {i + 1}")
        chunk['first_name'] = chunk['first_name'].apply(lambda x: hash(x) if pd.notnull(x) else x)
        chunk['last_name'] = chunk['last_name'].apply(lambda x: hash(x) if pd.notnull(x) else x)
        chunk['address'] = chunk['address'].apply(lambda x: hash(x) if pd.notnull(x) else x)
        chunk.to_csv(output_csv, mode='a', index=False, header=not os.path.exists(output_csv))
        print(f"Chunk {i + 1} written to {output_csv}")
    print(f"Anonymization complete. Output saved to: {output_csv}")

if __name__ == "__main__":
    process_large_csv("large_input.csv", "output/anonymized_output.csv", chunk_size=1000)
