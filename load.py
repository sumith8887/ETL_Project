from supabase import create_client
import os
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_data_to_supabase():
    csv_path="../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        print(f"File {csv_path} does not exist. Please run the transform step first.")
        return
    df = pd.read_csv(csv_path)

    #convert timestamp to strings
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['extracted_at'] = pd.to_datetime(df['extracted_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df),None).to_dict(orient="records")
    
        values = [
            f"('{r['time']}', {r.get('temperature_C','NULL')}, {r.get('humidity_percent','NULL')}, {r.get('wind_speed_kmph','NULL')}, '{r.get('city','Hyderabad')}', '{r['extracted_at']}')"
            for r in batch
        ]

        insert_query = f"""
        INSERT INTO weather_data (time, temperature_C, humidity_percent, wind_speed_kmph, city, extracted_at)
        VALUES {','.join(values)};
        """

        supabase.rpc("execute_sql", {"query": insert_query}).execute()

        print(f"Inserted batch {i//batch_size + 1} into Supabase")
        time.sleep(0.5)  # To avoid overwhelming the server
    print("Data loading to Supabase completed.")

if __name__ == "__main__":
    load_data_to_supabase()

