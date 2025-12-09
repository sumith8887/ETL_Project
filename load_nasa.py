from supabase import create_client
import os
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_nasa_data_to_supabase():
    csv_path = "../data/staged/nasa_apod_cleaned.csv"
    if not os.path.exists(csv_path):
        print(f"File {csv_path} does not exist. Please run the transform step first.")
        return

    df = pd.read_csv(csv_path)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size].where(pd.notnull(df), None).to_dict(orient="records")

        values = []
        for r in batch:
            date = r["date"]
            title = (r["title"] or "").replace("'", "''")
            explanation = (r["explanation"] or "").replace("'", "''")
            media_type = r.get("media_type") or ""
            url = r.get("url") or ""

            values.append(
                f"('{date}', '{title}', '{explanation}', '{media_type}', '{url}')"
            )

        if not values:
            continue

        insert_query = f"""
        INSERT INTO nasa_apod (date, title, explanation, media_type, image_url)
        VALUES {",".join(values)};
        """

        supabase.rpc("execute_sql", {"query": insert_query}).execute()
        print(f"Inserted batch {i // batch_size + 1} into Supabase")
        time.sleep(0.5)  

    print("NASA APOD data loading to Supabase completed.")

if __name__ == "__main__":
    load_nasa_data_to_supabase()
