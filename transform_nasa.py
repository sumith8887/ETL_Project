import json
import pandas as pd
import glob
import os

def transform_nasa_data():
    os.makedirs("data/staged", exist_ok=True)
    latest_file = sorted(glob.glob("../data/raw/nasa_apod_*.json"))[-1]
    with open(latest_file, 'r') as f:
        data = json.load(f)

    df = pd.DataFrame({
        "date": [data.get("date")],
        "title": [data.get("title")],
        "explanation": [data.get("explanation")],
        "url": [data.get("url")],
        "media_type": [data.get("media_type")],
        "extracted_at": [pd.Timestamp.now()]
    })

    output_path = "../data/staged/nasa_apod_cleaned.csv"
    df.to_csv(output_path, index=False)
    print(f"NASA APOD data transformed and saved to {output_path}")
    return df

if __name__ == "__main__":
    transform_nasa_data()