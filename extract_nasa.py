from pathlib import Path
import requests
from datetime import datetime
import json


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def extract_nasa_data():
    url = "https://api.nasa.gov/planetary/apod?api_key=dMo7SdEBsOpgqtRxMx0vKgaIl76NqR02ebyL1fhu"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    filename = DATA_DIR / f"nasa_apod_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"NASA APOD data extracted and saved to {filename}")
    return data

if __name__ == "__main__":
    extract_nasa_data()