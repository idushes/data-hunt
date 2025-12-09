import os
import json
import httpx
from datetime import datetime
from config import DEBANK_ACCESS_KEY, get_target_ids

async def fetch_and_save_data():
    print(f"[{datetime.now()}] Starting data fetch task...")
    
    if not DEBANK_ACCESS_KEY:
        print("Error: DEBANK_ACCESS_KEY is missing!")
        return

    target_ids = get_target_ids()
    if not target_ids:
        print("Warning: No target IDs found (ENV keys starting with TARGET_ID_)")
        return

    async with httpx.AsyncClient() as client:
        headers = {"AccessKey": DEBANK_ACCESS_KEY}
        
        for user_id in target_ids:
            try:
                url = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"
                params = {"id": user_id}
                
                print(f"Fetching data for {user_id}...")
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                
                # Ensure directory exists
                os.makedirs("data", exist_ok=True)
                
                file_path = f"data/{user_id}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                print(f"Saved data to {file_path}")
                
            except Exception as e:
                print(f"Failed to fetch data for {user_id}: {str(e)}")
    
    print(f"[{datetime.now()}] Data fetch task completed.")
