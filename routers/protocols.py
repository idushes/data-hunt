import os
import json
import io
import csv
from glob import glob
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

@router.get("/protocols")
async def get_protocols():
    # Columns: address (last 4), id, chain
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["address", "id", "chain"])

    # Iterate over all .json files in data/
    json_files = glob("data/*.json")
    
    for file_path in json_files:
        try:
            # Extract filename (address)
            filename = os.path.basename(file_path)
            # Address is filename without extension
            address_full = filename.replace(".json", "")
            # Take last 4 characters of address
            address_short = address_full[-4:]
            
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            # data is a list of protocols
            if isinstance(data, list):
                for item in data:
                    protocol_id = item.get("id", "")
                    chain = item.get("chain", "")
                    
                    writer.writerow([address_short, protocol_id, chain])
                    
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return Response(content=output.getvalue(), media_type="text/csv")
