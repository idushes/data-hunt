import os
import json
import io
import csv
from glob import glob
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

@router.get("/stability")
async def get_stability():
    # Columns: id, asset_usd_value
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "asset_usd_value"])

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
                    
                    portfolio_list = item.get("portfolio_item_list", [])
                    for portfolio in portfolio_list:
                        # Filter by detail_types = "common"
                        detail_types = portfolio.get("detail_types", [])
                        if "common" not in detail_types:
                            continue

                        # Get stats
                        stats = portfolio.get("stats", {})
                        asset_usd_value = stats.get("asset_usd_value", 0)
                        
                        # Construct ID: {address_short}-{protocol_id}-{chain}
                        combined_id = f"{address_short}-{protocol_id}-{chain}"
                        
                        writer.writerow([combined_id, asset_usd_value])

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return Response(content=output.getvalue(), media_type="text/csv")
