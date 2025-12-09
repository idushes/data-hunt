import os
import json
import io
import csv
from glob import glob
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

@router.get("/debt")
async def get_debt():
    # Columns: id (combined), debt
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "debt"])

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
                    
                    # Calculate total debt for this protocol
                    total_debt = 0.0
                    portfolio_list = item.get("portfolio_item_list", [])
                    for portfolio in portfolio_list:
                        stats = portfolio.get("stats", {})
                        debt_value = stats.get("debt_usd_value", 0)
                        total_debt += debt_value
                    
                    # Sort out negligible debts (optional, but requested > 0, using slight threshold for float precision good practice or strict > 0)
                    if total_debt > 0:
                        combined_id = f"{address_short}-{protocol_id}-{chain}"
                        writer.writerow([combined_id, total_debt])
                    
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return Response(content=output.getvalue(), media_type="text/csv")
