import os
import json
import io
import csv
from glob import glob
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

@router.get(
    "/pool",
    summary="Export Liquidity Pool Data",
    description="Generates a CSV export of liquidity pool positions for all tracked addresses.\n\nColumns: `id`, `symbol_1`, `amount_1`, `symbol_2`, `amount_2`, `reward_symbol_1`, `reward_amount_1`, `reward_symbol_2`, `reward_amount_2`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing pool data.",
        }
    },
)
async def get_pool():
    # Columns: id, symbol_1, amount_1, symbol_2, amount_2
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "symbol_1", "amount_1", "symbol_2", "amount_2", "reward_symbol_1", "reward_amount_1", "reward_symbol_2", "reward_amount_2"])

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
                        # We are looking for pools, usually they have detail_types common, but the strict req 
                        # is to look into supply_token_list having 2 tokens.
                        
                        detail = portfolio.get("detail", {})
                        supply_token_list = detail.get("supply_token_list", [])

                        # Filter: must be exactly 2 tokens in supply (liquidity pool)
                        if len(supply_token_list) == 2:
                            token1 = supply_token_list[0]
                            token2 = supply_token_list[1]
                            
                            symbol_1 = token1.get("symbol", "")
                            amount_1 = token1.get("amount", 0)
                            
                            symbol_2 = token2.get("symbol", "")
                            amount_2 = token2.get("amount", 0)

                            # Rewards
                            reward_token_list = detail.get("reward_token_list", [])
                            
                            # Initialize defaults
                            reward_symbol_1 = ""
                            reward_amount_1 = 0
                            reward_symbol_2 = ""
                            reward_amount_2 = 0

                            if len(reward_token_list) > 0:
                                reward_symbol_1 = reward_token_list[0].get("symbol", "")
                                reward_amount_1 = reward_token_list[0].get("amount", 0)
                            
                            if len(reward_token_list) > 1:
                                reward_symbol_2 = reward_token_list[1].get("symbol", "")
                                reward_amount_2 = reward_token_list[1].get("amount", 0)

                            # Construct ID: {address_short}-{protocol_id}-{chain}
                            combined_id = f"{address_short}-{protocol_id}-{chain}"
                            
                            writer.writerow([combined_id, symbol_1, amount_1, symbol_2, amount_2, reward_symbol_1, reward_amount_1, reward_symbol_2, reward_amount_2])

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return Response(content=output.getvalue(), media_type="text/csv")
