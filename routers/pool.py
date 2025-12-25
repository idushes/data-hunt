import io
import csv
from fastapi import APIRouter, Depends
from fastapi.responses import Response
from sqlalchemy.orm import Session
from database import get_db
from models import Account
from dependencies import get_current_account
from utils import get_latest_debank_data

router = APIRouter()

@router.get(
    "/pool",
    summary="Export Liquidity Pool Data",
    description="Generates a CSV export of liquidity pool positions for the authenticated user's addresses.\n\nColumns: `id`, `symbol_1`, `amount_1`, `symbol_2`, `amount_2`, `reward_symbol_1`, `reward_amount_1`, `reward_symbol_2`, `reward_amount_2`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing pool data.",
        }
    },
)
async def get_pool(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    # Columns: id, symbol_1, amount_1, symbol_2, amount_2
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "symbol_1", "amount_1", "symbol_2", "amount_2", "reward_symbol_1", "reward_amount_1", "reward_symbol_2", "reward_amount_2"])

    # Extract active addresses
    valid_addresses = [addr.address.lower() for addr in account.addresses] if account.addresses else []

    # Fetch data from DB, filtered by active addresses
    current_data = get_latest_debank_data(db, account.id, valid_addresses)
    
    for address, data in current_data:
        # Address context
        address_short = address[-4:]
        
        # data is a list of protocols
        if isinstance(data, list):
            for item in data:
                protocol_id = item.get("id", "")
                chain = item.get("chain", "")
                
                portfolio_list = item.get("portfolio_item_list", [])
                for portfolio in portfolio_list:
                    # We are looking for pools
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
                        combined_id = f"{address_short}-{protocol_id}-{chain}-{symbol_1.lower()}-{symbol_2.lower()}"
                        
                        writer.writerow([combined_id, symbol_1, amount_1, symbol_2, amount_2, reward_symbol_1, reward_amount_1, reward_symbol_2, reward_amount_2])

    return Response(content=output.getvalue(), media_type="text/csv")
