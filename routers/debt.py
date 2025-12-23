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
    "/debt",
    summary="Export Debt Data",
    description="Generates a CSV export of debt data for the authenticated user's addresses.\n\nColumns: `id`, `amount`, `symbol`, `health_rate`, `reward`, `supply_amount`, `supply_symbol`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing debt data.",
        }
    },
)
async def get_debt(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    # Columns: id, amount, symbol, health_rate, reward, supply_amount, supply_symbol
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "amount", "symbol", "health_rate", "reward", "supply_amount", "supply_symbol"])

    # Extract active addresses
    valid_addresses = [addr.address.lower() for addr in account.addresses] if account.addresses else []

    # Fetch data from DB, filtered by active addresses
    current_data = get_latest_debank_data(db, account.id, valid_addresses)
    
    for address, data in current_data:
        # Address context is restored
        address_short = address[-4:]
        
        # data is a list of protocols
        if isinstance(data, list):
            for item in data:
                protocol_id = item.get("id", "")
                chain = item.get("chain", "")
                
                portfolio_list = item.get("portfolio_item_list", [])
                for portfolio in portfolio_list:
                    # Get Detail
                    detail = portfolio.get("detail", {})
                    
                    # Calculate total reward USD value
                    reward_list = detail.get("reward_token_list", [])
                    reward_usd = sum(r.get("amount", 0) * r.get("price", 0) for r in reward_list)
                    
                    # Get health rate
                    health_rate = detail.get("health_rate")
                    
                    # Get supply token symbol and amount
                    supply_list = detail.get("supply_token_list", [])
                    supply_symbol = ""
                    supply_amount = 0
                    if supply_list:
                        supply_symbol = supply_list[0].get("symbol", "")
                        supply_amount = supply_list[0].get("amount", 0)
                    
                    supply_symbol_lower = supply_symbol.lower()

                    # Iterate borrow list
                    borrow_list = detail.get("borrow_token_list", [])
                    
                    if not borrow_list:
                        continue
                        
                    for borrow in borrow_list:
                        amount = borrow.get("amount", 0)
                        symbol = borrow.get("symbol", "")
                        
                        combined_id = f"{address_short}-{protocol_id}-{chain}"
                        if supply_symbol_lower:
                            combined_id += f"-{supply_symbol_lower}"
                        
                        if symbol:
                            combined_id += f"-{symbol.lower()}"
                            
                        writer.writerow([combined_id, amount, symbol, health_rate, reward_usd, supply_amount, supply_symbol])

    return Response(content=output.getvalue(), media_type="text/csv")
