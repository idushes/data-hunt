import io
import csv
from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session
from database import get_db
from models import Account
from dependencies import get_current_account
from utils import get_latest_debank_data

router = APIRouter()

@router.get(
    "/wallet",
    summary="Export Wallet Token Data",
    description="Generates a CSV export of wallet tokens for the authenticated user's addresses.\n\nColumns: `id`, `symbol`, `amount`, `price`, `usd_value`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing wallet token data.",
        }
    },
)
async def get_wallet(
    min_usd_value: float = Query(0, description="Minimum USD value to include in the export"),
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    # Columns: id, symbol, amount, price, usd_value
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "symbol", "amount", "price", "usd_value"])

    # Extract active addresses
    valid_addresses = [addr.address.lower() for addr in account.addresses] if account.addresses else []

    # Fetch data from DB, specifically for token list
    current_data = get_latest_debank_data(db, account.id, valid_addresses, path="/v1/user/all_token_list")
    
    for address, data in current_data:
        # Address context
        address_short = address[-4:]
        
        # data is a list of tokens
        if isinstance(data, list):
            for token in data:
                # Safe extraction
                symbol = token.get("symbol", "")
                amount = token.get("amount", 0)
                price = token.get("price", 0)
                chain = token.get("chain", "")
                
                # Calculate USD value
                usd_value = amount * price
                
                # Apply Filter
                if usd_value < min_usd_value:
                    continue
                
                # Construct ID: {address_short}-{chain}-{symbol_lower}
                symbol_lower = symbol.lower()
                combined_id = f"{address_short}-{chain}-{symbol_lower}"
                
                writer.writerow([combined_id, symbol, amount, price, usd_value])

    return Response(content=output.getvalue(), media_type="text/csv")
