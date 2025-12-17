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
    "/stability",
    summary="Export Stability Data",
    description="Generates a CSV export of stability metrics (asset USD value) for the authenticated user's addresses.\n\nColumns: `id`, `asset_usd_value`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing stability data.",
        }
    },
)
async def get_stability(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    # Columns: id, asset_usd_value
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "asset_usd_value"])

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

    return Response(content=output.getvalue(), media_type="text/csv")
