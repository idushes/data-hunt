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
    description="Generates a CSV export of stability metrics (asset USD value and token balances) for the authenticated user's addresses.\n\nColumns: `id`, `asset_usd_value`, `token_symbol_N`, `token_amount_N`.",
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "CSV file containing stability data.",
        }
    },
)
async def get_stability(
    account: Account = Depends(get_current_account), db: Session = Depends(get_db)
):
    rows = []
    max_token_count = 0

    # Extract active addresses
    valid_addresses = (
        [addr.address.lower() for addr in account.addresses]
        if account.addresses
        else []
    )

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

                    detail = portfolio.get("detail", {})
                    supply_token_list = detail.get("supply_token_list", [])
                    token_pairs = [
                        (token.get("symbol", ""), token.get("amount", 0))
                        for token in supply_token_list
                    ]
                    max_token_count = max(max_token_count, len(token_pairs))

                    # Construct ID: {address_short}-{protocol_id}-{chain}
                    combined_id = f"{address_short}-{protocol_id}-{chain}"

                    rows.append(
                        {
                            "id": combined_id,
                            "asset_usd_value": asset_usd_value,
                            "token_pairs": token_pairs,
                        }
                    )

    output = io.StringIO()
    writer = csv.writer(output)

    header = ["id", "asset_usd_value"]
    for index in range(1, max_token_count + 1):
        header.extend([f"token_symbol_{index}", f"token_amount_{index}"])
    writer.writerow(header)

    for row in rows:
        csv_row = [row["id"], row["asset_usd_value"]]
        token_pairs = row["token_pairs"]

        for symbol, amount in token_pairs:
            csv_row.extend([symbol, amount])

        missing_tokens = max_token_count - len(token_pairs)
        for _ in range(missing_tokens):
            csv_row.extend(["", 0])

        writer.writerow(csv_row)

    return Response(content=output.getvalue(), media_type="text/csv")
