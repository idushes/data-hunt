import os
from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()

@router.get("/csv")
async def get_csv():
    # Return the file directly
    if os.path.exists("data.csv"):
        return FileResponse(
            "data.csv",
            media_type="text/csv",
            filename="data.csv"
        )
    return {"error": "data.csv not found"}
