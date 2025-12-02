import csv
import io
from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn

app = FastAPI()

@app.get("/csv")
async def get_csv():
    # Return the file directly
    return FileResponse(
        "data.csv",
        media_type="text/csv",
        filename="data.csv"
    )

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8111))
    uvicorn.run(app, host="0.0.0.0", port=port)
