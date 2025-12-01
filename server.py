import csv
import io
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import uvicorn

app = FastAPI()

@app.get("/csv")
async def get_csv():
    # Sample data - in a real app this might come from a DB
    data = [
        ["id", "name", "value"],
        [1, "Item 1", 100],
        [2, "Item 2", 200],
        [3, "Item 3", 300],
    ]

    # Create an in-memory string buffer
    stream = io.StringIO()
    writer = csv.writer(stream)
    
    # Write data to the buffer
    writer.writerows(data)
    
    # Reset buffer position to the beginning
    stream.seek(0)

    # Return as a streaming response
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=data.csv"}
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8111)
