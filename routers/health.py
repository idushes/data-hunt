from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(
    prefix="/health",
    tags=["health"]
)

@router.get("/liveness")
def liveness():
    """
    K8s liveness probe.
    Returns 200 OK if the service is running.
    """
    return {"status": "ok"}

@router.get("/readiness")
def readiness(db: Session = Depends(get_db)):
    """
    K8s readiness probe.
    Checks database connectivity.
    Returns 200 OK if DB is reachable, 503 otherwise.
    """
    try:
        # Simple query to check DB connection
        db.execute(text("SELECT 1"))
    except Exception as e:
        # Log the error if you have a logger set up, otherwise just return 503
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed"
        )
    return {"status": "ready"}
