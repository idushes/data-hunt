from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from sqlalchemy.orm import Session
from config import SECRET_KEY, ALGORITHM
from database import get_db
from models import AccountToken, Account
import time

security = HTTPBearer(auto_error=False)

async def get_current_token_payload(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    token: str = None
) -> dict:
    if credentials:
        token = credentials.credentials
    elif token:
        token = token
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        account_id: str = payload.get("sub")
        token_id: str = payload.get("jti")
        if account_id is None or token_id is None:
            print(f"Auth Failed: Missing sub or jti in token. Payload: {payload}")
            raise credentials_exception
        return payload
    except jwt.PyJWTError as e:
        print(f"Auth Failed: JWT Error: {str(e)}")
        raise credentials_exception

def get_current_account(
    payload: dict = Depends(get_current_token_payload),
    db: Session = Depends(get_db)
):
    token_id = payload.get("jti")
    account_id = payload.get("sub")
    
    # Check if token exists and is active in DB
    db_token = db.query(AccountToken).filter(AccountToken.id == token_id).first()
    if not db_token:
        print(f"Auth Failed: Token {token_id} not found in database.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Token is invalid"
        )
    if not db_token.is_active:
        print(f"Auth Failed: Token {token_id} is inactive (revoked).")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Token has been revoked"
        )
    
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        print(f"Auth Failed: Account {account_id} not found.")
        raise HTTPException(status_code=404, detail="Account not found")
        
    return account

def get_current_token_id(payload: dict = Depends(get_current_token_payload)) -> str:
    return payload.get("jti")
