from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from eth_account import Account
from eth_account.messages import encode_defunct

router = APIRouter(prefix="/web3", tags=["web3"])

class SignatureVerification(BaseModel):
    address: str
    message: str
    signature: str

from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from eth_account import Account as EthAccount
from eth_account.messages import encode_defunct
from sqlalchemy.orm import Session

from database import get_db
from models import Account, AccountAddress, AccountToken
from security import create_access_token
from dependencies import get_current_account, get_current_token_id

router = APIRouter(prefix="/web3", tags=["web3"])

class SignatureVerification(BaseModel):
    address: str
    message: str
    signature: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class DeactivateTokenRequest(BaseModel):
    token_id: str

class TokenInfo(BaseModel):
    id: str
    current: bool
    created_at: int
    is_active: bool

@router.post("/login", response_model=TokenResponse)
async def login(data: SignatureVerification, db: Session = Depends(get_db)):
    try:
        # Encode the message as required by EIP-191
        encoded_message = encode_defunct(text=data.message)
        
        # Recover the address from the signature
        recovered_address = EthAccount.recover_message(encoded_message, signature=data.signature)
        
        # Check if the recovered address matches the claimed address
        if recovered_address.lower() != data.address.lower():
             print(f"Login Failed: Address mismatch. Claimed: {data.address}, Recovered: {recovered_address}")
             raise HTTPException(status_code=400, detail="Invalid signature or address mismatch")

        # Find or Create Account
        # Check if address exists
        account_addr = db.query(AccountAddress).filter(AccountAddress.address == recovered_address.lower()).first()
        
        if account_addr:
            account = account_addr.account
        else:
            # Create new account
            # Assumption: First login with this address creates an account.
            # We don't have network info in request, assuming 'ethereum' or just storing what we have? 
            # Models say: init_address_network is nullable=False.
            # We should probably ask client for network, but for now let's default to 'unknown' or just ignore if strict. 
            # Prompt didn't specify registration, but "authorize via web3". 
            # I will default network to 'ethereum' for now to satisfy model constraint.
            account = Account(
                init_address=recovered_address.lower(),
                init_address_network="ethereum" 
            )
            db.add(account)
            db.commit() # Commit to get ID
            
            account_addr = AccountAddress(
                account_id=account.id,
                address=recovered_address.lower(),
                network="ethereum",
                can_auth=True
            )
            db.add(account_addr)
            db.commit()

        # Create AccountToken
        new_token = AccountToken(
            account_id=account.id,
            created_at=int(datetime.now(timezone.utc).timestamp()),
            is_active=True
        )
        db.add(new_token)
        db.commit()
        db.refresh(new_token)

        # Generate JWT
        access_token = create_access_token(
            data={"sub": account.id, "jti": new_token.id}
        )

        return {"access_token": access_token, "token_type": "bearer"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Login Failed: Exception occurred: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/logout")
async def logout(
    current_token_id: str = Depends(get_current_token_id),
    db: Session = Depends(get_db),
    account: Account = Depends(get_current_account) # Ensures valid auth
):
    token = db.query(AccountToken).filter(AccountToken.id == current_token_id).first()
    if token:
        token.is_active = False
        db.commit()
    return {"message": "Logged out successfully"}

@router.get("/tokens", response_model=List[TokenInfo])
async def list_tokens(
    current_token_id: str = Depends(get_current_token_id),
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    tokens = db.query(AccountToken).filter(
        AccountToken.account_id == account.id,
        AccountToken.is_active == True
    ).all()
    
    return [
        TokenInfo(
            id=t.id, 
            current=(t.id == current_token_id),
            created_at=t.created_at,
            is_active=t.is_active
        )
        for t in tokens
    ]

@router.post("/deactivate")
async def deactivate_token(
    request: DeactivateTokenRequest,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    token = db.query(AccountToken).filter(
        AccountToken.id == request.token_id,
        AccountToken.account_id == account.id
    ).first()
    
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
        
    token.is_active = False
    db.commit()
    return {"message": "Token deactivated successfully"}

