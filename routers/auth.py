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

from utils import get_valid_chain_ids

@router.post("/login", response_model=TokenResponse, summary="Web3 Login / Registration", description="Authenticates a user via Web3 signature. Creates a new account if the address is not found and no other account has authorized it. Returns a JWT access token containing the authenticated address.")
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
        
        network = "eth" # Default to 'eth' to match ID in chain list

        if account_addr:
            if not account_addr.can_auth:
                raise HTTPException(status_code=403, detail="Address not authorized for login")
            account = account_addr.account
            network = account_addr.network
        else:
            # Create new account
            account = Account(
                init_address=recovered_address.lower(),
                init_address_network=network 
            )
            db.add(account)
            db.commit() # Commit to get ID
            
            account_addr = AccountAddress(
                account_id=account.id,
                address=recovered_address.lower(),
                network=network,
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

        # Generate JWT with address claim
        access_token = create_access_token(
            data={
                "sub": account.id, 
                "jti": new_token.id,
                "address": recovered_address.lower(),
                "network": network
            }
        )

        return {"access_token": access_token, "token_type": "bearer"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/logout", summary="Logout", description="Revokes the current access token.")
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

@router.get("/tokens", response_model=List[TokenInfo], summary="List Active Sessions", description="Returns a list of all active sessions (tokens) for the current account.")
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

# New Address Management Endpoints

class AddAddressRequest(BaseModel):
    address: str
    network: str

class AddressInfo(BaseModel):
    id: int
    address: str
    network: str
    can_auth: bool

@router.post("/addresses", response_model=AddressInfo, summary="Add Secondary Address", description="Links a new Web3 address to the current account. The new address is initially disabled for authentication (`can_auth=False`).\n\n**Validation:** `network` must be a valid Chain ID from `/chains` (e.g. 'eth', 'bsc', 'matic').")
async def add_address(
    request: AddAddressRequest,
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    # Validate Network
    valid_chains = get_valid_chain_ids()
    if request.network not in valid_chains:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid network '{request.network}'. Please use a valid chain ID from /chains."
        )

    # Check if address already exists for this account
    existing = db.query(AccountAddress).filter(
        AccountAddress.account_id == account.id,
        AccountAddress.address == request.address.lower()
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="Address already linked to this account")

    # We do NOT check if address exists on OTHER accounts here. 
    # It allows observing an address on multiple accounts, but only one can have 'can_auth=True' (enforced in toggle_auth)
    
    new_addr = AccountAddress(
        account_id=account.id,
        address=request.address.lower(),
        network=request.network,
        can_auth=False 
    )
    db.add(new_addr)
    db.commit()
    db.refresh(new_addr)
    
    return AddressInfo(
        id=new_addr.id,
        address=new_addr.address,
        network=new_addr.network,
        can_auth=new_addr.can_auth
    )

@router.get("/addresses", response_model=List[AddressInfo], summary="List Linked Addresses", description="Returns all Web3 addresses linked to the current account.")
async def get_addresses(
    account: Account = Depends(get_current_account),
    db: Session = Depends(get_db)
):
    addrs = db.query(AccountAddress).filter(AccountAddress.account_id == account.id).all()
    return [
        AddressInfo(
            id=a.id,
            address=a.address,
            network=a.network,
            can_auth=a.can_auth
        ) for a in addrs
    ]

class AuthToggleRequest(BaseModel):
    enable: bool
    signature: Optional[str] = None
    message: Optional[str] = None

from dependencies import get_current_token_payload

@router.post("/addresses/{address}/auth", summary="Toggle Address Authorization", description="Enables or disables login authorization for a linked address. Enabling requires a valid signature to prove ownership. Disabling is prevented if it's the current session's address.")
async def toggle_address_auth(
    address: str,
    request: AuthToggleRequest,
    account: Account = Depends(get_current_account),
    token_payload: dict = Depends(get_current_token_payload),
    db: Session = Depends(get_db)
):
    target_addr = db.query(AccountAddress).filter(
        AccountAddress.account_id == account.id,
        AccountAddress.address == address.lower()
    ).first()

    if not target_addr:
        raise HTTPException(status_code=404, detail="Address not found on this account")

    if request.enable:
        # Verify ownership
        if not request.signature or not request.message:
             raise HTTPException(status_code=400, detail="Signature and message required to enable auth")
        
        try:
            encoded_message = encode_defunct(text=request.message)
            recovered_address = EthAccount.recover_message(encoded_message, signature=request.signature)
            
            if recovered_address.lower() != address.lower():
                raise HTTPException(status_code=400, detail="Signature invalid for this address")
                
        except Exception:
            raise HTTPException(status_code=400, detail="Signature verification failed")

        # Check collision: ensure no other account has this address with can_auth=True
        # Note: It IS allowed to have the same address authorized on the SAME account (if we had copies), but strictly unique across accounts for auth
        collision = db.query(AccountAddress).filter(
            AccountAddress.address == address.lower(),
            AccountAddress.can_auth == True,
            AccountAddress.account_id != account.id
        ).first()
        
        if collision:
            raise HTTPException(status_code=409, detail="Address is active on another account")
            
        target_addr.can_auth = True
        
    else:
        # Disable auth
        # Safety check: Do not allow disabling the address we are currently logged in with
        current_login_address = token_payload.get("address")
        if current_login_address and current_login_address.lower() == address.lower():
            raise HTTPException(status_code=400, detail="Cannot disable authorization for the current session address")
            
        target_addr.can_auth = False

    db.commit()
    return {"message": "Authorization status updated", "can_auth": target_addr.can_auth}

@router.post("/deactivate", summary="Deactivate Token", description="Deactivates a specific access token. This is useful for remote logout or invalidating a compromised session.")
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

