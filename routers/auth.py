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

@router.post("/verify")
async def verify_signature(data: SignatureVerification):
    try:
        # Encode the message as required by EIP-191
        encoded_message = encode_defunct(text=data.message)
        
        # Recover the address from the signature
        recovered_address = Account.recover_message(encoded_message, signature=data.signature)
        
        # Check if the recovered address matches the claimed address
        is_valid = recovered_address.lower() == data.address.lower()
        
        return JSONResponse(content={
            "verified": is_valid,
            "recovered_address": recovered_address
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
