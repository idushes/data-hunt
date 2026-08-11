import base64
import json
import os
import secrets
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException


BINANCE_CAPSULE_PREFIX = "dhbn1"
BINANCE_CAPSULE_MAX_LENGTH = 4096
BINANCE_API_KEY_MAX_LENGTH = 256
BINANCE_API_SECRET_MAX_LENGTH = 256
CAPSULE_KEYS_ENV = "COINBASE_CAPSULE_KEYS_JSON"
CAPSULE_ACTIVE_KEY_ID_ENV = "COINBASE_CAPSULE_ACTIVE_KEY_ID"


@dataclass(frozen=True)
class BinanceCredentials:
    api_key: str
    api_secret: str


def _decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception as exc:
        raise ValueError("Invalid base64url value") from exc


def _encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _load_keyring() -> tuple[str, dict[str, bytes]]:
    active_key_id = os.getenv(CAPSULE_ACTIVE_KEY_ID_ENV, "").strip()
    raw_keyring = os.getenv(CAPSULE_KEYS_ENV, "").strip()
    if not active_key_id or not raw_keyring:
        raise HTTPException(
            status_code=503,
            detail="Binance credential capsules are not configured",
        )

    try:
        parsed = json.loads(raw_keyring)
        if not isinstance(parsed, dict):
            raise ValueError("Keyring must be an object")

        keyring: dict[str, bytes] = {}
        for key_id, encoded_key in parsed.items():
            if not isinstance(key_id, str) or not key_id or "." in key_id:
                raise ValueError("Invalid key id")
            if not isinstance(encoded_key, str):
                raise ValueError("Invalid key")
            decoded_key = _decode_base64url(encoded_key)
            if len(decoded_key) != 32:
                raise ValueError("Capsule keys must contain 32 bytes")
            keyring[key_id] = decoded_key

        if active_key_id not in keyring:
            raise ValueError("Active key id is missing from keyring")
        return active_key_id, keyring
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Binance credential capsule configuration is invalid",
        ) from exc


def _associated_data(key_id: str) -> bytes:
    return f"data-hunt:binance:capsule:v1:{key_id}".encode()


def encrypt_binance_credentials(api_key: str, api_secret: str) -> str:
    if not api_key or len(api_key) > BINANCE_API_KEY_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid Binance API key")
    if not api_secret or len(api_secret) > BINANCE_API_SECRET_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid Binance API secret")

    active_key_id, keyring = _load_keyring()
    payload = json.dumps(
        {"v": 1, "api_key": api_key, "api_secret": api_secret},
        separators=(",", ":"),
    ).encode()
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(keyring[active_key_id]).encrypt(
        nonce,
        payload,
        _associated_data(active_key_id),
    )
    return ".".join(
        [BINANCE_CAPSULE_PREFIX, active_key_id, _encode_base64url(nonce + ciphertext)]
    )


def decrypt_binance_credentials(capsule: str) -> BinanceCredentials:
    if not capsule or len(capsule) > BINANCE_CAPSULE_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid Binance access key")

    try:
        prefix, key_id, encoded_payload = capsule.split(".", 2)
        if prefix != BINANCE_CAPSULE_PREFIX:
            raise ValueError("Unsupported capsule version")

        _, keyring = _load_keyring()
        key = keyring.get(key_id)
        if key is None:
            raise ValueError("Unknown key id")

        encrypted = _decode_base64url(encoded_payload)
        if len(encrypted) <= 12:
            raise ValueError("Invalid encrypted payload")
        plaintext = AESGCM(key).decrypt(
            encrypted[:12],
            encrypted[12:],
            _associated_data(key_id),
        )
        payload = json.loads(plaintext)
        if not isinstance(payload, dict) or payload.get("v") != 1:
            raise ValueError("Invalid payload version")

        api_key = payload.get("api_key")
        api_secret = payload.get("api_secret")
        if not isinstance(api_key, str) or not isinstance(api_secret, str):
            raise ValueError("Invalid credentials")
        if not api_key or not api_secret:
            raise ValueError("Invalid credentials")
        return BinanceCredentials(api_key=api_key, api_secret=api_secret)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Binance access key") from exc
