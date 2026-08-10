import base64
import json
import os
import secrets
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException


COINBASE_CAPSULE_KEYS_ENV = "COINBASE_CAPSULE_KEYS_JSON"
COINBASE_CAPSULE_ACTIVE_KEY_ID_ENV = "COINBASE_CAPSULE_ACTIVE_KEY_ID"
COINBASE_CAPSULE_PREFIX = "dhc1"
COINBASE_CAPSULE_MAX_LENGTH = 32768
COINBASE_KEY_NAME_MAX_LENGTH = 1024
COINBASE_KEY_SECRET_MAX_LENGTH = 16384


@dataclass(frozen=True)
class CoinbaseCredentials:
    key_name: str
    key_secret: str


def _decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception as exc:
        raise ValueError("Invalid base64url value") from exc


def _encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _load_keyring() -> tuple[str, dict[str, bytes]]:
    active_key_id = os.getenv(COINBASE_CAPSULE_ACTIVE_KEY_ID_ENV, "").strip()
    raw_keyring = os.getenv(COINBASE_CAPSULE_KEYS_ENV, "").strip()
    if not active_key_id or not raw_keyring:
        raise HTTPException(
            status_code=503,
            detail="Coinbase credential capsules are not configured",
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
            detail="Coinbase credential capsule configuration is invalid",
        ) from exc


def _associated_data(key_id: str) -> bytes:
    return f"data-hunt:coinbase:capsule:v1:{key_id}".encode()


def encrypt_coinbase_credentials(key_name: str, key_secret: str) -> str:
    if len(key_name) > COINBASE_KEY_NAME_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Coinbase key_name is too long")
    if len(key_secret) > COINBASE_KEY_SECRET_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Coinbase key_secret is too long")

    active_key_id, keyring = _load_keyring()
    payload = json.dumps(
        {"v": 1, "key_name": key_name, "key_secret": key_secret},
        separators=(",", ":"),
    ).encode()
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(keyring[active_key_id]).encrypt(
        nonce,
        payload,
        _associated_data(active_key_id),
    )
    return ".".join(
        [COINBASE_CAPSULE_PREFIX, active_key_id, _encode_base64url(nonce + ciphertext)]
    )


def decrypt_coinbase_credentials(capsule: str) -> CoinbaseCredentials:
    if not capsule or len(capsule) > COINBASE_CAPSULE_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid Coinbase access key")

    try:
        prefix, key_id, encoded_payload = capsule.split(".", 2)
        if prefix != COINBASE_CAPSULE_PREFIX:
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

        key_name = payload.get("key_name")
        key_secret = payload.get("key_secret")
        if not isinstance(key_name, str) or not isinstance(key_secret, str):
            raise ValueError("Invalid credentials")
        if not key_name or not key_secret:
            raise ValueError("Invalid credentials")
        return CoinbaseCredentials(key_name=key_name, key_secret=key_secret)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Coinbase access key") from exc
