import hashlib


def hash_payload(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()