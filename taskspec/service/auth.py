import os
import json
import hashlib
import secrets
from typing import Dict
from pydantic import BaseModel

class AuthEntry(BaseModel):
    key: str
    salt: str
    secret_hash: str

class AuthService:
    def __init__(self, auth_file: str):
        self._auth_file = auth_file
        self._auth_entries: Dict[str, AuthEntry] = {}

    def load(self):
        if not os.path.exists(self._auth_file):
            raise FileNotFoundError(f"Auth file not found: {self._auth_file}")

        with open(self._auth_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                entry = AuthEntry.model_validate(json.loads(line))
                self._auth_entries[entry.key] = entry

    def verify(self, key: str, secret: str) -> bool:
        if key not in self._auth_entries:
            return False

        entry = self._auth_entries[key]
        return self._hash_secret(secret, entry.salt) == entry.secret_hash

    @staticmethod
    def _hash_secret(secret: str, salt: str) -> str:
        return hashlib.sha256((secret + salt).encode('utf-8')).hexdigest()

    @classmethod
    def add_key(cls, auth_file: str, key: str, secret: str):
        salt = secrets.token_hex(16)
        secret_hash = cls._hash_secret(secret, salt)
        entry = AuthEntry(key=key, salt=salt, secret_hash=secret_hash)

        os.makedirs(os.path.dirname(os.path.abspath(auth_file)), exist_ok=True)
        with open(auth_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry.model_dump()) + '\n')
