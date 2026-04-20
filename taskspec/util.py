import hashlib
import uuid
import base64
import os
from typing import Optional

def gen_task_id(key: str = "") -> str:
    if key:
        # Use MD5 for idempotent key
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        raw_bytes = m.digest()
    else:
        # Use UUID v4
        raw_bytes = uuid.uuid4().bytes

    # Base32 encode and remove padding
    b32 = base64.b32encode(raw_bytes).decode('utf-8').rstrip('=')
    return b32

def fset(path: str, text: str="", encoding='utf-8') -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding=encoding) as f:
        f.write(text)

def fdel(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)

def fget(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
