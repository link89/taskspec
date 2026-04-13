import hashlib
import uuid
import base64

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
