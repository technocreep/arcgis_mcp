from _backend.auth_utils.constants import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, \
    SCHEMES, JWE_ALG, JWE_ENC, JWE_ENC_PASSWORD

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from jose import jwt, jwe
from datetime import datetime, timedelta, timezone
from jose.exceptions import JWTError, JWEError
from fastapi import Body, Request, HTTPException, Depends
from typing import Optional
import base64
import hashlib

from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

def derive_jwe_key() -> bytes:
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,  # ← РОВНО 32 БАЙТА для A256GCM
        salt=None,  # можно задать, но тогда нужно хранить
        info=b"jwe-a256gcm-key",
        backend=default_backend(),
    )
    return hkdf.derive(JWE_ENC_PASSWORD.encode("utf-8"))


pwd_context = CryptContext(schemes=[SCHEMES], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def normalize_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def hash_password(password: str):
    return pwd_context.hash(password)


def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)


def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    to_encode["exp"] = datetime.now(timezone.utc) + expires_delta
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def b64url_encode(data: bytes) -> str:
    # data уже bytes → просто кодируем в URL-safe string
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

def b64url_decode(data: str) -> bytes:
    padding = '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)

def _extract_token_from_request(request: Request) -> str | None:
    auth_header = request.headers.get("Authorization")
    if auth_header:
        if auth_header.startswith("Bearer "):
            return auth_header[len("Bearer "):].strip()
        return auth_header.strip()

    # Cookie-based auth (frontend uses credentials: "include")
    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        return cookie_token.strip()

    return None


async def verify_user(request: Request):
    token = _extract_token_from_request(request)
    if not token:
        print("No token found in request headers.")
        raise HTTPException(status_code=401, detail="Missing sacred token")
    try:
        payload = decode_token(token)
        user_id = payload.get("sub")
        token_version_in_token = payload.get("token_version")  

        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid sacred token(no data)")
    except JWTError as e:
        print(f"JWTError during token verification: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid sacred token")
    except Exception as e:
        print(f"Exception during token verification: {str(e)}")
        # Includes JWEParseError / decrypt failures when a non-JWE token is sent
        raise HTTPException(status_code=401, detail="Invalid sacred token, error during decryption or parsing")

    if int(user_id) == 0:
        if token_version_in_token != 0:
            print(f"Token version mismatch for user_id={user_id}: token has {token_version_in_token}, but DB has {email_exists_result.get('token_version')}")
            raise HTTPException(status_code=401, detail="Invalid sacred token (token version mismatch)")
        current_user = {"user_id": int(user_id)}
        try:
            request.state.current_user = current_user
        except Exception:
            pass
        return current_user
    raise HTTPException(status_code=404, detail="User not found")


def create_signed_jwt(user_id: int, token_version: int) -> str:
    payload = {
        "sub": str(user_id),
        "iss": "https://test.com",
        "aud": "api.example.com",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "token_version": token_version,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def encrypt_jwt(signed_jwt: str) -> str:
    return jwe.encrypt(
        signed_jwt,
        derive_jwe_key(),
        algorithm=JWE_ALG,
        encryption=JWE_ENC,
    )


def issue_token(user_id: str, token_version: int) -> str:
    signed = create_signed_jwt(user_id, token_version)
    encrypted = encrypt_jwt(signed)
    if isinstance(encrypted, bytes):
        encrypted = encrypted.decode("utf-8")
    return encrypted


def decode_token(token: str) -> dict:
    if not isinstance(token, str):
        raise JWTError("Token must be a string")

    # проверяем количество частей токена
    parts = token.split(".")
    #token = b64url_decode(token)
    if len(parts) == 5:
        try:
            decrypted = jwe.decrypt(token, derive_jwe_key())
            if isinstance(decrypted, (bytes, bytearray)):
                token_to_decode = decrypted.decode("utf-8")
            else:
                token_to_decode = decrypted
        except JWEError:
            raise JWTError("Invalid sacred token (cannot decrypt)")
    else:
        token_to_decode = token

    # Проверяем подпись JWT
    payload = jwt.decode(
        token_to_decode,
        SECRET_KEY,
        algorithms=[ALGORITHM],
        options={"verify_aud": False, "verify_iss": False},
    )

    return payload



if __name__ == "__main__":
    id = 2
    token = issue_token(id)

    print(f"Token for user_id={id}: {token}")

    decoded = decode_token(token.decode("utf-8"))

    print(f"Decoded token payload: {decoded}")