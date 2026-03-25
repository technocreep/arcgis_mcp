from fastapi import FastAPI, Body, Request, HTTPException, Depends, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware

from passlib.context import CryptContext
import time

from pydantic import BaseModel


from _backend.auth_utils.constants import ACCESS_TOKEN_EXPIRE_MINUTES
from _backend.auth_utils.auth_utils import create_access_token, \
    verify_password, hash_password, normalize_password, issue_token
from _backend.auth_utils.auth_utils import verify_user
from _projects_API import router as projects_router
from _upload_API import router as upload_router



import _data_read_API

class Token(BaseModel):
    access_token: str
    token_type: str


app = FastAPI(debug=True)

app.include_router(_data_read_API.router)
app.include_router(projects_router)
app.include_router(upload_router)
#app.include_router(_data_read_API.public_router)

@app.middleware("http")
async def strip_api_prefix(request: Request, call_next):
    path = request.scope.get("path", "")
    # If request starts with /api/, remove the prefix so existing routes continue to work
    if path == "/api":
        request.scope["path"] = "/"
    elif path.startswith("/api/"):
        request.scope["path"] = path[len("/api") :]
    return await call_next(request)

origins = [
    "http://localhost:5173",  # Vite dev
    "http://127.0.0.1:5173",
    "http://localhost:3000",  # other dev ports
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # 🔑 важно для httponly cookies
    allow_methods=["*"],
    allow_headers=["*"],
)

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.middleware("http")
async def chrono_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start
    print(f"[CHRONO] {request.url.path} → {duration:.6f}s")
    return response







@app.post("/token", response_model=Token)
async def login(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    user_name = form_data.username
    password = form_data.password

    # Simple single-user authentication (no DB yet)
    if not (user_name == "admin" and password == "password"):
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    token = issue_token(0, 0)
    print(f"Issued token for user_id=0: {token}")
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES * 60),
        path="/",
    )
    return {"access_token": token, "token_type": "bearer"}


@app.get("/user_info")
async def user_info(request: Request, user=Depends(verify_user)):
    """Return minimal information about current user. Protected endpoint.

    This endpoint allows the frontend to confirm the login succeeded when the
    backend uses httponly cookies for session/token storage.
    """
    # verify_user sets request.state.current_user when successful
    current = getattr(request.state, "current_user", None)
    return {"user": "admin"}

if __name__ == "__main__":
	import uvicorn
	uvicorn.run(app, host="0.0.0.0", port=8000)
