import os
import requests
import redis
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import SessionLocal, TaskToken
from worker import copy_drive_task
from celery.result import AsyncResult

app = FastAPI(title="FolderCopier Backend API")

origins = [
    "https://foldercopier.com",
    "https://www.foldercopier.com",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')

# Khởi tạo kết nối Redis để đọc dữ liệu thống kê
redis_client = redis.from_url(CELERY_BROKER_URL, decode_responses=True)

class CopyRequest(BaseModel):
    source_link: str
    dest_link: str
    auth_code: str

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root_health_check():
    return {"status": "Backend is running flawlessly", "service": "FolderCopier"}

@app.post("/api/copy")
async def start_copy(request: CopyRequest, db: Session = Depends(get_db)):
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Server OAuth credentials are not configured.")

    token_url = "https://oauth2.googleapis.com/token"
    payload = {
        "code": request.auth_code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": "postmessage",
        "grant_type": "authorization_code"
    }
    
    res = requests.post(token_url, data=payload)
    token_data = res.json()
    
    # Ép Backend trả về lỗi chi tiết từ Google Cloud
    if "error" in token_data:
        google_error = token_data.get('error_description', token_data.get('error', 'Unknown Error'))
        raise HTTPException(status_code=400, detail=f"Google Error: {google_error}")
        
    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        raise HTTPException(
            status_code=400, 
            detail="Failed to acquire Refresh Token. Please revoke access in your Google Account settings and try again."
        )

    task = copy_drive_task.delay(request.source_link, request.dest_link)
    
    db_token = TaskToken(task_id=task.id, refresh_token=refresh_token)
    db.add(db_token)
    db.commit()

    return {"message": "Task queued successfully", "task_id": task.id}

@app.get("/api/status/{task_id}")
async def get_status(task_id: str):
    task_result = AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": task_result.status,
        "info": task_result.info or {}
    }

# --- API MỚI: LẤY THỐNG KÊ SỐ LƯỢT COPY ---
@app.get("/api/stats")
async def get_system_stats():
    try:
        count = redis_client.get("total_successful_copies")
        return {"total_copies": int(count) if count else 0}
    except Exception as e:
        # Nếu Redis lỗi, trả về 0 để giao diện ẩn đi chứ không làm sập trang web
        return {"total_copies": 0}
