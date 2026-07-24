import os
import re
import time
import redis  # Thêm thư viện redis
from celery import Celery
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from database import SessionLocal, TaskToken

CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

# Khởi tạo Celery
celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

# Khởi tạo Redis client cho bộ đếm thống kê (Tận dụng luôn URL của Broker)
redis_client = redis.from_url(CELERY_BROKER_URL)

def extract_id(url: str) -> str:
    match = re.search(r'[-\w]{25,}', url)
    return match.group(0) if match else url

def execute_with_backoff(request, max_retries=6):
    for n in range(max_retries):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status in [403, 429, 500, 503]:
                time.sleep(2 ** n) 
            else:
                raise e
    raise Exception("API rate limit exceeded. Please try again later.")

@celery_app.task(bind=True)
def copy_drive_task(self, source_link: str, dest_link: str):
    task_id = self.request.id
    
    db = SessionLocal()
    try:
        token_record = db.query(TaskToken).filter(TaskToken.task_id == task_id).first()
        if not token_record:
            self.update_state(state='FAILURE', meta={'error': 'Token authentication missing.'})
            raise Exception("Token missing")
            
        refresh_token = token_record.refresh_token

        self.update_state(state='PROGRESS', meta={'progress': 1, 'message': 'Connecting to Google API...'})
        
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET
        )
        service = build('drive', 'v3', credentials=creds)

        source_id = extract_id(source_link)
        dest_id = extract_id(dest_link)
        
        def copy_recursive(src_folder_id, target_folder_id, current_count=0):
            query = f"'{src_folder_id}' in parents and trashed = false"
            page_token = None
            
            while True:
                results = execute_with_backoff(
                    service.files().list(
                        q=query, 
                        fields="nextPageToken, files(id, name, mimeType)", 
                        pageToken=page_token,
                        supportsAllDrives=True, 
                        includeItemsFromAllDrives=True,
                        pageSize=100
                    )
                )
                items = results.get('files', [])

                for item in items:
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        folder_metadata = {
                            'name': item['name'],
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [target_folder_id]
                        }
                        new_folder = execute_with_backoff(
                            service.files().create(body=folder_metadata, fields='id', supportsAllDrives=True)
                        )
                        current_count = copy_recursive(item['id'], new_folder['id'], current_count)
                    else:
                        file_metadata = {'parents': [target_folder_id]}
                        execute_with_backoff(
                            service.files().copy(fileId=item['id'], body=file_metadata, supportsAllDrives=True)
                        )
                        current_count += 1
                        
                        display_progress = min(99, int(5 + (current_count % 94))) 
                        self.update_state(state='PROGRESS', meta={
                            'progress': display_progress,
                            'message': f"Copied {current_count} files..."
                        })
                        
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
                    
            return current_count

        total_copied = copy_recursive(source_id, dest_id)
        
        # --- THÊM MỚI: TĂNG BIẾN ĐẾM THỐNG KÊ ---
        try:
            redis_client.incr("total_successful_copies")
        except Exception:
            pass # Bỏ qua âm thầm nếu Redis lỗi, không làm gián đoạn quy trình
        # ----------------------------------------

        return {'progress': 100, 'message': f'Completed! Successfully duplicated {total_copied} items.'}

    except Exception as e:
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise e
    finally:
        # Đảm bảo xóa token khỏi database bất kể thành công hay thất bại
        db.query(TaskToken).filter(TaskToken.task_id == task_id).delete()
        db.commit()
        db.close()
