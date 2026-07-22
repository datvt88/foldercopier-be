import os
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import event

# Đảm bảo thư mục data tồn tại
os.makedirs("/app/data", exist_ok=True)
DATABASE_URL = "sqlite:////app/data/app.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class TaskToken(Base):
    __tablename__ = "task_tokens"
    task_id = Column(String, primary_key=True, index=True)
    refresh_token = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)
