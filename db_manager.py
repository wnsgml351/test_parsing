import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

# DB 접속 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'db': os.getenv('DB_NAME'),
    'charset': 'utf8mb4',
    'autocommit': False
}

def get_db_connection():
    """DB 연결 객체를 반환하는 공통 함수"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return None
