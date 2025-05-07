import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def process():
    print("Đang đọc dữ liệu từ file CSV...")
    df = pd.read_csv('quotes.csv')
    
    print("Đang xử lý dữ liệu...")
    df['Author'] = df['Author'].str.strip()
    
    # Lưu lại vào file CSV
    df.to_csv('quotes.csv', index=False)
    print("Đã lưu dữ liệu đã xử lý vào file CSV.")
    
    # Kết nối đến PostgreSQL và lưu dữ liệu
    print("Đang kết nối đến PostgreSQL...")
    try:
        # Tạo kết nối đến PostgreSQL
        conn_string = "postgresql://airflow:airflow@postgres:5432/airflow"
        db = create_engine(conn_string)
        
        # Lưu DataFrame vào PostgreSQL
        print("Đang lưu dữ liệu vào PostgreSQL...")
        df.to_sql('quotes', db, if_exists='replace', index=False)
        print("Đã lưu dữ liệu vào PostgreSQL thành công!")
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào PostgreSQL: {e}")

if __name__ == '__main__':
    process()
