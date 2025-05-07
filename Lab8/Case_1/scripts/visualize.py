import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sqlalchemy import create_engine
import os

def visualize():
    # Tạo thư mục visualizations nếu chưa tồn tại
    vis_dir = '/opt/airflow/visualizations'
    if not os.path.exists(vis_dir):
        os.makedirs(vis_dir)
    
    # Kết nối đến PostgreSQL và đọc dữ liệu
    print("Đang kết nối đến PostgreSQL...")
    try:
        # Tạo kết nối đến PostgreSQL
        conn_string = "postgresql://airflow:airflow@postgres:5432/airflow"
        db = create_engine(conn_string)
        
        # Đọc dữ liệu từ PostgreSQL
        print("Đang đọc dữ liệu từ PostgreSQL...")
        df = pd.read_sql("SELECT * FROM quotes", db)
        print(f"Đã đọc {len(df)} bản ghi từ PostgreSQL.")
        
        # Nếu không thể kết nối tới PostgreSQL, thử đọc từ file CSV
        if df.empty:
            print("Không có dữ liệu trong PostgreSQL, đang đọc từ file CSV...")
            df = pd.read_csv('quotes.csv')
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu từ PostgreSQL: {e}")
        print("Đang đọc từ file CSV...")
        df = pd.read_csv('quotes.csv')

    # Tạo biểu đồ top 10 tác giả
    print("Đang tạo biểu đồ top 10 tác giả...")
    author_counts = df['Author'].value_counts()
    author_counts[:10].plot(kind='bar', color='skyblue')
    plt.title('Top 10 tác giả có nhiều trích dẫn nhất')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_dir, 'top_authors.png'))
    print(f"Đã lưu biểu đồ {os.path.join(vis_dir, 'top_authors.png')}")

    # Tạo word cloud
    print("Đang tạo word cloud...")
    text_all = " ".join(df['Quote'])
    wc = WordCloud(width=800, height=400, background_color='white').generate(text_all)
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(os.path.join(vis_dir, 'wordcloud.png'))
    print(f"Đã lưu wordcloud {os.path.join(vis_dir, 'wordcloud.png')}")

if __name__ == '__main__':
    visualize()
