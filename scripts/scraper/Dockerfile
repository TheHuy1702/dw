# Sử dụng image Python làm nền
FROM python:3.10-slim

# Cài các thư viện cần thiết
RUN pip install --no-cache-dir requests beautifulsoup4 lxml

# Tạo thư mục làm việc
WORKDIR /app

# Sao chép mã nguồn vào container
COPY scraper.py /app/scraper.py

# Chạy script khi container khởi động
CMD ["python", "/app/scraper.py"]

