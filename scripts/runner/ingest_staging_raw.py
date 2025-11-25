import psycopg2
import csv
import os
import datetime
import socket, getpass
import xml.etree.ElementTree as ET
import traceback
import sys
sys.path.append("/opt/shared/dw_project/scripts")
from notify_mail import send_mail

cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()

db_stg = cfg.find('databases/staging_db')
db_ctl = cfg.find('databases/control_db')
RAW_DIR = cfg.find('storage/raw_dir').text


# =============================
# KẾT NỐI CONTROL DB
# =============================
def get_control_conn():
    return psycopg2.connect(
        host=db_ctl.find('host').text,
        port=db_ctl.find('port').text,
        database=db_ctl.find('name').text,
        user=db_ctl.find('user').text,
        password=db_ctl.find('password').text
    )

# ============================================================
# CHECK is_enabled TRONG process_config
# ============================================================
def check_process_ready(process_name):
    conn = get_control_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT is_enabled
        FROM process_config
        WHERE process_name=%s
    """, (process_name,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return False, "Process chưa khai báo"

    is_enabled = row[0]

    if not is_enabled:
        conn.close()
        return False, "Process bị TẮT trong process_config"

    conn.close()
    return True, "OK"

# =============================
# KIỂM TRA SCRAPER HÔM NAY ĐÃ CHẠY SUCCESS
# =============================
def check_scraper_ok_today(job_name="scraper_dienthoai"):
    today = datetime.date.today()

    conn = get_control_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1
        FROM raw_config_log
        WHERE job_name=%s AND run_date=%s AND status='SUCCESS'
    """, (job_name, today))

    ok = cur.fetchone() is not None
    conn.close()
    return ok


# =============================
# KIỂM TRA INGEST HÔM NAY ĐÃ CHẠY SUCCESS
# =============================
def check_ingest_done_today(process_name="ingest_raw"):
    today = datetime.date.today()

    conn = get_control_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1
        FROM process_log
        WHERE process_name=%s AND run_date=%s AND status='SUCCESS'
    """, (process_name, today))

    ok = cur.fetchone() is not None
    conn.close()
    return ok


# =============================
# GHI LOG PROCESS
# =============================

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))  # không cần gửi dữ liệu
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def write_process_log(process_name, status, message):
    conn = get_control_conn()
    cur = conn.cursor()

    now = datetime.datetime.now()
    host = get_local_ip()
    run_by = getpass.getuser()           # user hệ thống

    cur.execute("""
        INSERT INTO process_log(process_name, run_date, run_time, status, message, run_by, hostname)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        process_name, now.date(), now, status, message,
        run_by, host
    ))

    conn.commit()
    conn.close()


# =============================
# INGEST RAW
# =============================
def ingest_raw():
    process_name = "ingest_raw"
    
    # ===== CHECK 0: is_enabled =====
    can_run, msg = check_process_ready(process_name)
    if not can_run:
        print("Không chạy:", msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return


    # ========= CHECK 1: ingest hôm nay đã chạy? =========
    if check_ingest_done_today(process_name):
        msg = "Hôm nay ingest_raw đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    # ========= CHECK 2: scraper hôm nay đã chạy? =========
    if not check_scraper_ok_today():
        msg = "scraper_dienthoai hôm nay CHƯA chạy SUCCESS → không ingest."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return

    # ========= CHECK 3: Tìm đúng file ngày hôm nay =========
    today_str = datetime.datetime.now().strftime("%d_%m_%Y")
    filename_today = f"dtdt_{today_str}.csv"
    file_path = os.path.join(RAW_DIR, filename_today)

    if not os.path.exists(file_path):
        msg = f"Không tìm thấy file CSV của hôm nay: {filename_today}"
        print(msg)
        write_process_log(process_name, "FAILED", msg)
        send_mail(process_name, msg)
        return

    print(f"Import file {file_path} → staging.sanpham_raw")

    try:
        # ========= CONNECT STAGING =========
        conn = psycopg2.connect(
            host=db_stg.find('host').text,
            port=db_stg.find('port').text,
            database=db_stg.find('name').text,
            user=db_stg.find('user').text,
            password=db_stg.find('password').text
        )
        cur = conn.cursor()

        # ========= CREATE TABLE IF NOT EXISTS =========
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sanpham_raw (
            masp TEXT,
            url TEXT,
            tensp TEXT,
            gia TEXT,
            giagoc TEXT,
            khuyenmai TEXT,
            img TEXT,
            danhgia TEXT,
            daban TEXT,
            ngaycapnhat TEXT,
            loai TEXT
        );
        """)

        # ========= XÓA DỮ LIỆU CŨ =========
        cur.execute("TRUNCATE sanpham_raw;")

        # ========= INSERT CSV =========
        count = 0
        with open(file_path, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO sanpham_raw VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    row['MaSP'], row['URL'], row['TenSP'], row['Gia'], row['GiaGoc'],
                    row['KhuyenMai'], row['Img'], row['DanhGia'], row['DaBan'],
                    row['NgayCapNhat'], row['Loai']
                ))
                count += 1

        conn.commit()
        conn.close()

        msg = f"Ingest thành công {count} bản ghi."
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception:
        error_msg = traceback.format_exc()
        print("Lỗi ingest:", error_msg)
        write_process_log(process_name, "FAILED", error_msg)
# Gửi email báo lỗi
        send_mail(process_name, error_msg)
        raise


if __name__ == "__main__":
    ingest_raw()
