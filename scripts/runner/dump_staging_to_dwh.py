#!/usr/bin/env python3
import psycopg2
import datetime
import socket, getpass
import traceback
import xml.etree.ElementTree as ET
import sys
sys.path.append("/opt/shared/dw_project/scripts")
from notify_mail import send_mail

# ================================
# ĐỌC CONFIG
# ================================
cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()
db_stg = cfg.find('databases/staging_db')
db_dwh = cfg.find('databases/datawarehouse_db')
db_ctl = cfg.find('databases/control_db')

# ================================
# HÀM KẾT NỐI
# ================================
def get_conn(db_node):
    return psycopg2.connect(
        host=db_node.find('host').text,
        port=db_node.find('port').text if db_node.find('port') else '5432',
        database=db_node.find('name').text,
        user=db_node.find('user').text,
        password=db_node.find('password').text
    )

# ================================
# GHI LOG + GET IP
# ================================
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def write_process_log(process_name, status, message=""):
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    now = datetime.datetime.now()
    cur.execute("""
        INSERT INTO process_log(process_name, run_date, run_time, status, message, run_by, hostname)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (process_name, now.date(), now, status, str(message)[:500], getpass.getuser(), get_local_ip()))
    conn.commit()
    conn.close()

# ================================
# CHECK ĐÃ CHẠY HÔM NAY CHƯA + DEPENDENCY
# ================================
def check_already_run_today(process_name):
    today = datetime.date.today()
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM process_log WHERE process_name=%s AND run_date=%s AND status='SUCCESS'", (process_name, today))
    result = cur.fetchone() is not None
    conn.close()
    return result

def check_append_clean_done_today():
    today = datetime.date.today()
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM process_log WHERE process_name='append_clean' AND run_date=%s AND status='SUCCESS'", (today,))
    result = cur.fetchone() is not None
    conn.close()
    return result

# ================================
# SCRIPT 5: TRUNCATE + INSERT TOÀN BỘ 
# ================================
def load_dim_sanpham_full():
    process_name = "load_dim_sanpham_full"

    # 1. Kiểm tra đã chạy hôm nay chưa
    if check_already_run_today(process_name):
        msg = "Hôm nay load_dim_sanpham_full đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    # 2. Kiểm tra append_clean đã chạy chưa
    if not check_append_clean_done_today():
        msg = "append_clean chưa chạy hôm nay → KHÔNG load dim_sanpham!"
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL BLOCKED] Thiếu append_clean")
        return

    print("Bắt đầu TRUNCATE dim_sanpham và load dữ liệu mới nhất từ sanpham_daily...")

    stg_conn = None
    dwh_conn = None
    try:
        stg_conn = get_conn(db_stg)
        dwh_conn = get_conn(db_dwh)
        stg_cur = stg_conn.cursor()
        dwh_cur = dwh_conn.cursor()

        # 1. TRUNCATE bảng đích
        dwh_cur.execute("TRUNCATE TABLE dim_sanpham;")

        # 2. Copy toàn bộ bản ghi hiện hành từ sanpham_daily
        stg_cur.execute("""
            SELECT 
                masp, url, tensp, gia, giagoc, khuyenmai, img,
                danhgia, daban, loai, ngaycapnhat, ngayhethan, date_key
            FROM sanpham_daily
            WHERE ngayhethan = '9999-12-31'   -- chỉ lấy phiên bản hiện tại
        """)

        rows = stg_cur.fetchall()
        if not rows:
            msg = "Không có dữ liệu hiện hành trong sanpham_daily!"
            write_process_log(process_name, "FAILED", msg)
            send_mail(process_name, msg)
            return

        # 3. Chèn toàn bộ vào dim_sanpham
        insert_query = """
            INSERT INTO dim_sanpham (
                masp, url, tensp, gia, giagoc, khuyenmai, img,
                danhgia, daban, loai, ngaycapnhat, ngayhethan, date_key
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        dwh_cur.executemany(insert_query, rows)

        dwh_conn.commit()
        stg_conn.close()
        dwh_conn.close()

        msg = f"Thành công! Đã load {len(rows)} sản phẩm mới nhất vào dim_sanpham (ngày {datetime.date.today()})"
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception as e:
        err = traceback.format_exc()
        print("LỖI load_dim_sanpham_full:", err)
        if dwh_conn:
            dwh_conn.rollback()
        write_process_log(process_name, "FAILED", err)
        send_mail(process_name, err, subject="[ETL FAILED] load_dim_sanpham_full")
        raise

# ================================
# CHẠY CHƯƠNG TRÌNH
# ================================
if __name__ == "__main__":
    load_dim_sanpham_full()