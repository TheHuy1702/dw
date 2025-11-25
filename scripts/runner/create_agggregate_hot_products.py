#!/usr/bin/env python3
# File: /opt/shared/dw_project/scripts/runner/create_agggregate_hot_products.py

import psycopg2
import datetime
import socket
import getpass
import traceback
import xml.etree.ElementTree as ET
import sys

sys.path.append("/opt/shared/dw_project/scripts")
from notify_mail import send_mail

# ================================
# ĐỌC CONFIG
# ================================
cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()
db_dwh = cfg.find('databases/datawarehouse_db')   # dwhouse
db_ctl = cfg.find('databases/control_db')         # control

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

# ============================================================
# CHECK is_enabled TRONG process_config
# ============================================================
def check_process_ready(process_name):
    conn = get_conn(db_ctl)
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

# ================================
# LOG UTILS
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
# KIỂM TRA ĐÃ CHẠY HÔM NAY CHƯA
# ================================
def already_run_today(process_name):
    today = datetime.date.today()
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name = %s AND run_date = %s AND status = 'SUCCESS'
    """, (process_name, today))
    result = cur.fetchone() is not None
    conn.close()
    return result

# ================================
# CHẠY CHÍNH – TẠO / CẬP NHẬT BẢNG AGGREGATE
# ================================
def create_agggregate_hot_products():
    process_name = "create_agggregate_hot_products"

     # ===== CHECK 0: is_enabled =====
    can_run, msg = check_process_ready(process_name)
    if not can_run:
        print("Không chạy:", msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return

    # 1. Kiểm tra đã chạy hôm nay chưa → tránh chạy 2 lần
    if already_run_today(process_name):
        msg = "Hôm nay create_agggregate_hot_products đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    print("Bắt đầu khởi tạo agggregate_hot_products.agg_hot_products (Top 20 HOT nhất)...")

    conn = None
    try:
        conn = get_conn(db_dwh)
        cur = conn.cursor()

        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS agggregate_hot_products;
            DROP TABLE IF EXISTS agggregate_hot_products.agg_hot_products;

            CREATE TABLE agggregate_hot_products.agg_hot_products AS
            WITH current_week AS (
                SELECT date_key
                FROM date_dim
                WHERE make_date(year_data, month_data, day_data) >= date_trunc('week', CURRENT_DATE)
                  AND make_date(year_data, month_data, day_data) <= CURRENT_DATE
            )
            SELECT
                s.masp,
                s.tensp,
                s.loai,
                s.gia::bigint                              AS gia,
                s.giagoc::bigint                           AS giagoc,
                COALESCE(s.daban, 0)                       AS daban,
                COALESCE(s.danhgia, 0)::numeric(3,1)       AS danhgia,
                s.khuyenmai,
                s.img,
                s.url,
                s.ngaycapnhat                              AS update_date,
                CURRENT_DATE                               AS report_date
            FROM dim_sanpham s
            JOIN current_week cw ON s.date_key = cw.date_key
            ORDER BY daban DESC NULLS LAST, danhgia DESC NULLS LAST
            LIMIT 20;
        """)

        conn.commit()
        cur.close()
        conn.close()

        msg = f"Thành công! Đã khởi tạo Top 20 sản phẩm HOT nhất TUẦN từ {datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday())} đến nay"
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception as e:
        err = traceback.format_exc()
        print("LỖI create_agggregate_hot_products:", err)
        if conn:
            conn.rollback()
        write_process_log(process_name, "FAILED", err)
        send_mail(
            process_name,
            err,
            subject="[Data Mart] Lỗi khởi tạo Top 20 HOT"
        )
        raise

# ================================
# CHẠY
# ================================
if __name__ == "__main__":
    create_agggregate_hot_products()