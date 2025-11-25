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
db_dwh = cfg.find('databases/datawarehouse_db')     # DWH nguồn
db_dm  = cfg.find('databases/mart_db')          # Datamart đích
db_ctl = cfg.find('databases/control_db')           # Control

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
# CHECK is_enabled
# ================================
def check_process_ready(process_name):
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    cur.execute("SELECT is_enabled FROM process_config WHERE process_name=%s", (process_name,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return False, "Process chưa khai báo"
    if not row[0]:
        return False, "Process bị TẮT trong process_config"
    return True, "OK"

# ================================
# CHECK ALREADY RUN TODAY
# ================================
def already_run_today(process_name):
    today = datetime.date.today()
    conn = get_conn(db_ctl)
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name=%s AND run_date=%s AND status='SUCCESS'
    """, (process_name, today))
    result = cur.fetchone() is not None
    conn.close()
    return result

# ================================
# LOAD AGGREGATE → DATAMART
# ================================
def load_agg_to_datamart():
    process_name = "load_agg_to_datamart"

    # 1. is_enabled
    can_run, msg = check_process_ready(process_name)
    if not can_run:
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg)
        return

    # 2. avoid double-run
    if already_run_today(process_name):
        msg = "Hôm nay load_agg_to_datamart đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    print("Bắt đầu load aggregate sang datamart (không tạo schema, không tạo bảng)...")

    dwh_conn = None
    dm_conn  = None

    try:
        dwh_conn = get_conn(db_dwh)
        dm_conn  = get_conn(db_dm)

        dwh_cur = dwh_conn.cursor()
        dm_cur  = dm_conn.cursor()

        # 1. SELECT dữ liệu từ bảng agg ở DWH
        dwh_cur.execute("""
            SELECT masp, tensp, loai, gia, giagoc, daban, danhgia,
                   khuyenmai, img, url, update_date, report_date
            FROM agggregate_hot_products.agg_hot_products
        """)
        rows = dwh_cur.fetchall()

        # 2. TRUNCATE bảng đích trong Data Mart
        dm_cur.execute("TRUNCATE TABLE agg_hot_products;")

        # 3. Insert lại dữ liệu
        insert_sql = """
            INSERT INTO agg_hot_products (
                masp, tensp, loai, gia, giagoc, daban, danhgia,
                khuyenmai, img, url, update_date, report_date
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        dm_cur.executemany(insert_sql, rows)

        dm_conn.commit()

        msg = f"SUCCESS — Đã load {len(rows)} bản ghi vào agg_hot_products."
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception as e:
        err = traceback.format_exc()
        print("LỖI load_agg_to_datamart:", err)
        if dm_conn:
            dm_conn.rollback()
        write_process_log(process_name, "FAILED", err)
        send_mail(process_name, err)
        raise

# ================================
# RUN
# ================================
if __name__ == "__main__":
    load_agg_to_datamart()

