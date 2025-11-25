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
db_ctl = cfg.find('databases/control_db')

# ================================
# HÀM KẾT NỐI CONTROL DB
# ================================
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

# ================================
# KIỂM TRA HÔM NAY INGEST ĐÃ CHẠY CHƯA
# ================================
def check_ingest_done_today():
    today = datetime.date.today()
    conn = get_control_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name='ingest_raw' AND run_date=%s AND status='SUCCESS'
    """, (today,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

# ================================
# KIỂM TRA HÔM NAY TRANSFORM ĐÃ CHẠY CHƯA
# ================================
def check_transform_done_today():
    today = datetime.date.today()
    conn = get_control_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name='transform_clean' AND run_date=%s AND status='SUCCESS'
    """, (today,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

# ================================
# GHI LOG PROCESS
# ================================

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
    host=get_local_ip()
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

# ================================
# THỰC THI TRANSFORM
# ================================
def run_transform():
    process_name = "transform_clean"
    # ===== CHECK 0: is_enabled =====
    can_run, msg = check_process_ready(process_name)
    if not can_run:
        print("Không chạy:", msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")

        return

    # ===== CHECK 1: Hôm nay đã transform chưa? =====
    if check_transform_done_today():
        msg = "Hôm nay transform_clean đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    # ===== CHECK 2: ingest_raw đã chạy chưa? =====
    if not check_ingest_done_today():
        msg = "Hôm nay ingest_raw CHƯA chạy SUCCESS → không transform."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return

    print("Đang transform dữ liệu staging_raw → staging_clean ...")

    try:
        conn = psycopg2.connect(
            host=db_stg.find('host').text,
            port=db_stg.find('port').text,
            database=db_stg.find('name').text,
            user=db_stg.find('user').text,
            password=db_stg.find('password').text
        )
        cur = conn.cursor()

        sql = open('/opt/shared/dw_project/filesql/transform_staging_clean.sql').read()
        cur.execute(sql)

        conn.commit()
        conn.close()

        msg = "Transform dữ liệu thành công."
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception:
        error_msg = traceback.format_exc()
        print("Lỗi transform:", error_msg)
        write_process_log(process_name, "FAILED", error_msg)
# Gửi email báo lỗi
        send_mail(process_name, error_msg)
        raise

# ================================
# CHẠY CHƯƠNG TRÌNH
# ================================ 
if __name__ == "__main__":
    run_transform()
