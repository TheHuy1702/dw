import psycopg2
import datetime
import socket, getpass
import xml.etree.ElementTree as ET

cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()
control = cfg.find("databases/control_db")

def get_conn():
    return psycopg2.connect(
        host=control.find('host').text,
        port=control.find('port').text,
        database=control.find('name').text,
        user=control.find('user').text,
        password=control.find('password').text
    )


def check_craw_ready(job_name):
    conn = get_conn()
    cur = conn.cursor()

    # Kiểm tra tồn tại & có bật hay không
    cur.execute("""
        SELECT is_enabled FROM craw_config
        WHERE job_name=%s
    """, (job_name,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return False, "Job chưa được khai báo"

    is_enabled = row[0]
    if not is_enabled:
        conn.close()
        return False, "Job bị tắt"

    # Kiểm tra đã chạy hôm nay chưa
    today = datetime.date.today()
    cur.execute("""
        SELECT 1 FROM raw_config_log
        WHERE job_name=%s AND run_date=%s AND status='SUCCESS'
    """, (job_name, today))

    if cur.fetchone():
        conn.close()
        return False, "Hôm nay đã chạy rồi"

    conn.close()
    return True, "OK"


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


def write_craw_log(job_name, status, message):
    conn = get_conn()
    cur = conn.cursor()

    now = datetime.datetime.now()
    ip_addr = get_local_ip()
    run_by = getpass.getuser()

    cur.execute("""
        INSERT INTO raw_config_log(job_name, run_date, run_time, status, message, run_by, hostname)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name, now.date(), now, status, message,
        run_by, ip_addr
    ))

    conn.commit()
    conn.close()

