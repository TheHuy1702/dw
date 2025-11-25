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
# CHECK transform_clean đã chạy hôm nay chưa?
# ================================
def check_transform_done_today():
    today = datetime.date.today()
    conn = get_control_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name='transform_clean'
        AND run_date=%s
        AND status='SUCCESS'
    """, (today,))

    ok = cur.fetchone() is not None
    conn.close()
    return ok


# ================================
# CHECK append_clean đã chạy hôm nay chưa?
# ================================
def check_append_done_today():
    today = datetime.date.today()
    conn = get_control_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM process_log
        WHERE process_name='append_clean'
        AND run_date=%s
        AND status='SUCCESS'
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
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def write_process_log(process_name, status, message):
    conn = get_control_conn()
    cur = conn.cursor()
    now = datetime.datetime.now()

    cur.execute("""
        INSERT INTO process_log(process_name, run_date, run_time, status, message, run_by, hostname)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        process_name,
        now.date(),
        now,
        status,
        message,
        getpass.getuser(),
        get_local_ip()
    ))

    conn.commit()
    conn.close()


# ================================
# APPEND CLEAN → DAILY (SCD2)
# ================================
def run_append_clean():
    process_name = "append_clean"
    # ===== CHECK 0: is_enabled =====
    can_run, msg = check_process_ready(process_name)
    if not can_run:
        print("Không chạy:", msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return

    # ===== CHECK 1: Hôm nay đã chạy chưa?
    if check_append_done_today():
        msg = "Hôm nay append_clean đã chạy SUCCESS → bỏ qua."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        return

    # ===== CHECK 2: transform_clean chưa chạy thì không được append
    if not check_transform_done_today():
        msg = "Hôm nay transform_clean CHƯA chạy SUCCESS → không append."
        print(msg)
        write_process_log(process_name, "SKIPPED", msg)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        return

    print("Đang chạy append_clean (SCD2) ...")

    try:
        conn = psycopg2.connect(
            host=db_stg.find('host').text,
            port=db_stg.find('port').text,
            database=db_stg.find('name').text,
            user=db_stg.find('user').text,
            password=db_stg.find('password').text
        )
        cur = conn.cursor()

        # Lấy dữ liệu clean
        cur.execute("""
            SELECT masp, url, tensp, gia, giagoc, khuyenmai,
                   img, danhgia, daban, loai, ngaycapnhat
            FROM sanpham_clean;
        """)
        rows = cur.fetchall()

        if len(rows) == 0:
            msg = "Không có dữ liệu trong sanpham_clean."
            print(msg)
            write_process_log(process_name, "FAILED", msg)
            send_mail(process_name, msg)
            return

        updated_cnt = 0

        for r in rows:
            masp, url, tensp, gia, giagoc, km, img, dg, dban, loai, ngay_new = r

            # ===== Lấy date_key =====
            cur.execute("""
                SELECT date_key
                FROM date_dim
                WHERE day_data=%s AND month_data=%s AND year_data=%s
            """, (ngay_new.day, ngay_new.month, ngay_new.year))

            row_date = cur.fetchone()
            if not row_date:
                msg = f"Không tìm thấy date_key cho ngày {ngay_new}"
                print(msg)
                write_process_log(process_name, "FAILED", msg)
                send_mail(process_name, msg)
                return

            date_key_new = row_date[0]

            # ===== Lấy bản hiện hành =====
            cur.execute("""
                SELECT id, url, tensp, gia, giagoc, khuyenmai,
                       img, danhgia, daban
                FROM sanpham_daily
                WHERE masp=%s AND ngayhethan='9999-12-31'
                ORDER BY id DESC LIMIT 1
            """, (masp,))

            exist = cur.fetchone()

            # ===== Nếu chưa có bản nào: insert mới =====
            if not exist:
                cur.execute("""
                    INSERT INTO sanpham_daily(
                        masp, url, tensp, gia, giagoc, khuyenmai, img,
                        danhgia, daban, loai, ngaycapnhat, ngayhethan, date_key
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,'9999-12-31',%s)
                """, (masp, url, tensp, gia, giagoc, km, img,
                      dg, dban, loai, ngay_new, date_key_new))
                continue

            # ===== So sánh thay đổi =====
            (old_id, old_url, old_tensp, old_gia, old_giagoc,
             old_km, old_img, old_dg, old_dban) = exist

            changed = (
                old_url != url or
                old_tensp != tensp or
                old_gia != gia or
                old_giagoc != giagoc or
                old_km != km or
                old_img != img or
                old_dg != dg or
                old_dban != dban
            )

            if not changed:
                continue

            # ===== Giá trị thay đổi → đóng bản cũ =====
            cur.execute("""
                UPDATE sanpham_daily
                SET ngayhethan=%s
                WHERE id=%s
            """, (ngay_new, old_id))

            # ===== Insert bản mới =====
            cur.execute("""
                INSERT INTO sanpham_daily(
                    masp, url, tensp, gia, giagoc, khuyenmai, img,
                    danhgia, daban, loai, ngaycapnhat, ngayhethan, date_key
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,'9999-12-31',%s)
            """, (masp, url, tensp, gia, giagoc, km, img,
                  dg, dban, loai, ngay_new, date_key_new))

            updated_cnt += 1

        # Commit
        conn.commit()
        conn.close()

        msg = f"Append_clean thành công {updated_cnt} bản ghi."
        print(msg)
        write_process_log(process_name, "SUCCESS", msg)

    except Exception:
        err = traceback.format_exc()
        print("Lỗi append_clean:", err)
        write_process_log(process_name, "FAILED", err)
        send_mail(process_name, msg, subject="[ETL WARNING] Missing dependency")
        raise


# ================================
# CHẠY CHƯƠNG TRÌNH
# ================================
if __name__ == "__main__":
    run_append_clean()
