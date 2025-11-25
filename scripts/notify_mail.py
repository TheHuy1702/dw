import smtplib
import xml.etree.ElementTree as ET
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Load config.xml
cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()
mail_cfg = cfg.find("mail")

SMTP_HOST = mail_cfg.find("smtp_host").text
SMTP_PORT = int(mail_cfg.find("smtp_port").text)
SMTP_USER = mail_cfg.find("smtp_user").text
SMTP_PASS = mail_cfg.find("smtp_pass").text
MAIL_TO = mail_cfg.find("to").text
MAIL_FROM = mail_cfg.find("from").text


def send_mail(process_name, error_message, subject=None):
    """
    Gửi email báo lỗi ETL, trong đó:
    - process_name: tên file hoặc process gây lỗi
    - error_message: full traceback
    """

    if subject is None:
        subject = f"[ETL ERROR] Process {process_name} FAILED"

    # Tạo nội dung email HTML
    html_content = f"""
    <html>
    <body style="font-family: Arial;">

        <h2 style="color: #c0392b;">ETL PROCESS: {subject}</h2>

        <p><b>Process / File:</b> {process_name}</p>

        <p><b>Thời gian:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        
        <hr>
        <p><b>Chi tiết thông tin:</b></p>
        <pre style="background: #f4f4f4; padding: 10px; border-radius: 5px; border: 1px solid #ddd;">
{error_message}
        </pre>
        <hr>

        <p style="color: gray; font-size: 12px;">
            Đây là email tự động từ hệ thống Data Warehouse.
        </p>

    </body>
    </html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = MAIL_FROM
        msg["To"] = MAIL_TO
        msg["Subject"] = subject

        msg.attach(MIMEText(html_content, "html"))

        # Gửi qua SMTP Gmail
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
        server.quit()

        print(f"[MAIL] Đã gửi email lỗi ETL (process: {process_name})")

    except Exception as e:
        print("[MAIL] Lỗi khi gửi email:", e)
