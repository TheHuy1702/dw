import requests
from bs4 import BeautifulSoup
import csv
import os
import datetime
import xml.etree.ElementTree as ET
from craw_job_control import check_craw_ready, write_craw_log
import traceback
import sys
sys.path.append("/opt/shared/dw_project/scripts")
from notify_mail import send_mail


# Đọc cấu hình từ config.xml
cfg = ET.parse('/opt/shared/dw_project/config/config.xml').getroot()
BASE_URL = cfg.find('scraper/base_url').text.strip()
OUT_DIR = cfg.find('storage/raw_dir').text.strip()
UA = cfg.find('scraper/user_agent').text.strip()

HEADERS = {
    "User-Agent": UA
}

def fetch_tgdd_phones(url):
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    products = []
    for item in soup.select("ul.listproduct li.item"):
        a_tag = item.find("a", class_="main-contain")
        if not a_tag:
            continue

        # ID sản phẩm
        pid = item.get("data-id", "").strip()

        # URL
        href = a_tag.get("href", "")
        link = "https://www.thegioididong.com" + href if href.startswith("/") else href

        # Tên
        name = a_tag.get("data-name") or (a_tag.h3.text.strip() if a_tag.h3 else "")

        # Giá hiện tại
        price_tag = item.find("strong", class_="price")
        price = price_tag.text.strip() if price_tag else ""

        # Giá gốc
        old_price_tag = item.select_one(".box-p .price-old")
        old_price = old_price_tag.text.strip() if old_price_tag else ""

        # Khuyến mãi
        promo = ""
        gift = item.find("p", class_="item-gift")
        discount = item.find("span", class_="percent")
        label = item.find("p", class_="result-label")
        if gift:
            promo = gift.get_text(strip=True)
        elif discount:
            promo = discount.get_text(strip=True)
        elif label:
            promo = label.get_text(strip=True)

        # Ảnh
        img_tag = item.find("img", class_="thumb")
        img = (
            img_tag.get("data-src")
            or img_tag.get("src")
            or img_tag.get("data-original")
            or ""
        ) if img_tag else ""

        # Rating
        rating_tag = item.select_one(".vote-txt b")
        rating = rating_tag.text.strip() if rating_tag else ""

        # Đã bán / Đánh giá
        sold_tag = item.select_one(".rating_Compare span")
        sold = sold_tag.text.strip("• ").strip() if sold_tag else ""

        products.append([
            pid,
            link,
            name,
            price,
            old_price,
            promo,
            img,
            rating,
            sold,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Điện thoại",
        ])

    return products


if __name__ == "__main__":
    job = "scraper_dienthoai"

    # Kiểm tra job có thể chạy
    can_run, msg = check_craw_ready(job)
    if not can_run:
        print("Không chạy:", msg)
        write_craw_log(job, "SKIPPED", msg)
        sys.exit(0)

    try:
        # Tạo tên file theo ngày
        today = datetime.datetime.now().strftime("%d_%m_%Y")
        filename = f"dtdt_{today}.csv"
        out_path = os.path.join(OUT_DIR, filename)

        os.makedirs(OUT_DIR, exist_ok=True)

        print(f"Đang cào dữ liệu từ {BASE_URL} ...")
        data = fetch_tgdd_phones(BASE_URL)

        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "MaSP",
                "URL",
                "TenSP",
                "Gia",
                "GiaGoc",
                "KhuyenMai",
                "Img",
                "DanhGia",
                "DaBan",
                "NgayCapNhat",
                "Loai",
            ])
            writer.writerows(data)

        print(f"Đã lưu {len(data)} sản phẩm vào {out_path}")
        write_craw_log(job, "SUCCESS", f"Lưu {len(data)} bản ghi")
    except Exception as e:
        error_msg = traceback.format_exc()
        print("LỖI: ", error_msg)
        write_craw_log(job, "FAILED", error_msg)
# Gửi email báo lỗi
        send_mail(job, error_msg)
        raise
