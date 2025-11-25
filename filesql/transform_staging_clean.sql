TRUNCATE TABLE sanpham_clean;
INSERT INTO sanpham_clean
SELECT
  masp,
  url,
  tensp,

  -- Giá số
  CASE WHEN gia ~ '^[0-9.]' THEN REPLACE(REPLACE(gia,'₫',''),'.','')::FLOAT ELSE NULL END AS gia,

  -- Giá gốc, null thì = -1
  CASE 
      WHEN giagoc ~ '^[0-9.]' THEN REPLACE(REPLACE(giagoc,'₫',''),'.','')::FLOAT 
      ELSE -1 
  END AS giagoc,

  -- Khuyến mãi, null thì "Chưa có"
  COALESCE(NULLIF(khuyenmai, ''), 'Chưa có') AS khuyenmai,

  img,

 CASE 
    WHEN danhgia ~ '^[0-9,\.]+$' THEN REPLACE(danhgia, ',', '.')::FLOAT
    ELSE -1
END AS danhgia,

  -- Đã bán → chuyển thành FLOAT
COALESCE(
    CASE
        -- Trường hợp có chữ "Đã bán ..."
        WHEN daban ILIKE 'Đã bán %' THEN
           (
	    CASE
                -- số kiểu 35,5k → 35.5 * 1000
                WHEN SPLIT_PART(daban, ' ', 3) ILIKE '%k%' THEN
                    (REPLACE(REPLACE(SPLIT_PART(daban, ' ', 3), 'k',''), ',', '.')::FLOAT * 1000)
                -- số thường 80 → 80
                ELSE
                    REPLACE(SPLIT_PART(daban, ' ', 3), ',', '.')::FLOAT
            END
           )
        -- Trường hợp không có chữ "Đã bán" nhưng có k
        WHEN daban ILIKE '%k%' THEN
            (REPLACE(REPLACE(daban, 'k', ''), ',', '.')::FLOAT * 1000)

        -- Chỉ là số: 80, 120, 200.5
        WHEN daban ~ '^[0-9,\.]+$' THEN
            REPLACE(daban, ',', '.')::FLOAT

        ELSE 0
    END,
0)::FLOAT AS daban,

  loai,

  -- Ngày
  TO_DATE(ngaycapnhat,'YYYY-MM-DD') AS ngaycapnhat

FROM sanpham_raw;

