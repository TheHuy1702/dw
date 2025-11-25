-- transform_staging_clean.sql

DROP TABLE IF EXISTS sanpham_clean;
CREATE TABLE sanpham_clean AS
SELECT
  masp,
  url,
  tensp,
  CASE WHEN gia ~ '^[0-9.]' THEN REPLACE(REPLACE(gia,'₫',''),'.','')::FLOAT ELSE NULL END AS gia,
  CASE WHEN giagoc ~ '^[0-9.]' THEN REPLACE(REPLACE(giagoc,'₫',''),'.','')::FLOAT ELSE NULL END AS giagoc,
  khuyenmai,
  img,
  danhgia,
  daban,
  loai,
  TO_DATE(ngaycapnhat,'YYYY-MM-DD') AS ngaycapnhat
FROM sanpham_raw;

