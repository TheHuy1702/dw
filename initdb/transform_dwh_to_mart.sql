DROP TABLE IF EXISTS vw_top_discount;
CREATE TABLE vw_top_discount AS
SELECT
  s.tensp,
  f.gia,
  f.giagoc,
  (f.giagoc - f.gia) AS giamgia,
  ROUND((f.giagoc - f.gia) / f.giagoc * 100, 2) AS phantram,
  f.ngaycapnhat
FROM fact_gia f
JOIN dim_sanpham s ON f.masp = s.masp
WHERE f.giagoc > f.gia;

