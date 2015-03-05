INSERT OVERWRITE TABLE pri_result.migo_kpi_4_arpu_slope PARTITION (dt='${date_ymd}')

SELECT seller_id, 
    cate_id, 
    CASE WHEN (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) = 0 THEN -999 ELSE (COUNT(*) * SUM(x*ay) - SUM(x)*SUM(ay)) / (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) END,
    CASE WHEN (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) = 0 THEN -999 ELSE (COUNT(*) * SUM(x*afy) - SUM(x)*SUM(afy)) / (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) END,
    CASE WHEN (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) = 0 THEN -999 ELSE (COUNT(*) * SUM(x*ary) - SUM(x)*SUM(ary)) / (COUNT(*)*SUM(POW(x, 2)) - POW(SUM(x), 2)) END
FROM (
    SELECT seller_id, cate_id, dt, 
        365 - DATEDIFF(to_date('${date_ymd}','yyyymmdd'), to_date(dt, 'yyyymmdd'), 'dd') AS x, 
        arpu AS ay, arpu_first AS afy, arpu_repeated AS ary
    FROM pri_result.migo_kpi_4_arpu
    WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -365, 'dd'),'yyyymmdd') AND dt <= '${date_ymd}'
    -- ORDER BY seller_id, cate_id, dt
) a
GROUP BY seller_id, cate_id;
