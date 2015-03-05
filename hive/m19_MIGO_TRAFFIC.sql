INSERT OVERWRITE TABLE pri_result.migo_traffic PARTITION(dt = '${date_ymd}')

SELECT '${date_ymd}', a.seller_id, a.cate_id
, CASE WHEN (b.login_mbr_pv IS NULL) THEN c.login_mbr_pv ELSE b.login_mbr_pv END AS login_mbr_pv
, CASE WHEN (b.non_login_mbr_pv IS NULL) THEN c.non_login_mbr_pv ELSE b.non_login_mbr_pv END AS non_login_mbr_pv
FROM (
    SELECT seller_id, cate_id
    FROM pri_result.migo_kpi_1_group_arg
    WHERE dt = '${date_ymd}'
) a
LEFT OUTER JOIN (
    SELECT '${date_ymd}', seller_id, -1 AS cate_id, SUM(login_mbr_pv) AS login_mbr_pv, SUM(non_login_mbr_pv) AS non_login_mbr_pv
    FROM sys_seller_dwb_shop_traffic_mbr_d
    WHERE dt >= TO_CHAR(DATEADD(TO_DATE('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
      AND dt <= '${date_ymd}'
    GROUP BY seller_id
) b 
ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id
LEFT OUTER JOIN ( -- ONLY ALL
    SELECT '${date_ymd}', seller_id, -1 AS cate_id, SUM(login_mbr_pv) AS login_mbr_pv, SUM(non_login_mbr_pv) AS non_login_mbr_pv
    FROM sys_seller_dwb_shop_traffic_mbr_d
    WHERE dt >= TO_CHAR(DATEADD(TO_DATE('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
      AND dt <= '${date_ymd}'
   GROUP BY seller_id
   
) c
ON a.seller_id = c.seller_id

