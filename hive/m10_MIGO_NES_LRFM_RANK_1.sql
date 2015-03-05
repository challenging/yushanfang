INSERT OVERWRITE TABLE pri_result.migo_lrfm_1 PARTITION(dt ='${date_ymd}')

SELECT seller_id, buyer_id, cate_id,
RANK() OVER (PARTITION BY seller_id ORDER BY datediff(max_transaction_date, min_transaction_date, "dd")  DESC) AS L,
RANK() OVER (PARTITION BY seller_id ORDER BY datediff(to_date('${date_ymd}', "YYYYMMDD"), max_transaction_date, "dd") ASC) AS R,
RANK() OVER (PARTITION BY seller_id ORDER BY purchasing_count DESC) AS F,
RANK() OVER (PARTITION BY seller_id ORDER BY transaction_amount DESC) AS M
FROM pri_result.migo_kpi_group_pi 
WHERE dt = '${date_ymd}';
