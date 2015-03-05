INSERT OVERWRITE TABLE pri_result.migo_cf_source  PARTITION (dt='${date_ymd}') 

SELECT d.seller_id, d.platform_buyer_id, t.tagging_id, auction_id, SUM(gmv_auction_num) as count 
FROM sys_seller_dwb_shop_order_d d JOIN pri_result.migo_tagging t
ON d.seller_id = t.seller_id AND d.platform_buyer_id = t.buyer_id
WHERE d.dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -180, 'dd'), 'yyyymmdd') AND d.dt <= '${date_ymd}'
  AND t.dt = '${date_ymd}'
GROUP BY d.seller_id, d.platform_buyer_id, t.tagging_id, auction_id;
