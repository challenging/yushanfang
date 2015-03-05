INSERT OVERWRITE TABLE pri_result.seller_bcg_for_mysql PARTITION (dt='${date_ymd}')

SELECT '${date_ymd}' AS thedate, seller_id, shop_id, shop_type, category_id, category_name, category_level
, pay_ord_amt_pct, pay_ord_byr_cnt_pct, pay_ord_itm_qty_pct, pay_mord_cnt_pct
, pay_ord_amt_grad_pct, pay_ord_byr_cnt_grad_pct, pay_mord_cnt_grad_pct, pay_ord_itm_qty_grad_pct 
FROM sys_seller_rpt_shop_trd_cate_pos_cm 
WHERE dt = '${date_ymd}'


