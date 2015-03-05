INSERT OVERWRITE TABLE  pri_result.migo_kpi_for_mysql PARTITION (dt="${date_ymd}")

SELECT dt 
    ,seller_id 
    ,cate_id
    ,CASE WHEN cate_name IS NULL THEN "" ELSE cate_name END
    ,CASE WHEN revenue_num IS NULL THEN 0 ELSE revenue_num END
    ,CASE WHEN revenue_first_num IS NULL THEN 0 ELSE revenue_first_num END
    ,CASE WHEN member_num IS NULL THEN 0 ELSE member_num END
    ,CASE WHEN member_new_num IS NULL THEN 0 ELSE member_new_num END
    ,CASE WHEN member_sleep_num IS NULL THEN 0 ELSE member_sleep_num END
    ,CASE WHEN member_s3_num IS NULL THEN 0 ELSE member_s3_num END
    ,CASE WHEN member_new_ratio IS NULL THEN 0 ELSE member_new_ratio END
    ,CASE WHEN member_sleep_ratio IS NULL THEN 0 ELSE member_sleep_ratio END
    ,CASE WHEN active_ratio IS NULL THEN 0 ELSE active_ratio END
    ,CASE WHEN active_old_ratio IS NULL THEN 0 ELSE active_old_ratio END
    ,CASE WHEN active_old_num IS NULL THEN 0 ELSE active_old_num END
    ,CASE WHEN stickiness_ratio IS NULL THEN 0 ELSE stickiness_ratio END
    ,CASE WHEN stickiness_new_ratio IS NULL THEN 0 ELSE stickiness_new_ratio END
    ,CASE WHEN stickiness_old_ratio IS NULL THEN 0 ELSE stickiness_old_ratio END
    ,CASE WHEN arpu_num IS NULL THEN 0 ELSE arpu_num END
    ,CASE WHEN arpu_new_num IS NULL THEN 0 ELSE arpu_new_num END
    ,CASE WHEN arpu_old_num IS NULL THEN 0 ELSE arpu_old_num END
    ,CASE WHEN visitor_num IS NULL THEN 0 ELSE visitor_num END
    ,CASE WHEN visitor_old_num IS NULL THEN 0 ELSE visitor_old_num END
    ,CASE WHEN visitor_new_num IS NULL THEN 0 ELSE visitor_new_num END
    ,CASE WHEN conversion_ratio IS NULL THEN 0 ELSE conversion_ratio END
    ,CASE WHEN conversion_old_ratio IS NULL THEN 0 ELSE conversion_old_ratio END
    ,CASE WHEN conversion_new_ratio IS NULL THEN 0 ELSE conversion_new_ratio END
FROM pri_result.migo_kpi
WHERE dt = '${date_ymd}'
