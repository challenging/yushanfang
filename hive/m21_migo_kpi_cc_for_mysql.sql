INSERT OVERWRITE TABLE  pri_result.migo_kpi_cc_for_mysql PARTITION (dt="${date_ymd}")

SELECT dt 
    ,seller_id
    ,cate_id
    ,cate_name 
    ,revenue_num_cc 
    ,revenue_first_num_cc
    ,member_num_cc 
    ,member_new_num_cc 
    ,member_sleep_num_cc 
    ,member_s3_num_cc 
    ,member_new_ratio_cc 
    ,member_sleep_ratio_cc 
    ,active_ratio_cc
    ,active_old_ratio_cc 
    ,active_old_num_cc 
    ,stickiness_ratio_cc 
    ,stickiness_new_ratio_cc 
    ,stickiness_old_ratio_cc 
    ,arpu_num_cc 
    ,arpu_new_num_cc 
    ,arpu_old_num_cc 
    ,visitor_num_cc 
    ,visitor_old_num_cc
    ,visitor_new_num_cc 
    ,conversion_ratio_cc 
    ,conversion_old_ratio_cc 
    ,conversion_new_ratio_cc 
FROM pri_result.migo_kpi_cc
WHERE dt = '${date_ymd}'
