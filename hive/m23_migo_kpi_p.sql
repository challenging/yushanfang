INSERT OVERWRITE TABLE  pri_result.migo_kpi_p_for_mysql PARTITION (dt="${date_ymd}")

SELECT thedate
    ,seller_id
    ,cate_id
    ,cate_name 
    ,-999 
    ,-999
    ,member_num_pct
    ,member_new_num_pct 
    ,member_sleep_num_pct 
    ,member_s3_num_pct
    ,member_new_ratio_pct 
    ,member_sleep_ratio_pct 
    ,active_ratio_pct
    ,active_old_ratio_pct 
    ,active_old_num_pct 
    ,stickiness_ratio_pct 
    ,stickiness_new_ratio_pct 
    ,stickiness_old_ratio_pct 
    ,arpu_num_pct 
    ,arpu_new_num_pct 
    ,arpu_old_num_pct 
    ,pv_pct 
    ,login_mbr_pv_pct
    ,non_login_mbr_pv_pct 
    ,conversion_ratio_pct 
    ,conversion_old_ratio_pct 
    ,conversion_new_ratio_pct 
FROM (
    SELECT thedate
        ,seller_id
        ,cate_id
        ,cate_name 
        ,-999 
        ,-999
        ,member_num_pct
        ,member_new_num_pct 
        ,member_sleep_num_pct 
        ,member_s3_num_pct
        ,member_new_ratio_pct 
        ,member_sleep_ratio_pct 
        ,active_ratio_pct
        ,active_old_ratio_pct 
        ,active_old_num_pct 
        ,stickiness_ratio_pct 
        ,stickiness_new_ratio_pct 
        ,stickiness_old_ratio_pct 
        ,arpu_num_pct 
        ,arpu_new_num_pct 
        ,arpu_old_num_pct 
        ,pv_pct 
        ,login_mbr_pv_pct
        ,non_login_mbr_pv_pct 
        ,conversion_ratio_pct 
        ,conversion_old_ratio_pct 
        ,conversion_new_ratio_pct 
    FROM sys_seller_rpt_shop_trd_cate_comp_30d
    WHERE dt = '${date_ymd}'
    
    UNION ALL
    
    SELECT a.thedate
        ,a.seller_id
        ,-1 AS cate_id
        ,"無分類" AS cate_name
        ,-999
        ,-999
        , CAST(ROUND(SUM(a.p1), 0) AS BIGINT) AS member_num_pct
        , CAST(ROUND(SUM(a.p2), 0) AS BIGINT) AS member_new_num_pct
        , CAST(ROUND(SUM(a.p3), 0) AS BIGINT) AS member_sleep_num_pct
        , CAST(ROUND(SUM(a.p4), 0) AS BIGINT) AS member_s3_num_pct
        , CAST(ROUND(SUM(a.p5), 0) AS BIGINT) AS member_new_ratio_pct
        , CAST(ROUND(SUM(a.p6), 0) AS BIGINT) AS member_sleep_ratio_pct
        , CAST(ROUND(SUM(a.p7), 0) AS BIGINT) AS active_ratio_pct
        , CAST(ROUND(SUM(a.p8), 0) AS BIGINT) AS active_old_ratio_pct
        , CAST(ROUND(SUM(a.p9), 0) AS BIGINT) AS active_old_num_pct
        , CAST(ROUND(SUM(a.p10), 0) AS BIGINT) AS stickiness_ratio_pct
        , CAST(ROUND(SUM(a.p11), 0) AS BIGINT) AS stickiness_new_ratio_pct
        , CAST(ROUND(SUM(a.p12), 0) AS BIGINT) AS stickiness_old_ratio_pct
        , CAST(ROUND(SUM(a.p13), 0) AS BIGINT) AS arpu_num_pct
        , CAST(ROUND(SUM(a.p14), 0) AS BIGINT) AS arpu_new_num_pct
        , CAST(ROUND(SUM(a.p15), 0) AS BIGINT) AS arpu_old_num_pct
        , CAST(ROUND(SUM(a.p16), 0) AS BIGINT) AS pv_pct
        , CAST(ROUND(SUM(a.p17), 0) AS BIGINT) AS login_mbr_pv_pct
        , CAST(ROUND(SUM(a.p18), 0) AS BIGINT) AS non_login_mbr_pv_pct
        , CAST(ROUND(SUM(a.p19), 0) AS BIGINT) AS conversion_ratio_pct
        , CAST(ROUND(SUM(a.p20), 0) AS BIGINT) AS conversion_old_ratio_pct
        , CAST(ROUND(SUM(a.p21), 0) AS BIGINT) AS conversion_new_ratio_pct
    FROM (
        SELECT a.thedate
            ,a.seller_id
            ,a.cate_id
            ,a.cate_name 
            ,-999 
            ,-999
            ,member_num_pct * b.ratio AS p1
            ,member_new_num_pct * b.ratio AS p2
            ,member_sleep_num_pct * b.ratio AS p3
            ,member_s3_num_pct * b.ratio AS p4
            ,member_new_ratio_pct * b.ratio AS p5
            ,member_sleep_ratio_pct * b.ratio AS p6
            ,active_ratio_pct * b.ratio AS p7
            ,active_old_ratio_pct * b.ratio AS p8
            ,active_old_num_pct * b.ratio AS p9
            ,stickiness_ratio_pct * b.ratio AS p10
            ,stickiness_new_ratio_pct * b.ratio AS p11
            ,stickiness_old_ratio_pct * b.ratio AS p12
            ,arpu_num_pct * b.ratio AS p13
            ,arpu_new_num_pct * b.ratio AS p14
            ,arpu_old_num_pct * b.ratio AS p15
            ,pv_pct * b.ratio AS p16
            ,login_mbr_pv_pct * b.ratio AS p17
            ,non_login_mbr_pv_pct * b.ratio AS p18
            ,conversion_ratio_pct * b.ratio AS p19
            ,conversion_old_ratio_pct * b.ratio AS p20
            ,conversion_new_ratio_pct * b.ratio AS p21
        FROM sys_seller_rpt_shop_trd_cate_comp_30d a JOIN (
            SELECT a.seller_id, a.cate_id, a.revenue_num / b.revenue_total as ratio
            FROM (
                SELECT seller_id, cate_id, revenue_num
                FROM pri_result.migo_kpi
                WHERE dt = '${date_ymd}' AND cate_id != -1
            ) a JOIN (
                SELECT seller_id, SUM(revenue_num) as revenue_total
                FROM pri_result.migo_kpi
                WHERE dt = '${date_ymd}' AND cate_id != -1
                GROUP BY seller_id
            ) b ON a.seller_id = b.seller_id
        ) b ON a.dt = '${date_ymd}' AND a.seller_id = b.seller_id AND a.cate_id = b.cate_id 
    ) a
    GROUP BY a.thedate, a.seller_id
) a;
