INSERT OVERWRITE TABLE pri_result.migo_kpi_1_group_arg PARTITION (dt="${date_ymd}")

SELECT a.seller_id, a.cate_id, a.mean, a.var
, CASE WHEN a.var=0 THEN -999 ELSE a.mean*a.mean/a.var + 2 END AS alpha
, CASE WHEN a.var=0 THEN -999 ELSE a.var/(a.mean*(a.mean*a.mean + a.var)) END AS theta
, CASE WHEN gap_var=0 THEN -999 ELSE a.gap_mean*a.gap_mean/gap_var END AS k
FROM (
    SELECT a.seller_id, a.cate_id
    , CASE WHEN b.mean IS NULL THEN 0 ELSE b.mean END as mean
    , CASE WHEN b.var IS NULL THEN 0 ELSE b.var END as var
    , CASE WHEN c.gap_mean IS NULL THEN 0 ELSE c.gap_mean END as gap_mean
    , CASE WHEN c.gap_var IS NULL THEN 0 ELSE c.gap_var END as gap_var
    FROM (SELECT seller_id, cate_id
          FROM pri_result.migo_kpi_group_pi 
          WHERE dt = '${date_ymd}'
          GROUP BY seller_id, cate_id
    ) a LEFT OUTER JOIN (
        SELECT seller_id, cate_id,
                AVG(datediff(max_transaction_date,min_transaction_date, "dd")/(purchasing_count-1)) AS mean,
                POW(STDDEV(datediff(max_transaction_date,min_transaction_date, "dd")/(purchasing_count-1)), 2) AS var
        FROM pri_result.migo_kpi_group_pi
        WHERE dt = '${date_ymd}' AND purchasing_count > 1
        GROUP BY seller_id, cate_id
    ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id LEFT OUTER JOIN (
        SELECT seller_id, cate_id
            ,AVG(purchasing_interval) AS gap_mean
            ,POW(STDDEV(purchasing_interval), 2) AS gap_var
        FROM pri_result.migo_kpi_1_individual_pi
        WHERE dt = '${date_ymd}'
            AND purchasing_interval IS NOT null
        GROUP BY seller_id, cate_id
    ) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id
) a;
