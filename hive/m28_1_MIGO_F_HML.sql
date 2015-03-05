INSERT OVERWRITE TABLE pri_result.migo_dmp_tag_f_hml PARTITION (dt='${date_ymd}')
---FH:2400,FM:2401,FL:2402
SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM
(
    SELECT  '${date_ymd}' AS thedate, 
            seller_id, 
            buyer_id, 
            CASE WHEN F = "FH" THEN 2400
                 WHEN F = "FM" THEN 2401
                 ELSE 2402 END AS tagging_id
    FROM    pri_result.migo_lrfm_results
    WHERE   dt = '${date_ymd}' AND cate_id = -1
)z
