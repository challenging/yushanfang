INSERT OVERWRITE TABLE pri_result.migo_dmp_tag_r_hml PARTITION (dt='${date_ymd}')
---RH:2300,RM:2301,RL:2302
SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM
(
    SELECT  '${date_ymd}' AS thedate, 
            seller_id, 
            buyer_id, 
            CASE WHEN R = "RH" THEN 2300
                 WHEN R = "RM" THEN 2301
                 ELSE 2302 END AS tagging_id
    FROM    pri_result.migo_lrfm_results
    WHERE   dt = '${date_ymd}' AND cate_id = -1
)z
