INSERT OVERWRITE TABLE pri_result.migo_dmp_tag_m_hml PARTITION (dt='${date_ymd}')
---MH:2500,MM:2501,ML:2502
SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM
(
    SELECT  '${date_ymd}' AS thedate, 
            seller_id, 
            buyer_id, 
            CASE WHEN M = "MH" THEN 2500
                 WHEN M = "MM" THEN 2501
                 ELSE 2502 END AS tagging_id
    FROM    pri_result.migo_lrfm_results
    WHERE   dt = '${date_ymd}' AND cate_id = -1
)z;
