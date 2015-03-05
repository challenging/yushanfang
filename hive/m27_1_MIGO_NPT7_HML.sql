INSERT OVERWRITE TABLE  pri_result.migo_dmp_tag_npt7_hml  PARTITION (dt='${date_ymd}')
--NPT7&H:2000,NPT7&M:2001,NPT7&L:2002

SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM (
    SELECT  '${date_ymd}' AS thedate, 
            a.seller_id, 
            a.buyer_id, 
            CASE WHEN ((a.hazard_ranking / b.MHR) <= 0.25) THEN "2000"
                 WHEN ((a.hazard_ranking / b.MHR) <= 0.50) THEN "2001"
                 ELSE "2002" END AS tagging_id
    FROM    pri_result.migo_npt_ranking_730 a
    JOIN    (
                SELECT  seller_id, 
                        MAX(hazard_ranking) AS MHR
                FROM    pri_result.migo_npt_ranking_730
                WHERE   dt = '${date_ymd}' AND tagging_npt = "NPT7" AND cate_id = -1
                GROUP   BY seller_id
    ) b ON a.seller_id = b.seller_id  
    WHERE   a.tagging_npt = "NPT7" AND a.cate_id = -1 AND a.dt = '${date_ymd}'
) z;
