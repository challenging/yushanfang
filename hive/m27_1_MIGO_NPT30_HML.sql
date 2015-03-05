INSERT OVERWRITE TABLE  pri_result.migo_dmp_tag_npt30_hml  PARTITION (dt='${date_ymd}')
--NPT30&H:2100,NPT30&M:2101,NPT30&L:2102

SELECT  thedate, 
        seller_id, 
        buyer_id, 
        tagging_id
FROM (
    SELECT  '${date_ymd}' AS thedate, 
            a.seller_id, 
            a.buyer_id, 
            CASE WHEN ((a.hazard_ranking / b.MHR) <= 0.25) THEN "2100"
                WHEN ((a.hazard_ranking / b.MHR) <= 0.50) THEN "2101"
                ELSE "2102" END AS tagging_id
    FROM    pri_result.migo_npt_ranking_730  a 
    JOIN    (
                SELECT  seller_id, 
                        MAX(hazard_ranking) AS MHR
                FROM    pri_result.migo_npt_ranking_730
                WHERE   dt = '${date_ymd}' AND tagging_npt = "NPT30" AND cate_id = -1
                GROUP   BY seller_id
    )  b 
    ON a.seller_id = b.seller_id 
    WHERE   a.tagging_npt = "NPT30" AND a.cate_id = -1 AND a.dt = '${date_ymd}'
) z

