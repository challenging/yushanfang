INSERT OVERWRITE TABLE  pri_result.migo_NES_gap_mean_v2  PARTITION (dt='${date_ymd}')

---continue from m3, create new group mean for active&stickness ---'${date_ymd}'
---group by seller_id, cate_id

---n pri_temp.migo_NES_gap_mean_v2 pri_result.migo_kpi_1_adj_gap_mean 

SELECT  c.seller_id,
        c.buyer_id,
        c.cate_id,
        c.rep_count,
        c.weight1,
        c.weight2,
        c.adj_gap_mean,
        c.k,
        c.alpha,
        c.theta,        
        b.MD_adj_gap_mean AS g_gap_mean
FROM
(
    SELECT  seller_id,
            buyer_id,
            cate_id,
            rep_count,
            weight1,
            weight2,
            adj_gap_mean,
            k,
            alpha,
            theta
    FROM    pri_result.migo_kpi_1_adj_gap_mean 
    WHERE   dt = '${date_ymd}' --AND seller_id='547623355'  
)c
LEFT OUTER JOIN
(
    SELECT  a.seller_id,
            a.cate_id,
            MEDIAN(a.adj_gap_mean) AS MD_adj_gap_mean
    FROM
    (
        SELECT  seller_id, 
                buyer_id, 
                cate_id, 
                adj_gap_mean 
        FROM    pri_result.migo_kpi_1_adj_gap_mean 
        WHERE   dt = '${date_ymd}'
    --'${date_ymd}'
    )a
    --WHERE a.seller_id='547623355'  
    GROUP BY a.seller_id, a.cate_id
)b
ON  c.seller_id = b.seller_id AND c.cate_id = b.cate_id


