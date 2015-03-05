INSERT OVERWRITE TABLE  pri_result.dmp_migo_tag PARTITION (dt='${date_ymd}') 

SELECT a.seller_id, a.buyer_id
, b1000.cate_level1_name, b1001.cate_level1_name, b1100.cate_level1_name
, b1101.cate_level1_name, b1102.cate_level1_name, b1200.cate_level1_name
, b1201.cate_level1_name, b1300.cate_level1_name, b1400.cate_level1_name
, b1401.cate_level1_name
FROM (
    SELECT seller_id, buyer_id, cate_id
    FROM pri_result.migo_tagging
    WHERE dt = '${date_ymd}'
    GROUP BY seller_id, buyer_id, cate_id
) a
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1000
    ) a JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1000
ON a.seller_id = b1000.seller_id AND a.buyer_id = b1000.buyer_id AND a.cate_id = b1000.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id, tagging_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1001
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1001
ON a.seller_id = b1001.seller_id AND a.buyer_id = b1001.buyer_id AND a.cate_id = b1001.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id,  cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1100
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1100
ON a.seller_id = b1100.seller_id AND a.buyer_id = b1100.buyer_id AND a.cate_id = b1100.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1101
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1101
ON a.seller_id = b1101.seller_id AND a.buyer_id = b1101.buyer_id AND a.cate_id = b1101.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1102
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1102
ON a.seller_id = b1102.seller_id AND a.buyer_id = b1102.buyer_id AND a.cate_id = b1102.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1200
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1200
ON a.seller_id = b1200.seller_id AND a.buyer_id = b1200.buyer_id AND a.cate_id = b1200.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1201
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1201
ON a.seller_id = b1201.seller_id AND a.buyer_id = b1201.buyer_id AND a.cate_id = b1201.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1300
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1300
ON a.seller_id = b1300.seller_id AND a.buyer_id = b1300.buyer_id AND a.cate_id = b1300.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1400
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1400
ON a.seller_id = b1400.seller_id AND a.buyer_id = b1400.buyer_id AND a.cate_id = b1400.cate_id
LEFT OUTER JOIN (
    SELECT a.seller_id, buyer_id, cate_id, cate_level1_name
    FROM (
        SELECT seller_id, cate_id, buyer_id
        FROM pri_result.migo_tagging
        WHERE dt = '${date_ymd}'
        AND tagging_id=1401
    ) a LEFT OUTER JOIN (
        SELECT DISTINCT cate_level1_id, cate_level1_name
        FROM sys_seller_dim_cate
        WHERE dt = '${date_ymd}'
    ) b
    ON a.cate_id = b.cate_level1_id
) b1401
ON a.seller_id = b1401.seller_id AND a.buyer_id = b1401.buyer_id AND a.cate_id = b1401.cate_id;
