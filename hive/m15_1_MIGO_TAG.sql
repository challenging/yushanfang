INSERT OVERWRITE TABLE pri_result.migo_tagging PARTITION(dt = '${date_ymd}')

SELECT seller_id, buyer_id, cate_id, tagging_id
FROM (
    -- N+: 1000, N0 + EB
    SELECT seller_id, buyer_id, cate_id, 1000 AS tagging_id
    FROM pri_result.migo_tagging_nes
    WHERE tagging IN ("N0", "EB") AND dt = '${date_ymd}'

    UNION ALL

    -- N+: 1001, E0 + MH
    SELECT a.seller_id, a.buyer_id, a.cate_id, 1001 AS tagging_id
    FROM pri_result.migo_tagging_nes a JOIN (
        SELECT seller_id, buyer_id, cate_id
        FROM pri_result.migo_lrfm_results
        WHERE M = "MH" AND dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id
    WHERE a.tagging = "E0" AND a.dt = '${date_ymd}'

    UNION ALL

    -- S-: 1100, S1
    SELECT seller_id, buyer_id, cate_id, 1100 AS tagging_id
    FROM pri_result.migo_tagging_nes
    WHERE tagging IN ("S1") AND dt = '${date_ymd}'

    UNION ALL

    -- S-: 1101, S2 + (MH|MM)
    SELECT a.seller_id, a.buyer_id, a.cate_id, 1101 AS tagging_id
    FROM pri_result.migo_tagging_nes a JOIN (
        SELECT seller_id, buyer_id, cate_id
        FROM pri_result.migo_lrfm_results
        WHERE M IN ("MH", "MM") AND dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id
    WHERE a.tagging = "S2" AND a.dt = '${date_ymd}'

    UNION ALL

    -- S-: 1102, S3 + MH
    SELECT a.seller_id, a.buyer_id, a.cate_id, 1102 AS tagging_id
    FROM pri_result.migo_tagging_nes a JOIN (
        SELECT seller_id, buyer_id, cate_id
        FROM pri_result.migo_lrfm_results
        WHERE M IN ("MH") AND dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id
    WHERE a.tagging = "S2" AND a.dt = '${date_ymd}'

    UNION ALL

    -- Active: 1200, NPT7 with H Tag
    SELECT seller_id, buyer_id, cate_id, 1200 AS tagging_id
    FROM (
        SELECT a.seller_id, a.buyer_id, a.cate_id, a.hazard_ranking, a.hazard_rate,
            CASE WHEN ((a.hazard_ranking / b.MHR) <= 0.25) THEN "NPT7H"
                WHEN ((a.hazard_ranking / b.MHR) <= 0.50) THEN "NPT7M"
                ELSE "NPT7L" END AS tagging_id
        FROM pri_result.migo_npt_ranking_730 a JOIN (
            SELECT seller_id, cate_id, MAX(hazard_ranking) AS MHR
            FROM pri_result.migo_npt_ranking_730
            WHERE dt = '${date_ymd}' AND tagging_npt = "NPT7"
            GROUP BY seller_id, cate_id
        ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id
        WHERE a.tagging_npt = "NPT7" AND a.dt = '${date_ymd}'
    ) a
    WHERE tagging_id = "NPT7H"

    UNION ALL

    -- Active: 1201, NPT30 with H Tag
    SELECT seller_id, buyer_id, cate_id, 1201 AS tagging_id
    FROM (
        SELECT a.seller_id, a.buyer_id, a.cate_id, a.hazard_ranking, a.hazard_rate,
            CASE WHEN ((a.hazard_ranking / b.MHR) <= 0.25) THEN "NPT30H"
                WHEN ((a.hazard_ranking / b.MHR) <= 0.50) THEN "NPT30M"
                ELSE "NPT30L" END AS tagging_id
        FROM pri_result.migo_npt_ranking_730 a JOIN (
            SELECT seller_id, cate_id, MAX(hazard_ranking) AS MHR
            FROM pri_result.migo_npt_ranking_730
            WHERE dt = '${date_ymd}' AND tagging_npt = "NPT30"
            GROUP BY seller_id, cate_id
        ) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id
        WHERE a.tagging_npt = "NPT30" AND a.dt = '${date_ymd}'
    ) a
    WHERE tagging_id = "NPT30H"

    UNION ALL

    -- Stickiness: 1300, S0
    SELECT seller_id, buyer_id, cate_id, 1300 AS tagging_id
    FROM pri_result.migo_tagging_s0
    WHERE dt = '${date_ymd}'

    UNION ALL

    -- ARPU: 1400 N0 + MH
    SELECT a.seller_id, a.buyer_id, a.cate_id, 1400 AS tagging_id
    FROM pri_result.migo_tagging_nes a JOIN (
        SELECT seller_id, buyer_id, cate_id
        FROM pri_result.migo_lrfm_results
        WHERE M = "MH" AND dt = '${date_ymd}'
    ) b ON a.seller_id = b.seller_id AND a.buyer_id = b.buyer_id AND a.cate_id = b.cate_id
    WHERE a.tagging = "N0" AND a.dt = '${date_ymd}'

    UNION ALL

    -- ARPU: 1401, E0
    SELECT seller_id, buyer_id, cate_id, 1401 AS tagging_id
    FROM pri_result.migo_tagging_nes
    WHERE tagging = "E0" AND dt = '${date_ymd}'
) a
