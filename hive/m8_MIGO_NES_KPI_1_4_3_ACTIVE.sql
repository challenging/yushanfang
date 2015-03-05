INSERT OVERWRITE TABLE pri_result.migo_nes_active_results PARTITION (dt='${date_ymd}')

SELECT a.seller_id, a.cate_id
, CASE WHEN active_total_count=0 THEN -999
       ELSE active_count/active_total_count END AS active_radio                   -- N + E
, CASE WHEN active_total_count_E=0 THEN -999
       ELSE active_count_E/active_total_count_E END AS active_radio_E             -- E
, CASE WHEN stickiness_total_count=0 THEN -999
       ELSE stickiness_count/stickiness_total_count END AS stickiness_radio       -- N + E
, CASE WHEN stickiness_total_count_N=0 THEN -999
       ELSE stickiness_count_N/stickiness_total_count_N END AS stickiness_radio_N -- N
, CASE WHEN stickiness_total_count_E=0 THEN -999
       ELSE stickiness_count_E/stickiness_total_count_E END AS stickiness_radio_E -- E
FROM (
    SELECT DISTINCT seller_id, cate_id
    FROM pri_result.migo_kpi_1_individual_pi
    WHERE dt = '${date_ymd}'
) a LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_active) AS active_count
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging in ('N0','EB','E0') --N + E
    GROUP BY seller_id, cate_id
) aa ON a.seller_id = aa.seller_id AND a.cate_id = aa.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_total) AS active_total_count
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging in ('N0','EB','E0') --N + E
    GROUP BY seller_id, cate_id
) b ON a.seller_id = b.seller_id AND a.cate_id = b.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_active) AS active_count_E
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging = 'E0' --E
    GROUP BY seller_id, cate_id
) c ON a.seller_id = c.seller_id AND a.cate_id = c.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_total) AS active_total_count_E
    FROM pri_result.migo_active
    WHERE dt = '${date_ymd}' AND tagging = 'E0' --E
    GROUP BY seller_id, cate_id
) d ON a.seller_id = d.seller_id AND a.cate_id = d.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM(count_stickiness_30) AS stickiness_count
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}' AND tagging IN ("EB2", "3TIMES")
    GROUP BY seller_id, cate_id
) e ON a.seller_id = e.seller_id AND a.cate_id = e.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM( count_total_30 ) AS stickiness_total_count
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}'
    GROUP BY seller_id, cate_id
) f ON a.seller_id = f.seller_id AND a.cate_id = f.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM( count_stickiness_30 ) AS stickiness_count_N
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}' AND tagging = "EB2" -- N
    GROUP BY seller_id, cate_id
) g ON a.seller_id = g.seller_id AND a.cate_id = g.cate_id LEFT OUTER JOIN (
      SELECT seller_id, cate_id, SUM( count_total_30) AS stickiness_total_count_N
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}' AND tagging = "EB2"
    GROUP BY seller_id, cate_id
) h ON a.seller_id = h.seller_id AND a.cate_id = h.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM( count_stickiness_30 ) AS stickiness_count_E
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}' AND tagging = '3TIMES'
    GROUP BY seller_id, cate_id
) i ON a.seller_id = i.seller_id AND a.cate_id = i.cate_id LEFT OUTER JOIN (
    SELECT seller_id, cate_id, SUM( count_total_30 ) AS stickiness_total_count_E
    FROM pri_result.migo_stickiness
    WHERE dt = '${date_ymd}' AND tagging = '3TIMES'
    GROUP BY seller_id, cate_id
) j ON a.seller_id = j.seller_id AND a.cate_id = j.cate_id;
