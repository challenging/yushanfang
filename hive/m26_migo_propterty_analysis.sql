INSERT OVERWRITE TABLE pri_result.migo_itemproperty_info PARTITION (dt='${date_ymd}')

--WHERE dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
 --   AND dt <= '${date_ymd}'

-- calculate GDP
SELECT  o.seller_id, o.item_pro_value, o.item_pro_value_des, o.trade_amt_item,
        --p.trade_amt_item AS ori_trade_amt
        o.item_pro_amt_pct, 
        CASE WHEN p.trade_amt_item IS NOT NULL 
            THEN (o.trade_amt_item-p.trade_amt_item)/p.trade_amt_item 
            ELSE 0 END AS item_GDP_pct
FROM (
    -- data for getdate()
    SELECT  seller_id, item_pro_value, item_pro_value_des, trade_amt_item, (trade_amt_item/all_trade_amt) AS item_pro_amt_pct, 0 AS item_GDP_pct
    FROM (
        SELECT  m.seller_id, m.item_pro_value, m.item_pro_value_des, m.all_trade_amt, SUM(trade_amt_itempro) AS trade_amt_item
        FROM  (  -- mapping total amt of seller_id
            SELECT  j.seller_id, j.item_pro_value, j.item_pro_value_des, j.trade_amt_itempro, k.all_trade_amt
            FROM (
                --sum amt per item property
                SELECT  seller_id,
                        --auction_id ---
                        item_pro_value, item_pro_value_des, item_trade_amt, SUM(alipay_trade_amt) AS trade_amt_itempro
                        --alipay_trade_amt/all_trade_amt as avg_trade_amt      
                FROM ( --get mapping table already
                    SELECT  g.seller_id, h.auction_id, h.alipay_trade_amt, g.item_trade_amt, g.item_pro_value, g.item_pro_value_des
                    FROM ( --get amt by order by item
                        SELECT  seller_id, auction_id, alipay_trade_amt
                        FROM    sys_seller_dwb_shop_order_d 
                        WHERE   trade_status &2 = 2
                            AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd') AND dt <= '${date_ymd}'
                            --AND seller_id=1780272813  --auction_id=35347026251
                    )h JOIN (
                        SELECT  e.seller_id, e.auction_id, f.item_pro_value, f.item_pro_value_des,e.item_trade_amt
                        FROM ( --get item in valid orders for property mapping
                            SELECT  seller_id, auction_id, SUM(alipay_trade_amt) AS  item_trade_amt
                            FROM    sys_seller_dwb_shop_order_d
                            WHERE   trade_status &2 = 2
                                AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd') AND dt <= '${date_ymd}'  
                                --AND seller_id=1780272813 --auction_id=35347026251
                            GROUP BY seller_id , auction_id
                        )e LEFT OUTER JOIN (
                            -- Split item_property_set by semicolon into separate rows 
                            -- Get property_id and property_value_id for further mapping
                            SELECT  b.seller_id, b.auction_id, b.item_pro_id, c.property_name AS item_pro_id_des, b.item_pro_value, d.property_value_data AS item_pro_value_des
                            FROM(
                                SELECT  --a.var0, 
                                        CAST(SPLIT_PART(a.var1, '_',1) AS BIGINT) AS seller_id,
                                        CAST(SPLIT_PART(a.var1, '_',2) AS BIGINT) AS auction_id,
                                        CAST(SPLIT_PART(a.var0, ':', 1) AS BIGINT) AS item_pro_id,
                                        CAST(SPLIT_PART(a.var0, ':', 2) AS BIGINT) AS item_pro_value 
                                FROM (   
                                    SELECT      MIGO_UDTF_SPLT(z.item_property_set, z.sid_aid) AS (var0, var1)
                                    FROM (
                                        SELECT  seller_id, auction_id, item_property_set, CONCAT(seller_id, '_', auction_id) AS sid_aid
                                        FROM    sys_seller_dim_item_online_d
                                        WHERE   dt = '${date_ymd}' -- '${date_ymd}'
                                            --AND seller_id in (1780272813)
                                            AND item_property_set <> ''
                                            --AND auction_id in (35347026251)
                                    )z
                                    --WHERE --z.dt = '20140728'
                                            --z.seller_id in (1780272813)
                                        ----AND z.auction_id in (35347026251)
                                )a
                            )b  LEFT OUTER JOIN (
                                SELECT  property_id, property_name
                                FROM    sys_seller_dim_property 
                                WHERE   dt = '${date_ymd}'
                            )c  ON b.item_pro_id = c.property_id LEFT OUTER JOIN (
                                SELECT  property_value_id, property_value_data
                                FROM    sys_seller_dim_property_value
                                WHERE   dt = '${date_ymd}'
                            )d  ON b.item_pro_value = d.property_value_id
                        )f ON e.seller_id = f.seller_id AND e.auction_id = f.auction_id
                    )g ON h.seller_id = g.seller_id AND h.auction_id = g.auction_id
                )i
                GROUP BY seller_id, item_pro_value,  item_pro_value_des, item_trade_amt --auction_id, 
            )j LEFT OUTER JOIN (   -- total amt by seller_id
                SELECT  seller_id, 
                        SUM(alipay_trade_amt) AS  all_trade_amt --denominator
                FROM    sys_seller_dwb_shop_order_d
                WHERE   trade_status &2 = 2
                    AND dt >=  to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -30, 'dd'), 'yyyymmdd')
                    AND dt <= '${date_ymd}'  
                    --AND seller_id=1780272813 --auction_id=35347026251
                GROUP BY seller_id 
            )k on j.seller_id = k.seller_id
            WHERE j.item_pro_value is not null
        )m
        GROUP BY m.seller_id, m.item_pro_value, m.item_pro_value_des, m.all_trade_amt
    )n
)o LEFT OUTER JOIN (
    SELECT  seller_id,
            item_pro_value,
            item_pro_value_des,
            trade_amt_item
            --item_pro_amt_pct
    FROM pri_result.migo_itemproperty_info
    WHERE dt = to_char(dateadd(to_date('${date_ymd}','yyyymmdd'), -1, 'dd'), 'yyyymmdd') --'${date_ymd}'
)p ON o.seller_id = p.seller_id AND o.item_pro_value = p.item_pro_value
