WITH purchases
AS (
SELECT
      PARSE_DATE("%Y%m%d", date) AS date,
      MAX(TRANSACTION.transactionid) AS transactionid,
      h1.hitNumber AS purchaseHit,
      h1.time AS purchaseHitStartTime,  
      clientId,
    REGEXP_EXTRACT(REGEXP_REPLACE(h1.page.pagePath,"/amp",""),r"-id-([0-9]{5,10})$") as article_id
      h1.page.pagePath AS page,
      COUNT (*) AS orders
    FROM `project.dataset.table_*`, UNNEST (hits) AS h1
    WHERE h1.eCommerceAction.action_type = '6'
    AND REGEXP_CONTAINS(h1.page.pagePath, r".*-id-[0-9].*")
    AND transaction.transactionid IS NOT NULL

      AND _table_suffix = FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    GROUP BY date ,article_id,page,purchaseHit,purchaseHitStartTime,clientId
    ),
    
    
   last_hits AS (
    SELECT
  pagePath,
  clientId,
  hitNumber,
  hitTime,
  second_pagePath,
  third_pagePath,
   REGEXP_EXTRACT(REGEXP_REPLACE(pagePath,"/amp",""),r"-id-([0-9]{5,10})$") as first_article_id,
   REGEXP_EXTRACT(REGEXP_REPLACE(second_pagePath,"/amp",""),r"-id-([0-9]{5,10})$") as second_article_id,
   REGEXP_EXTRACT(REGEXP_REPLACE(third_pagePath,"/amp",""),r"-id-([0-9]{5,10})$") as third_article_id,
  upcomingtransactionid,
  COUNT(*) AS count
FROM (

  SELECT
        clientId,
        h1.hitNumber AS hitNumber,
        h1.time AS hitTime,
        h1.page.pagePath AS pagePath,
        LEAD (h1.page.pagePath, 1) OVER (PARTITION BY clientId ORDER BY h1.time) AS second_pagePath,
        LEAD (h1.page.pagePath, 2) OVER (PARTITION BY clientId ORDER BY h1.time) AS third_pagePath,
        LAST_VALUE((SELECT MAX(transaction.transactionid) 
              FROM UNNEST(hits) AS h1 WHERE transaction.transactionid IS NOT NULL) IGNORE NULLS) OVER (PARTITION BY fullvisitorid ORDER BY h1.time                  DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS upcomingtransactionid,
    FROM `project.dataset.table_*`, UNNEST (hits) AS h1
      WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(),         INTERVAL 1 DAY))
      AND  h1.type = "PAGE"
 ORDER BY
      clientId,
      hitTime)
 
WHERE
  third_pagePath LIKE "%-id-%"
  AND second_pagePath LIKE "%-id-%"
  AND pagePath LIKE "%-id-%"

GROUP BY
  pagePath,
  second_pagePath,
  third_pagePath,
    first_article_id,
 second_article_id,
 third_article_id,
  clientId,
  upcomingtransactionid,
  hitTime,
  hitNumber
  HAVING (third_article_id != first_article_id OR third_article_id != second_article_id)

ORDER BY
  count DESC
 ),
    
    
  list AS (  
SELECT
    date,
  first_article_id,
 second_article_id,
 third_article_id,
CASE WHEN purchaseHit = hitNumber THEN "last click" ELSE "assisted click" END AS conversionType,
COUNT (DISTINCT transactionid) AS Transactions
FROM 
    (
    SELECT *
    FROM `purchases`
     ) AS a
    LEFT JOIN 
    (SELECT *
    FROM `last_hits`) AS b
    
    USING (clientId)
  WHERE
    upcomingtransactionid = transactionid
    AND purchaseHit >= hitNumber
    AND page = third_pagePath
    AND purchaseHitStartTime - hitTime <= (30*24*60*60)
    GROUP BY date,page,pagePath,second_pagePath,third_pagePath,first_article_id, second_article_id, third_article_id,conversionType
    HAVING conversionType = 'assisted click'
)

SELECT 
date,
CASE 
WHEN third_article_id != first_article_id THEN first_article_id
WHEN third_article_id != second_article_id THEN second_article_id
END AS assisted_id,
SUM(Transactions) AS assisted_transactions


FROM `list`
GROUP BY date,assisted_id
ORDER BY assisted_transactions
    
