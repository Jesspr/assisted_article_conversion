-- Define a CTE called 'purchases' that contains information about ecommerce purchases.
WITH purchases
AS (
  SELECT
    PARSE_DATE("%Y%m%d", date) AS date, -- Convert the date string to a date type.
    MAX(TRANSACTION.transactionid) AS transactionid, -- Get the maximum transaction ID for the hit
    h1.hitNumber AS purchaseHit,
    h1.time AS purchaseHitStartTime,  
    clientId,
    -- Extract the article ID from the page path using a regular expression.
    REGEXP_EXTRACT(REGEXP_REPLACE(h1.page.pagePath,"/amp",""),r"-id-([0-9]{5,10})$") as article_id
    h1.page.pagePath AS page,
    COUNT (*) AS orders
  FROM `project.dataset.table_*`, UNNEST (hits) AS h1
  WHERE h1.eCommerceAction.action_type = '6' -- Filters only ecommerce purchase 
    AND REGEXP_CONTAINS(h1.page.pagePath, r".*-id-[0-9].*") -- Filters pages that contain article IDs
    AND transaction.transactionid IS NOT NULL -- Filters transactions that have a transaction ID
    AND _table_suffix = FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) -- Only include data from yesterday's tables
  GROUP BY date, article_id, page, purchaseHit, purchaseHitStartTime, clientId
),
    
-- Define a CTE called 'last_hits' that contains information about the last three pages viewed before a purchase.    
last_hits AS (
  SELECT
    pagePath,
    clientId,
    hitNumber,
    hitTime,
    second_pagePath,
    third_pagePath,
    -- Extract the article ID from the page path using a regular expression.
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
      LAST_VALUE(
        (
          SELECT MAX(transaction.transactionid) 
          FROM UNNEST(hits) AS h1 
          WHERE transaction.transactionid IS NOT NULL
        ) IGNORE NULLS
      ) OVER (PARTITION BY fullvisitorid ORDER BY h1.time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS upcomingtransactionid,
    FROM `project.dataset.table_*`, UNNEST (hits) AS h1
      WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) -- Only include data from the past 30 days (up to yesterday's data)
        AND h1.type = "PAGE" -- Filters only pageview events
      ORDER BY clientId, hitTime
  )
 
WHERE third_pagePath LIKE "%-id-%" -- Filters only pages that contain an article ID
  AND second_pagePath LIKE "%-id-%"
  AND pagePath LIKE "%-id-%"

GROUP BY pagePath, second_pagePath, third_pagePath, first_article_id, second_article_id, third_article_id, clientId, upcomingtransactionid, hitTime, hitNumber
  HAVING (third_article_id != first_article_id OR third_article_id != second_article_id)

ORDER BY
  count DESC
 ),
    
  -- Define the CTE named "list". This subquery retrieves data from two tables "purchases" and "last_hits" and joins them based on the "clientId" column.    
list AS (  
  SELECT
    date,
    first_article_id,
    second_article_id,
    third_article_id,
    CASE WHEN purchaseHit = hitNumber THEN "last click" ELSE "assisted click" END AS conversionType, -- identifies the type of conversion: last click or assisted click
    COUNT (DISTINCT transactionid) AS Transactions -- count of transactions for a specific combination of the article IDs and conversion type
  FROM 
    (
    SELECT *
    FROM `purchases`
    ) AS a  -- subquery to retrieve all data from the purchases table
    LEFT JOIN 
    (SELECT *
    FROM `last_hits`) AS b -- subquery to retrieve all data from the last_hits table
    
    USING (clientId)
    WHERE upcomingtransactionid = transactionid -- selecting only matching transactions
      AND purchaseHit >= hitNumber 
      AND page = third_pagePath
      AND purchaseHitStartTime - hitTime <= (30*24*60*60) -- filter transactions that occurred within the last 30 days
    GROUP BY date,page,pagePath,second_pagePath,third_pagePath,first_article_id, second_article_id, third_article_id,conversionType
    HAVING conversionType = 'assisted click'
)
-- Select the date, assisted article ID, and sum of transactions for the combination of date and assisted article ID from the CTE named "list".
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
    
