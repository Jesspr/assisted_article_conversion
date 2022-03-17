# assisted_article_conversion

I query the Google BigQuery database from the Universal Analytics data for the number of items that helped complete an order. For this, the orders of the previous day are taken. For these orders, the transaction ID is used to see which pages the user has visited before. The last 30 days and the last three pages are included. The pages that were not articles (e.g. homepage) are then excluded. 
