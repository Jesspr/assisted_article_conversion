# Google Analytics Data Analysis of Assisted Article Conversion

This project is an analysis of Google Analytics data with the purpose of being useful for a publishing house, performed using SQL in Google BigQuery. The goal of the analysis is to identify which pages users visited before making a purchase and how these pages contributed to the purchase, i.e., whether they were the last click before the purchase or an assisted click.

## Data Source
The data is sourced from Google Analytics Universal Analytics data for the previous day. Specifically, the transaction ID is used to see which pages the user has visited before, including the last 30 days and the last three pages. The pages that were not articles (e.g., homepage) are then excluded.

## Analysis Steps
The analysis consists of the following steps:

    *Extracting Purchase Data:* Extract data for all purchases made in the previous day, including the transaction ID, the page where the purchase was made, and the ID of the article that was purchased.

    *Identifying Last Three Page Visits:* For each user who made a purchase, identify the last three pages they visited before the purchase, excluding pages that were not articles. This is done using the LEAD function to identify the second and third pages, and then filtering to only include visits where all three pages were articles.

    *Matching Page Visits to Purchases:* Match each purchase to the user's last three page visits, and identify whether the purchase was made on the last click or as an assisted click. An assisted click is defined as a click on a page that the user visited before the last click, within the last 30 days. This is done by joining the purchase data with the page visit data and comparing the hit numbers and timestamps.

    *Aggregating Results:* Aggregate the results by date and assisted page ID, and sum the number of assisted transactions. If a page was visited multiple times before a purchase, it will appear as an assisted click for each of those visits.

## Results
The results of the analysis are presented in a table that shows, for each date, the ID of the assisted page and the number of assisted transactions for that page. The table is sorted by the number of assisted transactions, with the most important assisted pages at the top. The results can be used to optimize the publishing house's website and marketing strategy, by identifying which pages are most effective in driving purchases and which pages could be improved to increase their impact.

## Conclusion
This project demonstrates how SQL can be used to extract valuable insights from Google Analytics data. The code is provided as an open-source project on GitHub, to allow others to replicate the analysis or use it as a starting point for their own data analysis projects.