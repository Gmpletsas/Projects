📊 Layoffs Data Analysis (SQL Project)

This repository contains two SQL projects focused on cleaning and analyzing a real-world layoffs dataset. The goal is to transform raw data into a structured format and extract meaningful insights through exploratory data analysis (EDA).

1. Data Cleaning (data_cleaning.sql)

This script prepares the raw dataset for analysis by addressing common data quality issues:

Key Steps:
Removing Duplicates
Identified duplicates using ROW_NUMBER() with PARTITION BY
Removed duplicate rows using a staging table
Standardizing Data
Trimmed whitespace from company names
Unified inconsistent values (e.g., "Crypto", "CryptoCurrency" → "Crypto")
Cleaned country names (removed trailing periods)
Converted date format from text to DATE
Handling Null & Blank Values
Identified missing values
Populated missing industry values using self-joins
Removed rows where both total_laid_off and percentage_laid_off were NULL
Removing Unnecessary Columns
Dropped helper column (row_num) after deduplication

2. Exploratory Data Analysis (eda.sql)

This script analyzes the cleaned dataset to uncover trends and insights.

Key Analyses:
Maximum layoffs
Identified highest total_laid_off and percentage_laid_off
Company-level analysis
Total layoffs per company
Ranking companies by layoffs per year using DENSE_RANK()
Time-based trends
Layoffs by year and month
Rolling cumulative layoffs over time
Industry insights
Total layoffs by industry
Geographical insights
Layoffs by country

🛠️ Tools & Techniques Used
SQL (MySQL)
Window Functions (ROW_NUMBER(), DENSE_RANK(), SUM() OVER)
Common Table Expressions (CTEs)
Data Cleaning Techniques
Aggregations & Grouping

📈 Key Insights
Certain industries (e.g., Retail, Consumer, Crypto) experienced higher layoffs
Layoffs peaked during specific years (likely influenced by global events such as COVID-19)
A small number of companies contributed disproportionately to total layoffs
The United States had the highest number of layoffs
