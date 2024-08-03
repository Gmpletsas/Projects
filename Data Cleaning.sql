# Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data 
-- 3. Null or Blank values
-- 4. Remove Any Columns (Not always)

-- Create a new table to remove columns because raw dataset should not be tampered 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Remove Duplicates: Since there is no unique row ID it will be difficult

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;			-- This will reveal the duplicates

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';	-- Remove one of the duplicates

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing Data
	-- Finding issues in the data and fixing them --
    
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry		-- There are rows that should be the same (Crypto, Crypto currency and CryptoCurrency)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location		-- No issues with data
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT Country			-- Similar data (USA, USA.)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM (country))	-- Trailing specifies what to trim --
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM (country))
WHERE country LIKE 'States%';

SELECT `Date`,						-- Date is in text format (not ideal for time series)
str_to_date(`date`, '%m/%d/%Y')		-- str_to_date changes format from text to date
FROM layoffs_staging2;	

UPDATE 	layoffs_staging2			
SET `date` = str_to_date(`date`, '%m/%d/%Y');
	
SELECT `Date`						-- Noa that it is in date format we can change the column into a date column
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2		-- Never do it on raw data. Always on staging tables
MODIFY COLUMN `Date` DATE;

SELECT *
FROM layoffs_staging2;

-- Null and Blank Values --

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;		-- When both are null these are useless rows that we will remove


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';						-- We will search info about missing values by company in order to populate them

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';				-- Industry is blank so we will populate it with 'Travel'

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON 	t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2 t1				-- Need to find diferrence between this and above query
JOIN layoffs_staging2 t2
	ON 	t1.company = t2.company			-- We need to translate this into an update statement which will populate blank alues in t1 with populated values from t2 if there are any
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1				
JOIN layoffs_staging2 t2				-- Update didn't work. Possible solution is to set blank values from t1 into NULLs first
	ON 	t1.company = t2.company
SET t1.industry = T2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 				-- Now that all values are NULLs the previous select statement will bring only rows with the industry column populated in t2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON 	t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1				
JOIN layoffs_staging2 t2				
SET t1.industry = T2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';	

-- Remove Rows And/Or Columns

SELECT *
FROM layoffs_staging2					-- These rows should be removed
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2				
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

		-- We dont nedd the row_num column anymore so we can drop it --
        
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

-- Now we have clean data that we can work with and Exploratory Data Analysis on the next session --