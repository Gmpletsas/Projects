# Exploratory Data Analysis 

SELECT *
FROM layoffs_staging2;

	-- We will work with column total_laid_off and percentage_laid_off
    
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;					-- In these 3 years all these layoffs have been maid --

SELECT Industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry						-- Retail and consumer being top 2 makes sense since these layoffs take place during COVID --
ORDER BY 2 DESC;

SELECT Country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY Country						-- USA by had by far the most layoffs 						
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1											
ORDER BY 2 DESC;

SELECT substring(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) 
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY 1								
ORDER BY 1 ASC;							-- We want a rolling sum of this

WITH Rolling_Total AS
(
SELECT substring(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS Total_off
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY 1								
ORDER BY 1 ASC
)
SELECT `MONTH`, Total_off, SUM(Total_off) OVER(ORDER BY `MONTH`)AS Rolling_Sum
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1, 2									
ORDER BY 1 ASC;

-- We can use this to rank which year the let off the most people
-- We will do that using CTEs

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1, 2						
ORDER BY 3 DESC;

WITH Company_year (company, years, total_laid_off) AS
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1, 2						
ORDER BY 3 DESC
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;

-- We will filter the ranking to get the top 5 for each year

WITH Company_year (company, years, total_laid_off) AS
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1, 2						
ORDER BY 3 DESC
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Rankingkalzion
FROM Company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
