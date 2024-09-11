SELECT *
FROM world_layoffs.layoffs;

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove Any Columns


-- REMOVE DUPLICATES

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry, total_laid_off, percentage_laid_off, 'date', stage
 ,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry, total_laid_off, percentage_laid_off, 'date', stage
 ,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging22` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging22;

INSERT INTO layoffs_staging22
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry, total_laid_off, percentage_laid_off, `date`, stage
 ,country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging22
WHERE row_num > 1;

DELETE
FROM layoffs_staging22
WHERE row_num > 1 ;

SET SQL_SAFE_UPDATES = 0;

SET SQL_SAFE_UPDATES = 1;

SELECT *
FROM layoffs_staging21;

-- STANDARDIZING DATA

SELECT company, TRIM(company)
FROM layoffs_staging22;

UPDATE layoffs_staging22
SET company = TRIM(company);

SELECT company 
FROM layoffs_staging22;

SELECT DISTINCT industry
FROM layoffs_staging22
ORDER BY 1;

SELECT *
FROM layoffs_staging22
WHERE industry LIKE 'Crypto%' ;

UPDATE layoffs_staging22
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging22
ORDER BY 1;

UPDATE layoffs_staging22 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; 

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging22;

SELECT `date`
FROM layoffs_staging22;

UPDATE layoffs_staging22
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging22
WHERE TRIM(`date`) = '' 
   OR `date` IN ('NULL', 'N/A', '-', '0000-00-00');
   
   DELETE FROM layoffs_staging22
WHERE TRIM(`date`) = '' 
   OR `date` IN ('NULL', 'N/A', '-', '0000-00-00');
   
ALTER TABLE layoffs_staging22
MODIFY COLUMN `date` DATE;

UPDATE layoffs_staging22
SET industry = NULL
WHERE industry IN ('NULL', 'N/A', '');

UPDATE layoffs_staging22
SET total_laid_off  = NULL
WHERE total_laid_off IN ('NULL', 'N/A', '');

UPDATE layoffs_staging22
SET percentage_laid_off  = NULL
WHERE percentage_laid_off IN ('NULL', 'N/A', '');


SELECT *
FROM layoffs_staging22
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging22
WHERE industry IS NULL
OR industry ='';

SELECT *
FROM layoffs_staging22
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging22 AS t1
JOIN layoffs_staging22 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging22 AS t1
JOIN layoffs_staging22 as t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- REMOVE COLUMNS AND ROWS
SELECT * 
FROM layoffs_staging22;

DELETE
FROM layoffs_staging22
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging22
DROP COLUMN row_num;











