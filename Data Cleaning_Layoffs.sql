-- SQL Project - Data Cleaning

-- -- https://www.kaggle.com/datasets/swaptr/layoffs-2022








SELECT *
FROM layoffs;




-- Creating a staging table to work on. We want to keep the table with the raw data in case something happens.

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- For data cleaning we will follow these steps:
-- 1. Check and Remove Duplicates
-- 2. Standardize and Fix Errors
-- 3. Check Null Values
-- 4. Remove unnecessary Rows and Columns


-- 1. Check and Remove Duplicates

	# Check for Duplicates


SELECT *,
	ROW_NUMBER() OVER(
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;



-- We want to delete the row number > 1

WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
	
DELETE
FROM duplicate_cte
WHERE row_num > 1;


-- Another option, we can add new column with row numbers as values. Then delete it later on.


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
		ROW_NUMBER() OVER(
				PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2;

-- Now we can delete the duplicates

DELETE
FROM layoffs_staging2
WHERE row_num >1;






-- 2. Standardize Data



-- Reviewing column details and making necessary adjustments to standardize the data for consistency.

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);




-- Checked the 'Industry' column and found variations such as 'Crypto' and 'Crypto Currency.' Standardized all entries to 'Crypto' for consistency.


SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;



-- I did the same for 'Country'


SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States.%';




 -- Changed the format of the 'Date' column and modified its data type for consistency and accuracy.

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;





-- 3. Check for Null Values



SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


-- For 'Airbnb,' the industry is 'Travel,' but it appears to be missing.
-- We will write a query to check if another row has the same company name and location. If found, it will update the industry for null values."
-- We will first update the blank to Null Values

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

 
-- We will first update the blank to Null Values

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';



-- To populate the values


SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
	AND T1.location = T2.location
SET T1.industry = T2.industry
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;



-- 3. Check Null Values 
-- Before deleting null values, we need to ensure that they cannot be populated anymore; otherwise, we might remove important data that we are supposed to work on. 
For this reason, we create a separate table to work on while maintaining the table with the raw data.

-- Other Null Values looks normal

	
SELECT *
FROM layoffs_staging2;



-- 4.  Remove unnecessary Rows and Columns


-- We cannot do anything about null values in 'total_laid_off' and 'percentage_laid_off' since there is no available data to populate them. 
Therefore, we can proceed with deleting them..

	
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- We can also delete the 'row_num' column since it is no longer needed after removing duplicates.

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
