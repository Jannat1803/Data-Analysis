-- Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove any unnecessary columns (removing any column from raw dataset is problematic so need to create another dataset.)

USE world_layoffs;
-- CREATE TABLE layoffs_staging LIKE layoffs;
-- INSERT layoffs_staging SELECT * FROM layoffs;
-- select * from layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num>1;
-- to remove the duplicate values need to create another table with column row_num so that run query as we cannot update into cte
-- CREATE TABLE `layoffs_staging2` (
--   `company` text,
--   `location` text,
--   `industry` text,
--   `total_laid_off` int DEFAULT NULL,
--   `percentage_laid_off` text,
--   `date` text,
--   `stage` text,
--   `country` text,
--   `funds_raised_millions` int DEFAULT NULL,
--   `row_num` int
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- insert into layoffs_staging2
-- SELECT *,
-- ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
-- FROM layoffs_staging;

select * from layoffs_staging2 where row_num>1;
delete from layoffs_staging2 where row_num>1;
select * from layoffs_staging2;

-- Standardizing data (finding issues in the data and fixing it)
update layoffs_staging2 set company = TRIM(company);

select distinct industry from layoffs_staging2 order by 1; -- 1 represents column number
select * from layoffs_staging2 where industry like 'Crypto%';
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';

select distinct location from layoffs_staging2 order by 1; -- ok, no cleaninh needed

select distinct country from layoffs_staging2 order by 1;
select * from layoffs_staging2 where country like 'United States%' order by 1;
select distinct country, TRIM(TRAILING '.' FROM country) from layoffs_staging2 order by 1; -- checking if it works
update layoffs_staging2 set country = TRIM(TRAILING '.' FROM country) where country like 'United States%'; -- updating accordingly

select `date` from layoffs_staging2; 
update layoffs_staging2 set `date` = str_to_date(`date`, '%m/%d/%Y');
alter table layoffs_staging2 modify column `date` DATE; -- changing the datatype of the table, not to do in raw data table

-- null or blank values

select * from layoffs_staging2 where industry IS NULL or industry = ''; 
-- check for the industries that are null or blank has the same company, if it belongs to the same company than blank industry will be replaced with non blank industry
select t1.industry, t2.industry from layoffs_staging2 as t1 join layoffs_staging2 as t2
on t1.company=t2.company where (t1.industry is null or t1.industry = '') and t2.industry is not null;
-- updating the blank value to null as it couldn't replace the blank values
update layoffs_staging2 set industry = NULL where industry = '';
-- update the table
update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company
set t1.industry = t2.industry where t1.industry is null and t2.industry is not null;

-- remove unnecessary columns

-- don't do it if not sure about the necessity of the data
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2 drop column row_num;
select * from layoffs_staging2;
