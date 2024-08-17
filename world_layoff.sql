-- Data Cleaning
use world_layoffs;
--  1. Remove Duplicates
--  2. Standardize Data
--  3. Null or Blank values
--  4. Remove Any Column
select count(*) from layoffs;
CREATE TABLE layoffs_staging like layoffs;
INSERT layoffs_staging SELECT * FROM layoffs;
SELECT * FROM layoffs_staging;

-- NOW CHECK HOW MANY VALUE ARE SAME USING ROW_NUM
SELECT *,
row_number() over (partition by company ,`date`,location,stage, industry,total_laid_off,funds_raised_millions,
percentage_laid_off) as row_num
FROM layoffs_staging;

with duplicate_cte as (
SELECT *,
row_number() over (partition by company ,`date`,location,stage, industry,total_laid_off,funds_raised_millions,
percentage_laid_off) as row_num
FROM layoffs_staging)
select * from duplicate_cte where row_num > 1;
SELECT * FROM layoffs_staging where company = "Casper";
-- now delete duplicates from this
with duplicate_cte as (
SELECT *,
row_number() over (partition by company ,`date`,location,stage, industry,total_laid_off,funds_raised_millions,
percentage_laid_off) as row_num
FROM layoffs_staging)
delete from duplicate_cte where row_num > 1;
-- Error Code: 1175. You are using safe update mode and
--  you tried to update a table without a WHERE that uses a KEY column.

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2 where row_num > 1;

insert into layoffs_staging2
SELECT *,
row_number() over (partition by company ,`date`,location,stage, industry,total_laid_off,funds_raised_millions,
percentage_laid_off) as row_num
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;  -- Re-enable safe updates

select * from layoffs_staging2 where row_num > 1;
select * from layoffs_staging2 ;
-- Standardizing data
select company , trim(company) from layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

select distinct industry from layoffs_staging2 order by 1;
select*from layoffs_staging2 where industry like "Crypto%";

UPDATE  layoffs_staging2
SET industry = "Crypto"
where industry like "Crypto%";

select distinct country from layoffs_staging2 where country like "United States%";
select distinct country , trim(trailing "." from country ) from layoffs_staging2 order by 1 ;

UPDATE layoffs_staging2
SET country = trim(trailing "." from country )
where country like  "United States%";

select 	`date`,str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;
select * from layoffs_staging2;
-- 3. Remove Null or Blank values

UPDATE layoffs_staging2
SET industry = null
where industry = "";

select distinct industry from layoffs_staging2 ;
select * from layoffs_staging2 where industry is null or industry = "";
select * from layoffs_staging2 where company = 'Airbnb';

select *  from layoffs_staging2 t1 join layoffs_staging2 t2 
on t1.company = t2.company and t1.location = t2.location 
where( t1.industry is null or t1.industry = "" )and t2.industry is not null;

UPDATE layoffs_staging2 t1
join layoffs_staging2 t2 
on t1.company = t2.company and t1.location = t2.location
SET t1.industry = t2.industry 
where( t1.industry is null  )and t2.industry is not null;

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
DELETE  from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

select * from layoffs_staging2;
--  4. Remove Any Column 
ALTER TABLE layoffs_staging2 DROP COLUMN row_num