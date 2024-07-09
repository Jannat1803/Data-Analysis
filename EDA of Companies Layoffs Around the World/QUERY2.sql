-- EDA : Exploratory Data Analysis
use world_layoffs;
select max(total_laid_off), max(percentage_laid_off) from layoffs_staging2;
-- all details of the company who cut off all the employee (company down) but has raised the highest fund
select * from layoffs_staging2 where percentage_laid_off = 1 order by funds_raised_millions DESC;

 select min(`date`), max(`date`) from layoffs_staging2;
-- from 2020-03-11 to 2023-03-06 these company cut off these amount of people
 select company, sum(total_laid_off) from layoffs_staging2 group by company order by 2 DESC;
 -- highest number of laid off in the type of workplace
  select industry, sum(total_laid_off) from layoffs_staging2 group by industry order by 2 DESC;
-- which countries have laid off how many employees
  select country, sum(total_laid_off) from layoffs_staging2 group by country order by 2 DESC;
-- how many employees have laid off in which year
  select year(`date`), sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 1 DESC;
  select stage, sum(total_laid_off) from layoffs_staging2 group by stage order by 2 DESC;
-- percentage_laid_off is not much relevant in these cases so wroking with total_laid_off

-- select `date` from layoffs_staging2;

-- rolling total layoffs
with Rolling_Total as(
select substring(`date`, 1, 7) as `Year_Month`, sum(total_laid_off) as total_off from layoffs_staging2
where substring(`date`, 1, 7) is not null 
group by `Year_Month` 
order by `Year_Month` ASC)
select `Year_Month`,total_off, sum(total_off) over(order by 'Year_Month' rows unbounded preceding) as rolling_total
from Rolling_Total;


 select company, year(`date`), sum(total_laid_off) from layoffs_staging2 
 group by company, year(`date`) order by sum(total_laid_off) DESC;

-- Top 5 companies who laid_off highest employee year-by-by
with Company_Year(company, years, total_off) as
(
 select company, year(`date`), sum(total_laid_off) from layoffs_staging2 
 group by company, year(`date`)
 ),
 Company_Year_Ranking as(
 select *, dense_rank() over(partition by years order by total_off desc) as Ranking
 from Company_Year where years is not null)
 select * from Company_Year_Ranking where Ranking<=5;
 ;