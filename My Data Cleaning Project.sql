use data_clean2;
select * from dirty_Cafe_sales;
create table dirty_Cafe_sales_staging
like dirty_Cafe_sales;

insert into dirty_cafe_sales_staging
select * from dirty_cafe_sales;

select * from dirty_cafe_sales_staging;

select * from dirty_Cafe_sales_staging
where `transaction id1` = TXN_1000555;

alter table dirty_Cafe_sales_staging
rename column `transaction id` to transaction_id;

alter table dirty_Cafe_sales_staging
rename column `price per unit` to price_per_unit;

alter table dirty_Cafe_sales_staging
rename column `total spent` to Total_spent;

alter table dirty_Cafe_sales_staging
rename column `Payment method` to Payment_method;

alter table dirty_Cafe_sales_staging
rename column `transaction date` to transaction_date;

select *,
row_number()over(Partition by transaction_id,Item,quantity,total_Spent,price_per_unit,payment_method,location,Transaction_Date) as Row_num
from dirty_Cafe_sales_staging;

with Duplicate_cte1 as
(
select *,
row_number()over(Partition by transaction_id,Item,quantity,total_Spent,price_per_unit,payment_method,location,Transaction_Date) as Row_num
from dirty_Cafe_sales_staging
)
delete from duplicate_cte1
where row_num >1; 

CREATE TABLE `dirty_cafe_sales_staging2` (
  `transaction_id` text,
  `Item` text,
  `Quantity` int DEFAULT NULL,
  `price_per_unit` double DEFAULT NULL,
  `Total_spent` text,
  `Payment_method` text,
  `Location` text,
  `transaction_date` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from dirty_cafe_sales_staging2;

insert into dirty_cafe_sales_staging2
select *,
row_number()over(Partition by transaction_id,Item,quantity,total_Spent,price_per_unit,payment_method,location,Transaction_Date) as Row_num
from dirty_Cafe_sales_staging;

delete  from dirty_cafe_sales_staging2
where row_num >1;

select * from dirty_cafe_sales_staging2;

select * from  dirty_Cafe_sales_staging
where transaction_id ='TXN_2544072';

select * from dirty_cafe_sales_staging2
where item ='error';

update dirty_cafe_sales_staging2
set item = 'unknown'
where item = 'Error' or item ='  ';


UPDATE dirty_cafe_sales_staging2
SET item = 'unknown'
WHERE item = 'Error' OR item IS NULL or item =' ';

select distinct Total_spent from dirty_cafe_sales_staging2;

UPDATE dirty_cafe_sales_staging2
SET total_spent = 'unknown'
WHERE total_spent = 'Error' OR total_spent = '' OR total_spent IS NULL;

select distinct payment_method from  dirty_cafe_sales_staging2;

UPDATE dirty_cafe_sales_staging2
SET payment_method = 'unknown'
WHERE payment_method = 'Error' OR payment_method = '' OR payment_method IS NULL;

select distinct location from dirty_cafe_sales_staging2;

UPDATE dirty_cafe_sales_staging2
SET location = 'unknown'
WHERE location = 'Error' OR location = '' OR location IS NULL;

UPDATE dirty_cafe_sales_staging2
SET transaction_date = 'unknown'
WHERE  transaction_date IS NULL;
select * from  dirty_cafe_sales_staging2;

select *
from dirty_cafe_sales_staging2;

alter table  dirty_cafe_sales_staging2
drop column row_num;


select *
from dirty_cafe_sales_staging2;
