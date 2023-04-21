use `dw`;
#Query 1
SELECT store.store_name,SUM(total_sale) as revenue FROM sales JOIN store on store.store_id = sales.store_id JOIN date on date.time_id = sales.time_id where date.month = "september" and date.year = 2017 group by sales.store_id order by revenue desc LIMIT 3;

#Query 2
SELECT supplier.supplier_name,SUM(total_sale) as Revenue FROM sales JOIN date on date.time_id = sales.time_id JOIN supplier on supplier.supplier_id = sales.supplier_id where date.weekend = 1 group by supplier.supplier_id order by Revenue desc LIMIT 10;

#We can forcast the next top suppliers by extracting data only for weekends where there wasnt any promotion which might give us a hint of the top suppliers
#whose product is needed by the customers regardless of a promotion or not, also suppliers dealing in necessity goods would be more in demand.

#Query 3
SELECT sales.product_id,product.product_name,date.month,date.quarter,SUM(total_sale) as Revenue FROM sales JOIN product on product.product_id = sales.product_id JOIN date on date.time_id = sales.time_id group by sales.product_id, date.quarter,date.month;

#Query 4
SELECT store.store_name,product.product_name,SUM(total_sale) as Revenue from sales join store on store.store_id = sales.store_id join product on product.product_id = sales.product_id group by sales.store_id,sales.product_id;

#Query 5
SELECT store.store_name,date.quarter,SUM(total_sale) as Revenue from sales join date on date.time_id = sales.time_id join store on store.store_id = sales.store_id group by store.store_id,date.quarter;

#Query 6
SELECT product.product_name,SUM(total_sale) as Revenue from sales join date on date.time_id = sales.time_id join product on product.product_id = sales.product_id where date.weekend = 1 group by sales.product_id order by Revenue desc limit 5;

#Query 7
SELECT store_id,supplier_id,product_id from sales group by store_id,supplier_id,product_id with rollup;

#This query assumes that there is a hierarchy between store,supplier and product which is store > supplier > product.
#ROLLUP makes grouping sets from this hierarchy we can see in the query result that all values matching the first value of store_id are mentioned and so on
#also if a value is not present then it shows NULL.

#Query 8
#Half of Year Analysis
SELECT product.product_name,date.half_of_year,SUM(total_sale) as Revenue from sales join product on product.product_id = sales.product_id join date on date.time_id = sales.time_id where date.year = 2017 group by sales.product_id, half_of_year;

#Yearly Analysis
SELECT product.product_name,SUM(total_sale) as Revenue from sales join product on product.product_id = sales.product_id join date on date.time_id = sales.time_id where date.year = 2017 group by sales.product_id;

#Query 9
SELECT * FROM dw.product where product_name = "Tomatoes";
#There are two entries for Tomatoes in the Products Table.

#Query 10
DROP TABLE if exists `dw`.`STORE_PRODUCT_ANALYSIS`;
CREATE TABLE `STORE_PRODUCT_ANALYSIS` AS 
  SELECT store_name,product_name,total_sale FROM sales join store on sales.store_id = store.store_id join product on sales.product_id = product.product_id order by store_name,product_name;

SELECT * from STORE_PRODUCT_ANALYSIS;
#The benefit of materialized view is it reduces the execution time for complex queries with joins and aggregate functions
#it is most useful when the query execution time is high but the result is small.
  








