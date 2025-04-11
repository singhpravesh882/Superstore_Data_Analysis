use MYPROJECT
SELECT * FROM superstore --Retrieves all records from Superstore dataset

SELECT TOP 10 * FROM superstore --Top 10 records from superstore dataset

SELECT DISTINCT(ship_mode) 
FROM superstore -- Distinct ship mode from superstore

SELECT count(*) as TotalOrders
FROM superstore  --Count of total orders from superstore

SELECT MIN(order_date) as min_date, MAX(order_date) as max_date
FROM superstore		-- minimum date and maximum date of orders from superstore (range of order date)

SELECT 
SUM(CASE WHEN customer_name is NULL then 1 else 0 end) as null_customers,
SUM(CASE WHEN order_id is NULL then 1 else 0 end) as null_orders,
SUM(CASE WHEN sales is NULL then 1 else 0 end) as null_sales
FROM superstore -- Checking for null values in customer name, sales and order_id

SELECT row_id, count(*)
FROM superstore
GROUP BY row_id 
HAVING count(*)>1	-- checking for duplicates in row_id

--CREATING SEPERATE TABLES TO DO THE ANALYSIS

--CUSTOMER TABLE
CREATE TABLE ss_customers(
customer_id varchar(20) PRIMARY KEY,
customer_name varchar(100),
segment varchar(50));

INSERT INTO ss_customers(customer_id, customer_name, segment)
SELECT distinct customer_id, customer_name, segment
from superstore


--ORDERS TABLE
CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES ss_customers(customer_id)
);

INSERT INTO Orders (order_id, order_date, ship_date, ship_mode, customer_id)
SELECT DISTINCT [order_id],
       CAST([order_date] AS DATE),
       CAST([ship_date] AS DATE),
       [ship_mode],
       [customer_id]
FROM superstore;

--PRODUCTS TABLE
CREATE TABLE products(
product_id varchar(20) PRIMARY KEY,
product_name varchar(200),
category varchar(50),
sub_category varchar(50))

SELECT DISTINCT product_id
from superstore;

WITH cte as(
SELECT product_id, product_name, category, sub_category,
ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) as rn
from superstore)

INSERT INTO products(product_id, product_name, category, sub_category)
SELECT [product_id], [product_name], [category], sub_category
FROM cte
where rn=1
		
SELECT * FROM products

--order_details
CREATE TABLE order_details(
order_id varchar(20),
product_id varchar(20),
sales decimal(10,2), 
quantity int,
discount decimal(10,2),
profit decimal(10,2),
FOREIGN KEY (order_id) REFERENCES orders(order_id),
FOREIGN KEY (product_id) REFERENCES products(product_id),
)

INSERT INTO order_details(order_id, product_id, sales, quantity, discount, profit)
SELECT order_id, product_id, sales, quantity, discount, profit
FROM superstore

--LOCATIONS
CREATE TABLE locations(
location_id int identity(1,1) Primary key,
city varchar(50),
state varchar(50),
postal_code varchar(20),
region varchar(50),
country varchar(50)
)

ALTER TABLE Orders --Since we dont have any constraints between orders and locations to relate, we create a identity for locations, which means for every row in locations, an unique integer will be assigned
add location_id int

ALTER TABLE Orders
ADD CONSTRAINT FK_Orders_Locations FOREIGN KEY (location_id) --location_id is foreign key in orders which will help to relate with locations table
    REFERENCES locations(location_id);

INSERT INTO locations (city, state, postal_code, region, country)
SELECT DISTINCT city, state, postal_code, region, country
FROM Superstore;

Update o 
set o.location_id = l.location_id --setting the value of location_id in locations table equal to location_id in order_table
from Orders o
join superstore s on s.order_id = o.order_id --joining superstore and order with order id to match the location_id
join locations l on s.city=l.city --joining locations table with superstore table so that locations in superstore table will be assigned to each location_id
AND s.state=l.state
AND s.postal_code=l.postal_code; --So now, every order in orders has a location ID which can help us know which city, state and post code it is from. 

SELECT * FROM Orders

--So we have seperated the tables to do the analysis.
--Following are the tables created from superstore
--ORDERS
--PRODUCTS
--ORDER_DETAILS
--LOCATIONS

--Analyzing the superstore data

--Question 1:
--Which is the total sales and profit by category?
SELECT SUM(sales) AS total_sales, SUM(profit) as total_profit, category
FROM superstore
GROUP BY category

--Which region has the highest number of orders
SELECT count( distinct order_id) as no_of_ordetrs, region
FROM superstore
group by region

--Who are top 5 customers by profit
SELECT top 5 SUM(profit) as total_profit, customer_name
FROM superstore
group by customer_name
order by total_profit desc

--What is the average discount given per segment
SELECT segment, AVG(discount) as avg_discount
FROM superstore
GROUP BY segment

--Find the total number of orders and total sales per ship mode
SELECT ship_mode, SUM(sales) as total_sales, count(distinct order_id) as total_orders
FROM superstore
group by ship_mode

--Which sub-category has the lowest total profit
SELECT top 1 sub_category, SUM(profit) as total_profit
FROM superstore
group by sub_category
order by total_profit

--how many orders were placed between Jan 2017 and Dec 2017
SELECT count(distinct order_id) as total_orders
FROM superstore
WHERE order_date between '2017-01-01' and '2017-12-31'

--Show monthly sales trends
SELECT FORMAT (order_date,'yyyy-MM') as order_month, SUM(sales) as monthly_sales
from superstore
group by FORMAT(order_date,'yyyy-MM')
order by order_month

--ADVANCED ANALYSIS

--TOP 5 PRODUCTS BY PROFIT
SELECT top 5 product_name, sum(profit) as total_profit
FROM order_details o
JOIN products p ON p.product_id=o.product_id
GROUP BY product_name
order by total_profit desc


--Monthly cummulative sales by region

with cte as (
select sum(od.sales) as monthly_sales, CAST(DATEFROMPARTS(YEAR(o.order_date), MONTH(o.order_date), 1) as date) as monthly, l.region
FROM order_details od
join orders o on o.order_id=od.order_id
join locations l on l.location_id = o.location_id
Group by CAST(DATEFROMPARTS(YEAR(o.order_date), MONTH(o.order_date), 1) as date), l.region
)
select Monthly, region, monthly_sales,
SUM(monthly_sales) OVER (PARTITION BY region order by monthly 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cummulative_sales
from cte
order by region, Monthly

--Customers with more than 5 purchases
select o.customer_id, s.customer_name, count(distinct o.order_id) as total_orders
from Orders o
join superstore s on s.customer_id=o.customer_id
group by o.customer_id,customer_name
having count(distinct o.order_id)>5

--Profit category (high profit if >100, low profit between 0 and 100, else loss)
with cte as (select distinct order_id, sum(profit) as total_profit,
case when sum(profit)>100 then 'High Profit' 
when sum(profit) between 0 and 100 then 'Low Profit'
else 'Loss' 
end as Profit_category
from order_details
group by order_id)
SELECT * 
from cte 
where Profit_category = 'High Profit'
order by total_profit desc

--Highest selling category per region
with cte as(
SELECT s.category, l.region, sum(o.sales) as total_sales
from order_details o
join superstore s on s.order_id = o.order_id
join orders od on od.order_id = o.order_id
join locations l on l.location_id= od.location_id
group by s.category, l.region)
select *
from(
select category, region, total_sales,
ROW_NUMBER() OVER (Partition by region order by total_sales desc) as rn
from cte)ranked
where rn=1

USE myproject

