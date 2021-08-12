/*Database to store the cleaned and processed data as a datawarehouse*/
create database ThinkESDW
use ThinkESDW;
/*Dimension table creation based on the shipping data*/
create table Shipping_Dimension(
Shipping_Key int not null identity(1,1) primary key,
Order_id nvarchar(50),
Shipping_date date,
Shipping_type nvarchar(50),
Postal_code int,
Region nvarchar(50),
[State] nvarchar(50)
);

/*drop table Shipping_Dimension;*/

/*Dimension table creation based on the products data*/
create table Products_Dimension(
Product_Key int not null identity(1,1) primary key,
Order_id nvarchar(50),
Product_id nvarchar(50),
Product_name nvarchar(100),
Product_type nvarchar(50),
Product_category nvarchar(50)
)

/*drop table Products_Dimension*/

/*Dimension table creation based on the customer data*/
create table Customer_Dimension(
Customer_Key int not null identity(1,1) primary key,
Customer_id nvarchar(50),
Order_id nvarchar(50),
Customer_section nvarchar(50)
);

/*drop table Customer_Dimension*/

/*Dimension table creation based on the Order data*/
create table Order_Dimension(
Order_Key int not null identity(1,1) primary key,
Order_id nvarchar(50),
Order_date date,
Order_sales float,
Order_quantity int,
Order_discount float,
Order_profit float
);

/*drop table Order_Dimension*/

/*Centralised Fact table combining all the dimensions giving new facts/insights. Linked with dimension tables with primary-foreign key relationship and represents a star schema*/
create table Sales_Fact(
Sales_Key int not null identity(1,1) primary key,
Order_Key int FOREIGN KEY REFERENCES Order_Dimension(Order_Key),
Customer_Key int FOREIGN KEY REFERENCES Customer_Dimension(Customer_Key),
Product_Key int FOREIGN KEY REFERENCES Products_Dimension(Product_Key),
Shipping_Key int FOREIGN KEY REFERENCES Shipping_Dimension(Shipping_Key),
Product_price float, 
Product_discount_price float,
Total_discount_price float,
Product_profit float,
Total_sales float
);

select * from Sales_Fact;
select * from Order_Dimension;
select * from Shipping_Dimension;
select * from Products_Dimension;
select * from Customer_Dimension;

/*drop table Sales_Fact;*/

/*ETL query to load the final fact table*/
insert into Sales_Fact(Order_Key, Shipping_Key, Product_Key, Customer_Key, Product_price, Product_discount_price, Total_discount_price, Product_profit, Total_sales) 
select a.Order_Key, b.Shipping_Key, c.Product_Key, d.Customer_Key, round((a.Order_sales/a.Order_quantity), 2) as [Product_price], 
round(((a.Order_sales/a.Order_quantity) * a.Order_discount), 2) as [Product_discount_price], round((a.Order_sales * a.Order_discount), 2) as [Total_discount_price],
round((a.Order_profit/a.Order_quantity), 2) as [Product_profit], round((a.Order_sales + a.Order_profit), 2) as [Total_sales]
from Order_Dimension a, Customer_Dimension d, Products_Dimension c, Shipping_Dimension b
where a.[Order_id] = b.[Order_id] and a.[Order_id] = c.[Order_id] and a.[Order_id] = d.[Order_id];

