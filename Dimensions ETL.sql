/*Database to store the raw data tables*/
create database ThinkES
/*Reading superstore_south_sales JSON file with the below script */
declare @json_table nvarchar(max)
select @json_table = BulkColumn
from openrowset(bulk 'E:\Visual Studio 2019\Downloads\Data Analyst Project\group_sales_data\superstore_south_sales.json', single_clob) as NewTable
insert into superstore_south_sales select [SampleDataSouth].[Order], [SampleDataSouth].[Date], [SampleDataSouth].[Date Shipped],
[SampleDataSouth].[Shipping Method], [SampleDataSouth].[Customer Number], [SampleDataSouth].[Segment], [SampleDataSouth].[Postal Code 1],
[SampleDataSouth].[Product ID], [SampleDataSouth].[Sales], [SampleDataSouth].[Quantity], [SampleDataSouth].[Discount], [SampleDataSouth].[Profit]
from openjson(@json_table) 
with(
[sample_data_south] nvarchar(max) as json
) as [sample_data]
cross apply openjson([sample_data].[sample_data_south])
with(
[Order] nvarchar(50),
[Date] nvarchar(50),
[Date Shipped] nvarchar(50),
[Shipping Method] nvarchar(50),
[Customer Number] nvarchar(100),
[Segment] nvarchar(50),
[Postal Code 1] int,
[Product ID] nvarchar(100),
[Sales] nvarchar(50),
[Quantity] int,
[Discount] nvarchar(50),
[Profit] nvarchar(100)
)as [SampleDataSouth]


/*drop table superstore_south_sales;*/
/*Retrieving all the records of the four table imported using the flat file importer after data cleaning and modification with Python*/
select * from [superstore_south_sales];
select * from superstore_east_sales;
select * from superstore_central_sales;
select * from superstore_west_sales;

/*Removing null values from superstore_west_sales table */
delete from [superstore_west_sales] where [postal_code] is null;
select * from Companies;
select * from Postal_codes;
select * from Product_types;
select * from Products;

/*Dropping the additional column 'column1' of the Python dataframe*/
alter table [superstore_south_sales] drop column column1
alter table [superstore_west_sales] drop column column1
alter table [superstore_east_sales] drop column column1
alter table [superstore_central_sales] drop column column1

/*ETL data insertion query for Products_Dimension table in other data warehouse database named as ThinkESDW*/
/*#######################################################################################################################################################################*/
insert into ThinkESDW.dbo.Products_Dimension(Order_id, Product_id, Product_name, Product_type, Product_category) select distinct a.[Order], a.[Product_ID], b.product_name, c.product_type, c.product_category from [superstore_south_sales] a, [Products] b, [Product_types] c
where a.[Product_ID] = b.product_id and b.category = c.product_type order by [Order];

insert into ThinkESDW.dbo.Products_Dimension(Order_id, Product_id, Product_name, Product_type, Product_category) select distinct a.[order_number], a.[product_id], b.product_name, c.product_type, c.product_category from [superstore_east_sales] a, [Products] b, [Product_types] c
where a.[product_id] = b.product_id and b.category = c.product_type order by [order_number];

insert into ThinkESDW.dbo.Products_Dimension(Order_id, Product_id, Product_name, Product_type, Product_category) select distinct a.[Order_ID], a.[Product_ID], b.product_name, c.product_type, c.product_category from [superstore_central_sales] a, [Products] b, [Product_types] c
where a.[Product_ID] = b.product_id and b.category = c.product_type order by [Order_ID];

insert into ThinkESDW.dbo.Products_Dimension(Order_id, Product_id, Product_name, Product_type, Product_category) select distinct a.[order_id], a.[product_id], b.product_name, c.product_type, c.product_category from [superstore_west_sales] a, [Products] b, [Product_types] c
where a.[product_id] = b.product_id and b.category = c.product_type order by [order_id];
/*#######################################################################################################################################################################*/

/*ETL data insertion query for Customer_Dimension table in other data warehouse database named as ThinkESDW*/
/*#######################################################################################################################################################################*/
insert into ThinkESDW.dbo.Customer_Dimension(Customer_id, Customer_section, Order_id) select distinct Customer_Number, Segment, [Order] from superstore_south_sales order by [Order]

insert into ThinkESDW.dbo.Customer_Dimension(Customer_id, Customer_section, Order_id) select distinct customer_key, customer_section, order_number from superstore_east_sales order by [order_number]

insert into ThinkESDW.dbo.Customer_Dimension(Customer_id, Customer_section, Order_id) select distinct Customer_ID, Segment, [Order_ID] from superstore_central_sales order by [Order_ID]

insert into ThinkESDW.dbo.Customer_Dimension(Customer_id, Customer_section, Order_id) select distinct customer_id, segment, order_id from superstore_west_sales order by [order_id]
/*#######################################################################################################################################################################*/

/*ETL data insertion query for Order_Dimension table in other data warehouse database named as ThinkESDW*/
/*#######################################################################################################################################################################*/
insert into ThinkESDW.dbo.Order_Dimension(Order_id, Order_date, Order_sales, Order_quantity, Order_discount, Order_profit) 
select distinct [Order], [Date], Sales, Quantity, Discount, Profit from [superstore_south_sales] order by [Order]

insert into ThinkESDW.dbo.Order_Dimension(Order_id, Order_date, Order_sales, Order_quantity, Order_discount, Order_profit)  
select distinct [order_number], [ord_date], sales, items, reduction, total from [superstore_east_sales] order by [order_number]

insert into ThinkESDW.dbo.Order_Dimension(Order_id, Order_date, Order_sales, Order_quantity, Order_discount, Order_profit)  
select distinct [Order_ID], [Order_Date], Sales, Quantity, Discount, Profit from [superstore_central_sales] order by [Order_ID]

insert into ThinkESDW.dbo.Order_Dimension(Order_id, Order_date, Order_sales, Order_quantity, Order_discount, Order_profit)  
select distinct [order_id], [order_date], sales, quantity, discount, profit from [superstore_west_sales] order by [order_id]
/*#######################################################################################################################################################################*/

/*ETL data insertion query for Shipping_Dimension table in other data warehouse database named as ThinkESDW*/
/*#######################################################################################################################################################################*/
insert into ThinkESDW.dbo.Shipping_Dimension(Shipping_date, Order_id, Shipping_type, Postal_code, Region, [State]) 
select distinct a.[Date_Shipped], a.[Order], a.[Shipping_Method], a.[Postal_Code_1], b.region, b.[state] from [superstore_south_sales] a, [Postal_codes] b 
where a.[Postal_Code_1] = b.postal_code order by [Order];

insert into ThinkESDW.dbo.Shipping_Dimension(Shipping_date, Order_id, Shipping_type, Postal_code, Region, [State]) 
select distinct a.[ship_date], a.[order_number], a.[ship_type], a.[post_code], b.region, b.[state] from [superstore_east_sales] a, [Postal_codes] b 
where a.[post_code] = b.postal_code order by [order_number];

insert into ThinkESDW.dbo.Shipping_Dimension(Shipping_date, Order_id, Shipping_type, Postal_code, Region, [State]) 
select distinct a.[Ship_Date], a.[Order_ID], a.[Ship_Mode], a.[Postal_Code], b.region, b.[state] from [superstore_central_sales] a, [Postal_codes] b 
where a.[Postal_Code] = b.postal_code order by [order_id];

insert into ThinkESDW.dbo.Shipping_Dimension(Shipping_date, Order_id, Shipping_type, Postal_code, Region, [State]) 
select distinct a.[ship_date], a.[order_id], a.[ship_mode], a.[postal_code], b.region, b.[state] from [superstore_west_sales] a, [Postal_codes] b 
where a.[postal_code] = b.postal_code order by [Order_ID];
/*#######################################################################################################################################################################*/
