/*Stored procedure used for Unit Testing on Sales_Fact table*/
create proc facttest
(    
   @Order_Key int = NULL,     
   @Customer_Key int  = NULL,
   @Product_Key  int = NULL,     
   @Shipping_Key int  = NULL,
   @Product_price	float = NULL,
   @Product_discount_price float = NULL,
   @Total_discount_price float = NULL,
   @Product_profit float = NULL,
   @Total_sales float = NULL
   )
   as
 INSERT INTO Sales_Fact
 VALUES 
      ( 
   @Order_Key,     
   @Customer_Key,
   @Product_Key,     
   @Shipping_Key,
   @Product_price,
   @Product_discount_price,
   @Total_discount_price,
   @Product_profit,
   @Total_sales    
       )  
go

select * from Sales_Fact;
