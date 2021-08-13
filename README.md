# Data engineering and analysis using multiple source data

Sample data
Group Reference Data (Excel): This contains useful lookup data that further enriches the data model
	- Companies: Details of the subsidiary companies in the group
	- Postal Codes: A list of valid postal codes and the regions they belong to
	- Product Types: A breakdown of product types and categories
	- Products: A list of all products available for sale within the group

Group Sales Data: The sales data of each company submitted to the group in varying formats
	- superstore_central_sales.xlsx
	- superstore_east_sales.csv
	- superstore_south_sales.json
	- superstore_west_sales.xlsx

Project Requirements:

The goal is to consolidate the sales data from various different sources into a single table, construct the group data model and report meaningful insights on the data.

1. ETL scripts must be written to extract/transform the sales and reference data before loading to the target tables.
2. ETL scripts can be written using the language of choice (e.g. python, SQL Management Studio, MS Visual Studio)
3. Data must be landed in a database
4. ETL must be accompanied by unit tests (preferably using the same technology as 2)
5. A data entity diagram to characterize the underlying data relationships
6. A visualization of the consolidated data using the tool of choice (Power BI) pointed at the database that showcases the following:
	a. Ability for users to compare one company in the group to another
	b. Ability for users to see a trend over time of a company's performance
	c. Ability for users to see a geographical breakdown of sales
	c. Ability for users to see the best performing products across the group
	d. Ability for users to drill down to the underlying data
	e. Any further insights that you deem useful.
