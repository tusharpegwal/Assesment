-- Check the total number of records
SELECT COUNT(*) FROM sales_data;

-- Verify that there are no duplicate OrderIds
SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1;

-- Check the total sales and net sales calculations
SELECT OrderId, QuantityOrdered, ItemPrice, PromotionDiscount, total_sales, net_sale FROM sales_data LIMIT 10;

-- Ensure no records with negative or zero net sales
SELECT * FROM sales_data WHERE net_sale <= 0;
