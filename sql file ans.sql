-- create database if not exists amazon_datas;
-- use amazon_datas;


-- CREATE TABLE orders (
--     OrderDate DATE,
--     OrderID DECIMAL(30,0),            -- bigger than BIGINT
--     DeliveryDate DATE,
--     CustomerID DECIMAL(30,0),
--     Location VARCHAR(100),
--     Zone VARCHAR(50),
--     DeliveryType VARCHAR(50),
--     ProductCategory VARCHAR(100),
--     SubCategory VARCHAR(100),
--     Product TEXT,
--     UnitPrice Varchar(100),
--     ShippingFee DECIMAL(20,2),
--     OrderQuantity INT,
--     SalePrice Varchar(100),
--     `Status` VARCHAR(50),
--     Reason TEXT,
--     Rating INT
-- );
-- LOAD DATA INFILE 'orders.csv'
-- INTO TABLE orders
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (OrderDate, OrderID, DeliveryDate, CustomerID, Location, Zone, DeliveryType, ProductCategory, SubCategory, Product, UnitPrice, ShippingFee, OrderQuantity, SalePrice, `Status`, Reason, Rating);

-- create table customers (
-- 	CustomerID bigint,
--     CustomerAge int,
--     CustomerGender varchar(50)
--     );
-- LOAD DATA INFILE 'customers.csv'
-- INTO TABLE customers
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (CustomerID, CustomerAge,CustomerGender);

-- OBJECTIVES 
-- 14.	Identify the top 5 most valuable customers using a composite score that combines three key metrics: (SQL)
-- a.	Total Revenue (50% weight): The total amount of money spent by the customer.
-- b.	Order Frequency (30% weight): The number of orders placed by the customer, indicating their loyalty and engagement.
-- c.	Average Order Value (20% weight): The average value of each order placed by the customer, reflecting the typical transaction size.
WITH CustomerMetrics AS (
    SELECT 
        CustomerID,
        SUM(`Sale Price`) AS Total_Revenue,
        COUNT(OrderID) AS Order_Frequency,
        COALESCE(AVG(`Sale Price`), 0) AS Avg_Order_Value
    FROM Orders
    GROUP BY CustomerID
),
CustomerRanks AS (
    SELECT 
        CustomerID,
        -- Assign Ranks (Higher Values Get Higher Ranks)
        RANK() OVER (ORDER BY Total_Revenue DESC) AS Revenue_Rank,
        RANK() OVER (ORDER BY Order_Frequency DESC) AS Frequency_Rank,
        RANK() OVER (ORDER BY Avg_Order_Value DESC) AS AOV_Rank,

        -- Get Maximum Ranks for Normalization
        COUNT(*) OVER () AS Max_Rank
    FROM CustomerMetrics
)
SELECT 
    CustomerID,
    -- Normalize ranks between 0 and 1
    (
        (Revenue_Rank * 1.0 / Max_Rank) * 0.5 + 
        (Frequency_Rank * 1.0 / Max_Rank) * 0.3 + 
        (AOV_Rank * 1.0 / Max_Rank) * 0.2
    ) AS Composite_Score
FROM CustomerRanks
ORDER BY Composite_Score DESC
LIMIT 5;

-- 15.	Calculate the month-over-month growth rate in total revenue across the entire dataset. (SQL)
WITH monthly_revenue AS (
    SELECT 
        DATE_FORMAT(OrderDate, '%Y-%m') AS Month,
        round(SUM(`SalePrice`),2) AS TotalRevenue
    FROM 
        orders
    WHERE 
        Status = 'Delivered'
    GROUP BY 
        DATE_FORMAT(OrderDate, '%Y-%m')
    ORDER BY 
        Month
),
growth_calc AS (
    SELECT 
        Month,
        TotalRevenue,
        LAG(TotalRevenue) OVER (ORDER BY Month) AS PrevMonthRevenue
    FROM 
        monthly_revenue
)
SELECT 
    Month,
    TotalRevenue,
    PrevMonthRevenue,
    ROUND(
        (TotalRevenue - PrevMonthRevenue) / PrevMonthRevenue * 100, 2
    ) AS MoM_Growth_Percentage
FROM 
    growth_calc;


-- 16.	Calculate the rolling 3-month average revenue for each product category. (SQL)
WITH MonthlyCategoryRevenue AS (
SELECT DATE_FORMAT(OrderDate, '%Y-%m') AS MonthYear,
ProductCategory, SUM(SalePrice) AS TotalRevenue
FROM Orders
GROUP BY DATE_FORMAT(OrderDate, '%Y-%m'), ProductCategory
),
RollingRevenue AS (
SELECT MonthYear, ProductCategory,TotalRevenue,
ROUND(AVG(TotalRevenue) OVER (PARTITION BY ProductCategory ORDER BY MonthYear ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS Rolling3MonthAvg
FROM MonthlyCategoryRevenue
)
SELECT MonthYear,ProductCategory,TotalRevenue,Rolling3MonthAvg
FROM RollingRevenue
ORDER BY ProductCategory, MonthYear;

-- 17.	Update the orders table to apply a 15% discount on the `Sale Price` for orders placed by customers who have made at least 10 orders. (SQL)
SET SQL_SAFE_UPDATES = 1;

UPDATE Orders
JOIN (
    SELECT CustomerID
    FROM Orders
    GROUP BY CustomerID
    HAVING COUNT(*) >= 10
) AS freq
ON Orders.CustomerID = freq.CustomerID
SET Orders.`SalePrice` = Orders.`SalePrice` * 0.85;

-- 18.	Calculate the average number of days between consecutive orders for customers who have placed at least five orders. (SQL)
WITH CustomerOrders AS (
    SELECT 
        CustomerID,
        OrderDate,
        LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderDate
    FROM Orders
),
DateDiffs AS (
    SELECT 
        CustomerID,
        DATEDIFF(OrderDate, PreviousOrderDate) AS DaysBetween
    FROM CustomerOrders
    WHERE PreviousOrderDate IS NOT NULL
),
QualifiedCustomers AS (
    SELECT CustomerID
    FROM Orders
    GROUP BY CustomerID
    HAVING COUNT(orderid) >= 5
)
SELECT 
    AVG(DaysBetween) AS AvgDaysBetweenOrders
FROM DateDiffs
WHERE CustomerID IN (SELECT CustomerID FROM QualifiedCustomers);

-- 19.	Identify customers who have generated revenue that is more than 30% higher than the average revenue per customer. (SQL)
WITH RevenuePerCustomer AS (
    SELECT CustomerID, SUM(`SalePrice`) AS TotalRevenue
    FROM Orders
    GROUP BY CustomerID
),
AverageRevenue AS (
    SELECT AVG(TotalRevenue) AS AvgRevenue
    FROM RevenuePerCustomer
)
SELECT 
-- rpc.CustomerID,
-- rpc.TotalRevenue,
count(rpc.customerid) as total_customers
FROM RevenuePerCustomer rpc
JOIN AverageRevenue ar ON 1=1
WHERE rpc.TotalRevenue > 1.3 * ar.AvgRevenue;

-- 20.	Determine the top 3 product categories that have shown the highest increase in sales over the past year compared to the previous year. (SQL)
WITH YearlySales AS (
    SELECT 
        `ProductCategory`,
        YEAR(`OrderDate`) AS SaleYear,
        round(SUM(`SalePrice`),2) AS TotalSales
    FROM Orders
    GROUP BY `ProductCategory`, YEAR(`OrderDate`)
),
SalesComparison AS (
    SELECT 
        curr.`ProductCategory`,
        curr.TotalSales AS CurrentYearSales,
        prev.TotalSales AS PreviousYearSales,
        round((curr.TotalSales - prev.TotalSales),2) AS SalesIncrease
    FROM YearlySales curr
    JOIN YearlySales prev
      ON curr.`ProductCategory` = prev.`ProductCategory`
     AND curr.SaleYear = prev.SaleYear + 1
)
SELECT 
    `ProductCategory`,
    CurrentYearSales,
    PreviousYearSales,
    SalesIncrease
FROM SalesComparison
ORDER BY SalesIncrease DESC
LIMIT 3;






