-- E-commerce Sample Database for Data Doctor Testing
-- This creates realistic data with proper relationships and statistics

-- Table 1: Customer Dimension
CREATE TABLE dim_customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert 1000 sample customers using stored procedure
DELIMITER $$
CREATE PROCEDURE insert_customers()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
        INSERT INTO dim_customer (email, first_name, last_name, country)
        VALUES (
            CONCAT('customer', i, '@example.com'),
            CONCAT('FirstName', i),
            CONCAT('LastName', i),
            CASE (i % 5)
                WHEN 0 THEN 'USA'
                WHEN 1 THEN 'UK'
                WHEN 2 THEN 'Canada'
                WHEN 3 THEN 'Australia'
                ELSE 'Germany'
            END
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;

CALL insert_customers();
DROP PROCEDURE insert_customers;

-- Table 2: Orders Fact
CREATE TABLE fact_orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATE NOT NULL,
    order_amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);

-- Insert 5000 sample orders
DELIMITER $$
CREATE PROCEDURE insert_orders()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 5000 DO
        INSERT INTO fact_orders (customer_id, order_date, order_amount, order_status)
        VALUES (
            FLOOR(1 + RAND() * 1000),  -- Random customer 1-1000
            DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 365) DAY),  -- Random date in last year
            ROUND(10 + RAND() * 500, 2),  -- Random amount $10-$510
            CASE FLOOR(RAND() * 4)
                WHEN 0 THEN 'completed'
                WHEN 1 THEN 'pending'
                WHEN 2 THEN 'shipped'
                ELSE 'cancelled'
            END
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;

CALL insert_orders();
DROP PROCEDURE insert_orders;

-- Table 3: Revenue Aggregation
CREATE TABLE fact_revenue (
    date DATE PRIMARY KEY,
    total_revenue DECIMAL(12, 2) NOT NULL,
    order_count INT NOT NULL,
    avg_order_value DECIMAL(10, 2) NOT NULL
);

-- Insert daily revenue for last 365 days
INSERT INTO fact_revenue (date, total_revenue, order_count, avg_order_value)
SELECT 
    order_date,
    COALESCE(SUM(order_amount), 0),
    COUNT(*),
    COALESCE(AVG(order_amount), 0)
FROM fact_orders
GROUP BY order_date;

-- Create indexes for performance
CREATE INDEX idx_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_orders_date ON fact_orders(order_date);
CREATE INDEX idx_revenue_date ON fact_revenue(date);

SELECT '✅ Sample database created successfully!' AS status;
SELECT '📊 Tables created:' AS info;
SELECT '   - dim_customer (1000 rows)' AS tables;
SELECT '   - fact_orders (5000 rows)' AS tables;
SELECT '   - fact_revenue (365 rows)' AS tables;
SELECT '🔗 Relationships:' AS info;
SELECT '   dim_customer → fact_orders (via customer_id)' AS relationships;
SELECT '   fact_orders → fact_revenue (via order_date aggregation)' AS relationships;
SELECT '🧪 Ready for OpenMetadata ingestion!' AS status;
