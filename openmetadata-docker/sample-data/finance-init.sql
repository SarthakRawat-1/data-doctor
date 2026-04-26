-- Finance Sample Database for Data Doctor Testing
-- Realistic financial data with banking transactions

-- Table 1: Account Dimension
CREATE TABLE dim_account (
    account_id INT AUTO_INCREMENT PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    account_status VARCHAR(20) NOT NULL,
    credit_score INT,
    account_opened_date DATE NOT NULL,
    last_activity_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert 1000 sample accounts (reduced from 2000)
DELIMITER $
CREATE PROCEDURE insert_accounts()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
        INSERT INTO dim_account (
            account_number, account_type, customer_name, email, phone,
            address, city, state, zip_code, account_status, credit_score,
            account_opened_date, last_activity_date
        )
        VALUES (
            CONCAT('ACC', LPAD(i, 10, '0')),
            CASE FLOOR(RAND() * 6)
                WHEN 0 THEN 'Checking'
                WHEN 1 THEN 'Savings'
                WHEN 2 THEN 'Credit Card'
                WHEN 3 THEN 'Mortgage'
                WHEN 4 THEN 'Investment'
                ELSE 'Business'
            END,
            CONCAT('Customer ', i, ' ', 
                   CASE FLOOR(RAND() * 5)
                       WHEN 0 THEN 'Smith'
                       WHEN 1 THEN 'Johnson'
                       WHEN 2 THEN 'Williams'
                       WHEN 3 THEN 'Brown'
                       ELSE 'Davis'
                   END),
            CONCAT('customer', i, '@finance.example.com'),
            CONCAT('555-', LPAD(FLOOR(RAND() * 10000), 4, '0')),
            CONCAT(FLOOR(RAND() * 9999), ' Financial St'),
            CASE (i % 12)
                WHEN 0 THEN 'New York'
                WHEN 1 THEN 'Los Angeles'
                WHEN 2 THEN 'Chicago'
                WHEN 3 THEN 'Houston'
                WHEN 4 THEN 'Phoenix'
                WHEN 5 THEN 'Philadelphia'
                WHEN 6 THEN 'San Antonio'
                WHEN 7 THEN 'San Diego'
                WHEN 8 THEN 'Dallas'
                WHEN 9 THEN 'San Jose'
                WHEN 10 THEN 'Austin'
                ELSE 'Seattle'
            END,
            CASE (i % 12)
                WHEN 0 THEN 'NY'
                WHEN 1 THEN 'CA'
                WHEN 2 THEN 'IL'
                WHEN 3 THEN 'TX'
                WHEN 4 THEN 'AZ'
                WHEN 5 THEN 'PA'
                WHEN 6 THEN 'TX'
                WHEN 7 THEN 'CA'
                WHEN 8 THEN 'TX'
                WHEN 9 THEN 'CA'
                WHEN 10 THEN 'TX'
                ELSE 'WA'
            END,
            LPAD(FLOOR(10000 + RAND() * 89999), 5, '0'),
            CASE FLOOR(RAND() * 10)
                WHEN 0 THEN 'Active'
                WHEN 1 THEN 'Active'
                WHEN 2 THEN 'Active'
                WHEN 3 THEN 'Active'
                WHEN 4 THEN 'Active'
                WHEN 5 THEN 'Active'
                WHEN 6 THEN 'Active'
                WHEN 7 THEN 'Dormant'
                WHEN 8 THEN 'Frozen'
                ELSE 'Closed'
            END,
            FLOOR(300 + RAND() * 550),  -- Credit score 300-850
            DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 3650) DAY),  -- Last 10 years
            DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 90) DAY)  -- Last 90 days
        );
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_accounts();
DROP PROCEDURE insert_accounts;

-- Table 2: Transaction Fact
CREATE TABLE fact_transaction (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT,
    transaction_date DATETIME NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    transaction_category VARCHAR(100),
    amount DECIMAL(12, 2) NOT NULL,
    balance_after DECIMAL(12, 2),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(100),
    location VARCHAR(255),
    transaction_status VARCHAR(20) NOT NULL,
    is_fraudulent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES dim_account(account_id)
);

-- Insert 25000 sample transactions (reduced from 50000)
DELIMITER $
CREATE PROCEDURE insert_transactions()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE running_balance DECIMAL(12, 2);
    WHILE i <= 25000 DO
        SET running_balance = ROUND(1000 + RAND() * 50000, 2);
        
        INSERT INTO fact_transaction (
            account_id, transaction_date, transaction_type, transaction_category,
            amount, balance_after, merchant_name, merchant_category, location,
            transaction_status, is_fraudulent
        )
        VALUES (
            FLOOR(1 + RAND() * 1000),  -- Random account 1-1000 (updated)
            DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 365) DAY),  -- Last year
            CASE FLOOR(RAND() * 6)
                WHEN 0 THEN 'Debit'
                WHEN 1 THEN 'Credit'
                WHEN 2 THEN 'Transfer'
                WHEN 3 THEN 'Withdrawal'
                WHEN 4 THEN 'Deposit'
                ELSE 'Payment'
            END,
            CASE FLOOR(RAND() * 15)
                WHEN 0 THEN 'Groceries'
                WHEN 1 THEN 'Dining'
                WHEN 2 THEN 'Gas'
                WHEN 3 THEN 'Shopping'
                WHEN 4 THEN 'Entertainment'
                WHEN 5 THEN 'Healthcare'
                WHEN 6 THEN 'Utilities'
                WHEN 7 THEN 'Rent/Mortgage'
                WHEN 8 THEN 'Insurance'
                WHEN 9 THEN 'Travel'
                WHEN 10 THEN 'Education'
                WHEN 11 THEN 'Salary'
                WHEN 12 THEN 'Investment'
                WHEN 13 THEN 'Refund'
                ELSE 'Other'
            END,
            ROUND(-500 + RAND() * 5000, 2),  -- -$500 to $4500
            running_balance,
            CASE FLOOR(RAND() * 20)
                WHEN 0 THEN 'Amazon'
                WHEN 1 THEN 'Walmart'
                WHEN 2 THEN 'Target'
                WHEN 3 THEN 'Starbucks'
                WHEN 4 THEN 'McDonalds'
                WHEN 5 THEN 'Shell Gas'
                WHEN 6 THEN 'Whole Foods'
                WHEN 7 THEN 'Netflix'
                WHEN 8 THEN 'Spotify'
                WHEN 9 THEN 'Apple Store'
                WHEN 10 THEN 'Best Buy'
                WHEN 11 THEN 'Home Depot'
                WHEN 12 THEN 'CVS Pharmacy'
                WHEN 13 THEN 'Uber'
                WHEN 14 THEN 'Lyft'
                WHEN 15 THEN 'Delta Airlines'
                WHEN 16 THEN 'Hilton Hotels'
                WHEN 17 THEN 'ATM Withdrawal'
                WHEN 18 THEN 'Direct Deposit'
                ELSE 'Wire Transfer'
            END,
            CASE FLOOR(RAND() * 10)
                WHEN 0 THEN 'Retail'
                WHEN 1 THEN 'Food & Beverage'
                WHEN 2 THEN 'Gas Station'
                WHEN 3 THEN 'Online Shopping'
                WHEN 4 THEN 'Subscription'
                WHEN 5 THEN 'Healthcare'
                WHEN 6 THEN 'Transportation'
                WHEN 7 THEN 'Travel'
                WHEN 8 THEN 'ATM'
                ELSE 'Transfer'
            END,
            CASE (i % 15)
                WHEN 0 THEN 'New York, NY'
                WHEN 1 THEN 'Los Angeles, CA'
                WHEN 2 THEN 'Chicago, IL'
                WHEN 3 THEN 'Houston, TX'
                WHEN 4 THEN 'Phoenix, AZ'
                WHEN 5 THEN 'Philadelphia, PA'
                WHEN 6 THEN 'San Antonio, TX'
                WHEN 7 THEN 'San Diego, CA'
                WHEN 8 THEN 'Dallas, TX'
                WHEN 9 THEN 'San Jose, CA'
                WHEN 10 THEN 'Austin, TX'
                WHEN 11 THEN 'Seattle, WA'
                WHEN 12 THEN 'Denver, CO'
                WHEN 13 THEN 'Boston, MA'
                ELSE 'Miami, FL'
            END,
            CASE FLOOR(RAND() * 20)
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Failed'
                ELSE 'Completed'
            END,
            CASE FLOOR(RAND() * 200)
                WHEN 0 THEN TRUE
                ELSE FALSE
            END  -- 0.5% fraud rate
        );
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_transactions();
DROP PROCEDURE insert_transactions;

-- Table 3: Account Balance History
CREATE TABLE fact_balance_snapshot (
    snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT,
    snapshot_date DATE NOT NULL,
    balance DECIMAL(12, 2) NOT NULL,
    available_balance DECIMAL(12, 2),
    pending_transactions INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES dim_account(account_id),
    UNIQUE KEY unique_account_date (account_id, snapshot_date)
);

-- Insert daily balance snapshots for last 365 days for 250 accounts (reduced from 500)
DELIMITER $
CREATE PROCEDURE insert_balance_snapshots()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT DEFAULT 0;
    DECLARE acc_id INT;
    WHILE i <= 250 DO
        SET acc_id = i;
        SET j = 0;
        WHILE j < 365 DO
            INSERT INTO fact_balance_snapshot (account_id, snapshot_date, balance, available_balance, pending_transactions)
            VALUES (
                acc_id,
                DATE_SUB(CURDATE(), INTERVAL j DAY),
                ROUND(1000 + RAND() * 50000, 2),
                ROUND(1000 + RAND() * 45000, 2),
                FLOOR(RAND() * 10)
            );
            SET j = j + 1;
        END WHILE;
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_balance_snapshots();
DROP PROCEDURE insert_balance_snapshots;

-- Table 4: Daily Financial Metrics
CREATE TABLE fact_daily_summary (
    summary_date DATE PRIMARY KEY,
    total_transactions INT NOT NULL,
    total_transaction_volume DECIMAL(15, 2) NOT NULL,
    avg_transaction_amount DECIMAL(10, 2),
    total_deposits DECIMAL(15, 2),
    total_withdrawals DECIMAL(15, 2),
    fraud_incidents INT,
    active_accounts INT,
    new_accounts INT
);

-- Insert daily summaries for last 365 days
INSERT INTO fact_daily_summary (
    summary_date, total_transactions, total_transaction_volume,
    avg_transaction_amount, total_deposits, total_withdrawals,
    fraud_incidents, active_accounts, new_accounts
)
SELECT 
    DATE(transaction_date) as summary_date,
    COUNT(*),
    SUM(ABS(amount)),
    AVG(ABS(amount)),
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END),
    SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END),
    COUNT(DISTINCT account_id),
    0  -- Placeholder for new accounts
FROM fact_transaction
GROUP BY DATE(transaction_date);

-- Create indexes for performance
CREATE INDEX idx_transaction_account ON fact_transaction(account_id);
CREATE INDEX idx_transaction_date ON fact_transaction(transaction_date);
CREATE INDEX idx_transaction_status ON fact_transaction(transaction_status);
CREATE INDEX idx_transaction_fraud ON fact_transaction(is_fraudulent);
CREATE INDEX idx_balance_account ON fact_balance_snapshot(account_id);
CREATE INDEX idx_balance_date ON fact_balance_snapshot(snapshot_date);
CREATE INDEX idx_account_number ON dim_account(account_number);
CREATE INDEX idx_account_status ON dim_account(account_status);

SELECT '✅ Finance sample database created successfully!' AS status;
SELECT '📊 Tables created:' AS info;
SELECT '   - dim_account (1000 rows)' AS tables;
SELECT '   - fact_transaction (25000 rows)' AS tables;
SELECT '   - fact_balance_snapshot (91250 rows)' AS tables;
SELECT '   - fact_daily_summary (365 rows)' AS tables;
SELECT '🔗 Relationships:' AS info;
SELECT '   dim_account → fact_transaction (via account_id)' AS relationships;
SELECT '   dim_account → fact_balance_snapshot (via account_id)' AS relationships;
SELECT '🧪 Ready for OpenMetadata ingestion!' AS status;
