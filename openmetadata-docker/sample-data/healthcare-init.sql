-- Healthcare Sample Database for Data Doctor Testing
-- Realistic healthcare data with HIPAA-compliant synthetic data

-- Table 1: Patient Dimension
CREATE TABLE dim_patient (
    patient_id INT AUTO_INCREMENT PRIMARY KEY,
    medical_record_number VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    blood_type VARCHAR(5),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    insurance_provider VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert 750 sample patients (reduced from 1500)
DELIMITER $
CREATE PROCEDURE insert_patients()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 750 DO
        INSERT INTO dim_patient (
            medical_record_number, first_name, last_name, date_of_birth, 
            gender, blood_type, email, phone, address, city, state, zip_code, insurance_provider
        )
        VALUES (
            CONCAT('MRN', LPAD(i, 8, '0')),
            CONCAT('Patient', i),
            CONCAT('LastName', i),
            DATE_SUB(CURDATE(), INTERVAL (20 + FLOOR(RAND() * 60)) YEAR),  -- Age 20-80
            CASE FLOOR(RAND() * 3)
                WHEN 0 THEN 'Male'
                WHEN 1 THEN 'Female'
                ELSE 'Other'
            END,
            CASE FLOOR(RAND() * 8)
                WHEN 0 THEN 'A+'
                WHEN 1 THEN 'A-'
                WHEN 2 THEN 'B+'
                WHEN 3 THEN 'B-'
                WHEN 4 THEN 'AB+'
                WHEN 5 THEN 'AB-'
                WHEN 6 THEN 'O+'
                ELSE 'O-'
            END,
            CONCAT('patient', i, '@healthcare.example.com'),
            CONCAT('555-', LPAD(FLOOR(RAND() * 10000), 4, '0')),
            CONCAT(FLOOR(RAND() * 9999), ' Medical Ave'),
            CASE (i % 10)
                WHEN 0 THEN 'New York'
                WHEN 1 THEN 'Los Angeles'
                WHEN 2 THEN 'Chicago'
                WHEN 3 THEN 'Houston'
                WHEN 4 THEN 'Phoenix'
                WHEN 5 THEN 'Philadelphia'
                WHEN 6 THEN 'San Antonio'
                WHEN 7 THEN 'San Diego'
                WHEN 8 THEN 'Dallas'
                ELSE 'San Jose'
            END,
            CASE (i % 10)
                WHEN 0 THEN 'NY'
                WHEN 1 THEN 'CA'
                WHEN 2 THEN 'IL'
                WHEN 3 THEN 'TX'
                WHEN 4 THEN 'AZ'
                WHEN 5 THEN 'PA'
                WHEN 6 THEN 'TX'
                WHEN 7 THEN 'CA'
                WHEN 8 THEN 'TX'
                ELSE 'CA'
            END,
            LPAD(FLOOR(10000 + RAND() * 89999), 5, '0'),
            CASE (i % 5)
                WHEN 0 THEN 'Blue Cross Blue Shield'
                WHEN 1 THEN 'UnitedHealthcare'
                WHEN 2 THEN 'Aetna'
                WHEN 3 THEN 'Cigna'
                ELSE 'Medicare'
            END
        );
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_patients();
DROP PROCEDURE insert_patients;

-- Table 2: Hospital Visits Fact
CREATE TABLE fact_visit (
    visit_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT,
    visit_date DATE NOT NULL,
    visit_type VARCHAR(50) NOT NULL,
    department VARCHAR(100),
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(255),
    attending_physician VARCHAR(100),
    visit_duration_minutes INT,
    visit_cost DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

-- Insert 4000 sample visits (reduced from 8000)
DELIMITER $
CREATE PROCEDURE insert_visits()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 4000 DO
        INSERT INTO fact_visit (
            patient_id, visit_date, visit_type, department, 
            diagnosis_code, diagnosis_description, attending_physician,
            visit_duration_minutes, visit_cost
        )
        VALUES (
            FLOOR(1 + RAND() * 750),  -- Random patient 1-750 (updated)
            DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND() * 730) DAY),  -- Last 2 years
            CASE FLOOR(RAND() * 5)
                WHEN 0 THEN 'Emergency'
                WHEN 1 THEN 'Outpatient'
                WHEN 2 THEN 'Inpatient'
                WHEN 3 THEN 'Surgery'
                ELSE 'Consultation'
            END,
            CASE FLOOR(RAND() * 8)
                WHEN 0 THEN 'Cardiology'
                WHEN 1 THEN 'Neurology'
                WHEN 2 THEN 'Orthopedics'
                WHEN 3 THEN 'Pediatrics'
                WHEN 4 THEN 'Oncology'
                WHEN 5 THEN 'Emergency'
                WHEN 6 THEN 'Surgery'
                ELSE 'General Medicine'
            END,
            CONCAT('ICD10-', CHAR(65 + FLOOR(RAND() * 26)), LPAD(FLOOR(RAND() * 999), 3, '0')),
            CASE FLOOR(RAND() * 10)
                WHEN 0 THEN 'Hypertension'
                WHEN 1 THEN 'Type 2 Diabetes'
                WHEN 2 THEN 'Acute Bronchitis'
                WHEN 3 THEN 'Migraine'
                WHEN 4 THEN 'Osteoarthritis'
                WHEN 5 THEN 'Anxiety Disorder'
                WHEN 6 THEN 'Coronary Artery Disease'
                WHEN 7 THEN 'Asthma'
                WHEN 8 THEN 'Depression'
                ELSE 'Routine Checkup'
            END,
            CONCAT('Dr. ', CHAR(65 + FLOOR(RAND() * 26)), '. ', 
                   CASE FLOOR(RAND() * 5)
                       WHEN 0 THEN 'Smith'
                       WHEN 1 THEN 'Johnson'
                       WHEN 2 THEN 'Williams'
                       WHEN 3 THEN 'Brown'
                       ELSE 'Davis'
                   END),
            FLOOR(15 + RAND() * 240),  -- 15-255 minutes
            ROUND(100 + RAND() * 5000, 2)  -- $100-$5100
        );
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_visits();
DROP PROCEDURE insert_visits;

-- Table 3: Prescription Records
CREATE TABLE fact_prescription (
    prescription_id INT AUTO_INCREMENT PRIMARY KEY,
    visit_id INT,
    patient_id INT,
    medication_name VARCHAR(255) NOT NULL,
    dosage VARCHAR(50),
    frequency VARCHAR(50),
    duration_days INT,
    prescribing_physician VARCHAR(100),
    pharmacy VARCHAR(100),
    prescription_date DATE NOT NULL,
    refills_remaining INT,
    cost DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (visit_id) REFERENCES fact_visit(visit_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id)
);

-- Insert 6000 sample prescriptions (reduced from 12000)
DELIMITER $
CREATE PROCEDURE insert_prescriptions()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE v_visit_id INT;
    DECLARE v_patient_id INT;
    DECLARE v_visit_date DATE;
    
    WHILE i <= 6000 DO
        -- Get random visit
        SELECT visit_id, patient_id, visit_date 
        INTO v_visit_id, v_patient_id, v_visit_date
        FROM fact_visit 
        ORDER BY RAND() 
        LIMIT 1;
        
        INSERT INTO fact_prescription (
            visit_id, patient_id, medication_name, dosage, frequency,
            duration_days, prescribing_physician, pharmacy, prescription_date,
            refills_remaining, cost
        )
        VALUES (
            v_visit_id,
            v_patient_id,
            CASE FLOOR(RAND() * 15)
                WHEN 0 THEN 'Lisinopril'
                WHEN 1 THEN 'Metformin'
                WHEN 2 THEN 'Amlodipine'
                WHEN 3 THEN 'Metoprolol'
                WHEN 4 THEN 'Omeprazole'
                WHEN 5 THEN 'Simvastatin'
                WHEN 6 THEN 'Losartan'
                WHEN 7 THEN 'Albuterol'
                WHEN 8 THEN 'Gabapentin'
                WHEN 9 THEN 'Hydrochlorothiazide'
                WHEN 10 THEN 'Sertraline'
                WHEN 11 THEN 'Ibuprofen'
                WHEN 12 THEN 'Levothyroxine'
                WHEN 13 THEN 'Atorvastatin'
                ELSE 'Amoxicillin'
            END,
            CONCAT(FLOOR(5 + RAND() * 500), 'mg'),
            CASE FLOOR(RAND() * 6)
                WHEN 0 THEN 'Once daily'
                WHEN 1 THEN 'Twice daily'
                WHEN 2 THEN 'Three times daily'
                WHEN 3 THEN 'Every 6 hours'
                WHEN 4 THEN 'Every 8 hours'
                ELSE 'As needed'
            END,
            FLOOR(7 + RAND() * 83),  -- 7-90 days
            CONCAT('Dr. ', CHAR(65 + FLOOR(RAND() * 26)), '. ', 
                   CASE FLOOR(RAND() * 5)
                       WHEN 0 THEN 'Smith'
                       WHEN 1 THEN 'Johnson'
                       WHEN 2 THEN 'Williams'
                       WHEN 3 THEN 'Brown'
                       ELSE 'Davis'
                   END),
            CASE FLOOR(RAND() * 5)
                WHEN 0 THEN 'CVS Pharmacy'
                WHEN 1 THEN 'Walgreens'
                WHEN 2 THEN 'Rite Aid'
                WHEN 3 THEN 'Walmart Pharmacy'
                ELSE 'Target Pharmacy'
            END,
            v_visit_date,
            FLOOR(RAND() * 6),  -- 0-5 refills
            ROUND(10 + RAND() * 290, 2)  -- $10-$300
        );
        SET i = i + 1;
    END WHILE;
END$
DELIMITER ;

CALL insert_prescriptions();
DROP PROCEDURE insert_prescriptions;

-- Table 4: Daily Healthcare Metrics
CREATE TABLE fact_daily_metrics (
    metric_date DATE PRIMARY KEY,
    total_visits INT NOT NULL,
    emergency_visits INT NOT NULL,
    total_patients_seen INT NOT NULL,
    avg_visit_duration_minutes DECIMAL(10, 2),
    total_revenue DECIMAL(12, 2) NOT NULL,
    prescriptions_written INT NOT NULL
);

-- Insert daily metrics for last 730 days
INSERT INTO fact_daily_metrics (
    metric_date, total_visits, emergency_visits, total_patients_seen,
    avg_visit_duration_minutes, total_revenue, prescriptions_written
)
SELECT 
    visit_date,
    COUNT(*),
    SUM(CASE WHEN visit_type = 'Emergency' THEN 1 ELSE 0 END),
    COUNT(DISTINCT patient_id),
    AVG(visit_duration_minutes),
    SUM(visit_cost),
    (SELECT COUNT(*) FROM fact_prescription WHERE prescription_date = visit_date)
FROM fact_visit
GROUP BY visit_date;

-- Create indexes for performance
CREATE INDEX idx_visit_patient ON fact_visit(patient_id);
CREATE INDEX idx_visit_date ON fact_visit(visit_date);
CREATE INDEX idx_prescription_visit ON fact_prescription(visit_id);
CREATE INDEX idx_prescription_patient ON fact_prescription(patient_id);
CREATE INDEX idx_prescription_date ON fact_prescription(prescription_date);
CREATE INDEX idx_patient_mrn ON dim_patient(medical_record_number);

SELECT '✅ Healthcare sample database created successfully!' AS status;
SELECT '📊 Tables created:' AS info;
SELECT '   - dim_patient (750 rows)' AS tables;
SELECT '   - fact_visit (4000 rows)' AS tables;
SELECT '   - fact_prescription (6000 rows)' AS tables;
SELECT '   - fact_daily_metrics (730 rows)' AS tables;
SELECT '🔗 Relationships:' AS info;
SELECT '   dim_patient → fact_visit (via patient_id)' AS relationships;
SELECT '   fact_visit → fact_prescription (via visit_id)' AS relationships;
SELECT '   dim_patient → fact_prescription (via patient_id)' AS relationships;
SELECT '🧪 Ready for OpenMetadata ingestion!' AS status;
