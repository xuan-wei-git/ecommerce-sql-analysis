SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;


-- ====================================================================================================
-- Load tables
-- ====================================================================================================

CREATE TABLE customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price DOUBLE,
    freight_value DOUBLE
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE order_payments (
    order_id TEXT,
    payment_sequential INT,
    payment_type TEXT,
    payment_installment INT,
    payment_value DOUBLE
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score DOUBLE,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);

-- Cell E77918 has \, affecting loading of data thus is cleaned
LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_order_reviews_dataset_cleaned.csv'
INTO TABLE order_reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_length INT,
    product_description_length INT,
    product_photo_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE sellers(
    seller_id TEXT,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE product_category_name_translation(
    product_category_name TEXT,
    product_category_name_english TEXT
);

LOAD DATA LOCAL INFILE 'C:/Users/Chew/Downloads/project/archive/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;




-- ====================================================================================================
-- 1. Inspect table structure
-- ====================================================================================================

DESCRIBE customers;
DESCRIBE order_items;
DESCRIBE order_payments;
DESCRIBE order_reviews;
DESCRIBE orders;
DESCRIBE product_category_name_translation;
DESCRIBE products;
DESCRIBE sellers;





-- ====================================================================================================
-- 2. Validate Primary Key Uniqueness
--    Ensure each table's primary key (or composite key) has no duplicate values
-- ====================================================================================================

-- Customers
SELECT customer_id, COUNT(*) AS cnt
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Order Items (composite key)
SELECT order_id, order_item_id, COUNT(*) AS cnt
FROM order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- Order Payments (composite key)
SELECT order_id, payment_sequential, COUNT(*) AS cnt
FROM order_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Order Reviews (composite key)
SELECT review_id, order_id, COUNT(*) AS cnt
FROM order_reviews
GROUP BY review_id, order_id
HAVING COUNT(*) > 1;

-- Orders
SELECT order_id, COUNT(*) AS cnt
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Category Translation
SELECT product_category_name, COUNT(*) AS cnt
FROM product_category_name_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1;

-- Products
SELECT product_id, COUNT(*) AS cnt
FROM products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Sellers
SELECT seller_id, COUNT(*) AS cnt
FROM sellers
GROUP BY seller_id
HAVING COUNT(*) > 1;





-- ====================================================================================================
-- 3. Detect Duplicate Rows and Repeated Records
-- ====================================================================================================

-- Order Items: check duplicate records
SELECT order_id, order_item_id, COUNT(*) AS cnt
FROM order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- Order Payments: check duplicate payment records
SELECT order_id, payment_sequential, COUNT(*) AS cnt
FROM order_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Order Reviews 1: Strict duplicate check (same review_id linked to same order)
-- Validates whether exact duplicate records exist
SELECT review_id, order_id, COUNT(*) AS cnt
FROM order_reviews
GROUP BY review_id, order_id
HAVING COUNT(*) > 1;
-- Order Reviews 2: Identify review_ids reused across multiple orders
-- Indicates that review_id is not a unique identifier
SELECT review_id, COUNT(*) AS review_count
FROM order_reviews
GROUP BY review_id
HAVING COUNT(*) > 1
ORDER BY review_count DESC;
-- Order Reviews 3: Check consistency of duplicated review_ids
-- Determines whether reused review_ids have identical or varying attributes
SELECT review_id,
       COUNT(*) AS row_count,
       COUNT(DISTINCT review_score) AS score_variation,
       COUNT(DISTINCT review_creation_date) AS date_variation
FROM order_reviews
GROUP BY review_id
HAVING COUNT(*) > 1;
-- Order Reviews 4: Summary check of identifier uniqueness
-- Confirms whether review_id alone is sufficient or requires order_id for uniqueness
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT review_id) AS unique_review_ids,
  COUNT(DISTINCT review_id, order_id) AS unique_review_order_pairs
FROM order_reviews;

-- Orders: check for duplicate timestamps per order
SELECT order_id, order_purchase_timestamp, COUNT(*) AS cnt
FROM orders
GROUP BY order_id, order_purchase_timestamp
HAVING COUNT(*) > 1;








-- ====================================================================================================
-- 4. Check missing values
-- ====================================================================================================

-- Customers
SELECT
  COUNT(*) AS total_rows,
  COUNT(customer_id) AS non_null_customer_id,
  COUNT(customer_unique_id) AS non_null_customer_unique_id
FROM customers;

-- Order Items
SELECT
  COUNT(*) AS total_rows,
  COUNT(order_id) AS non_null_order_id,
  COUNT(product_id) AS non_null_product_id,
  COUNT(seller_id) AS non_null_seller_id
FROM order_items;

-- Order Payments
SELECT
  COUNT(*) AS total_rows,
  COUNT(order_id) AS non_null_order_id,
  COUNT(payment_type) AS non_null_payment_type,
  COUNT(payment_value) AS non_null_payment_value
FROM order_payments;

-- Order Reviews
SELECT
  COUNT(*) AS total_rows,
  COUNT(review_id) AS non_null_review_id,
  COUNT(order_id) AS non_null_order_id
FROM order_reviews;

-- Orders
SELECT
  COUNT(*) AS total_rows,
  COUNT(order_id) AS non_null_order_id,
  COUNT(order_purchase_timestamp) AS non_null_order_purchase_timestamp,
  COUNT(order_delivered_customer_date) AS non_null_order_delivered_customer_date
FROM orders;

-- Identify missing critical timestamps by order status
SELECT order_status, COUNT(*) AS rows_with_nulls
FROM orders
WHERE order_delivered_customer_date IS NULL
GROUP BY order_status
ORDER BY rows_with_nulls DESC;

-- Supporting tables (basic completeness check)
SELECT COUNT(*) AS total_rows, COUNT(product_category_name) AS non_null_product_category_name
FROM product_category_name_translation;

SELECT COUNT(*) AS total_rows, COUNT(product_id) AS non_null_product_id
FROM products;

SELECT COUNT(*) AS total_rows, COUNT(seller_id) AS non_null_seller_id
FROM sellers;

-- Create cleaned orders dataset by retaining only valid delivered orders
CREATE TABLE orders_clean AS
SELECT *
FROM orders
WHERE order_status = 'delivered'
  AND order_approved_at IS NOT NULL
  AND order_delivered_carrier_date IS NOT NULL
  AND order_delivered_customer_date IS NOT NULL;





-- ====================================================================================================
-- 5. Clean String Columns (Trim & Handle Empty Values)
-- ====================================================================================================

-- Customers
UPDATE customers
SET customer_id = NULLIF(TRIM(customer_id), ''),
    customer_unique_id = NULLIF(TRIM(customer_unique_id), ''),
    customer_city = NULLIF(TRIM(customer_city), ''),
    customer_state = NULLIF(TRIM(customer_state), '');

-- Order Items
UPDATE order_items
SET order_id = NULLIF(TRIM(order_id), ''),
    product_id = NULLIF(TRIM(product_id), ''),
    seller_id = NULLIF(TRIM(seller_id), '');

-- Order Payments
UPDATE order_payments
SET order_id = NULLIF(TRIM(order_id), ''),
    payment_type = NULLIF(TRIM(payment_type), '');

-- Order Reviews
UPDATE order_reviews
SET review_id = NULLIF(TRIM(review_id), ''),
    order_id = NULLIF(TRIM(order_id), '');

-- Orders
UPDATE orders_clean
SET order_id = NULLIF(TRIM(order_id), ''),
    customer_id = NULLIF(TRIM(customer_id), ''),
    order_status = NULLIF(TRIM(order_status), ''),
    order_purchase_timestamp = NULLIF(TRIM(order_purchase_timestamp), ''),
    order_approved_at = NULLIF(TRIM(order_approved_at), ''),
    order_delivered_carrier_date = NULLIF(TRIM(order_delivered_carrier_date), ''),
    order_delivered_customer_date = NULLIF(TRIM(order_delivered_customer_date), ''),
    order_estimated_delivery_date = NULLIF(TRIM(order_estimated_delivery_date), '');

UPDATE products
SET product_id = NULLIF(TRIM(product_id), ''),
    product_category_name = NULLIF(TRIM(product_category_name), '');





-- ====================================================================================================
-- 6. Standardize Categorical Values
-- ====================================================================================================

SELECT DISTINCT payment_type
FROM order_payments;

SELECT *
FROM order_payments
WHERE payment_type = 'not_defined';

-- Payment type 'not_defined' are cancelled orders
SELECT *
FROM orders
WHERE order_id IN (
  SELECT order_id
  FROM order_payments
  WHERE payment_type = 'not_defined'
);

SELECT COUNT(DISTINCT product_category_name)
FROM product_category_name_translation;

SELECT COUNT(DISTINCT product_category_name)
FROM products;

-- Find the product category names not translated
SELECT DISTINCT p.product_category_name
FROM products p
LEFT JOIN product_category_name_translation t
ON p.product_category_name = t.product_category_name
WHERE t.product_category_name IS NULL;





-- ====================================================================================================
-- 7. Fix Data Types
-- ====================================================================================================

-- Order Items
ALTER TABLE order_items
MODIFY COLUMN price DECIMAL (10, 2),
MODIFY COLUMN freight_value DECIMAL (10, 2)
MODIFY COLUMN shipping_limit_date DATE;

-- Order Payments
ALTER TABLE order_payments
MODIFY COLUMN payment_value DECIMAL (10, 2);

-- Order reviews
ALTER TABLE order_reviews
MODIFY COLUMN review_score INT;

UPDATE order_reviews
SET review_creation_date = STR_TO_DATE(review_creation_date, '%e/%m/%Y %H:%i')
WHERE review_creation_date IS NOT NULL;

-- Orders
UPDATE orders_clean
SET order_purchase_timestamp = STR_TO_DATE(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
    order_approved_at = STR_TO_DATE(order_approved_at, '%Y-%m-%d %H:%i:%s'),
    order_delivered_carrier_date = STR_TO_DATE(order_delivered_carrier_date, '%Y-%m-%d %H:%i:%s'),
    order_delivered_customer_date = STR_TO_DATE(order_delivered_customer_date, '%Y-%m-%d %H:%i:%s'),
    order_estimated_delivery_date = STR_TO_DATE(order_estimated_delivery_date, '%Y-%m-%d %H:%i:%s');

ALTER TABLE orders_clean
MODIFY order_purchase_timestamp DATETIME,
MODIFY order_approved_at DATETIME,
MODIFY order_delivered_carrier_date DATETIME,
MODIFY order_delivered_customer_date DATETIME,
MODIFY order_estimated_delivery_date DATETIME;





-- ====================================================================================================
-- 8. Validate Numeric Ranges & Outliers
-- ====================================================================================================

-- Price distribution
SELECT MIN(price), MAX(price), AVG(price)
FROM order_items;

-- Inspect highest price values
SELECT *
FROM order_items
ORDER BY price DESC
LIMIT 20;

-- Check highest price product category
SELECT pct.product_category_name_english
FROM order_items oi
JOIN products p
ON oi.product_id = p.product_id
LEFT JOIN product_category_name_translation pct
ON p.product_category_name = pct.product_category_name
WHERE oi.price = (
    SELECT MAX(price)
    FROM order_items
);

-- Payment value distribution
SELECT MIN(payment_value), MAX(payment_value), AVG(payment_value)
FROM order_payments;

-- Check zero-value payments (negligible impact)
SELECT *
FROM order_payments
WHERE payment_value = 0;

-- Product attribute validation
SELECT
    MIN(product_name_length) AS min_name_len, MAX(product_name_length) AS max_name_len,
    MIN(product_description_length) AS min_desc_len, MAX(product_description_length) AS max_desc_len,
    MIN(product_photo_qty) AS min_photos, MAX(product_photo_qty) AS max_photos,
    MIN(product_weight_g) AS min_weight, MAX(product_weight_g) AS max_weight
FROM products;

-- Replace invalid zero values with NULL
UPDATE products
SET product_name_length = NULLIF(product_name_length, 0),
    product_description_length = NULLIF(product_description_length, 0),
    product_photo_qty = NULLIF(product_photo_qty, 0),
    product_weight_g = NULLIF(product_weight_g, 0),
    product_length_cm = NULLIF(product_length_cm, 0),
    product_height_cm = NULLIF(product_height_cm, 0),
    product_width_cm = NULLIF(product_width_cm, 0);





-- ====================================================================================================
-- 9. Validate date logic
-- ====================================================================================================

-- Identify inconsistent timestamps (retained but excluded in calculations)
SELECT *
FROM orders_clean
WHERE order_approved_at < order_purchase_timestamp
  OR  order_delivered_carrier_date < order_approved_at
  OR  order_delivered_carrier_date < order_purchase_timestamp
  OR  order_delivered_customer_date < order_delivered_carrier_date
  OR  order_estimated_delivery_date < order_approved_at;





-- ====================================================================================================
-- 10. Validate Foreign Key Integrity
-- ====================================================================================================

-- Orders without customers
SELECT COUNT(*)
FROM orders o
LEFT JOIN customers c
ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Order items with missing orders
SELECT COUNT(*)
FROM order_items oi
LEFT JOIN orders o
ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Payments without orders
SELECT COUNT(*)
FROM order_payments op
LEFT JOIN orders o
ON op.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Reviews without orders
SELECT COUNT(*)
FROM order_reviews r
LEFT JOIN orders o
ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Order items without products or sellers
SELECT COUNT(*)
FROM order_items oi
LEFT JOIN products p
ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT COUNT(*)
FROM order_items oi
LEFT JOIN sellers s
ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;





-- ====================================================================================================
-- 11. Feature Engineering (Generated Columns)
-- ====================================================================================================

-- Delivery-related metrics
ALTER TABLE orders_clean
ADD COLUMN order_fulfilment_days INT
GENERATED ALWAYS AS (
  CASE 
    WHEN order_delivered_carrier_date IS NULL OR order_purchase_timestamp IS NULL THEN NULL
    WHEN order_delivered_carrier_date < order_purchase_timestamp THEN NULL
    ELSE DATEDIFF(order_delivered_carrier_date, order_purchase_timestamp)
  END
) STORED;

ALTER TABLE orders_clean
ADD COLUMN delivery_time_days INT
GENERATED ALWAYS AS (
  CASE 
    WHEN order_delivered_customer_date IS NULL OR order_purchase_timestamp IS NULL THEN NULL
    WHEN order_delivered_customer_date < order_purchase_timestamp THEN NULL
    ELSE DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)
  END
) STORED;

ALTER TABLE orders_clean
ADD COLUMN delivery_delay_days INT
GENERATED ALWAYS AS (
  CASE 
    WHEN order_delivered_customer_date IS NULL OR order_estimated_delivery_date IS NULL THEN NULL
    ELSE DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)
  END
) STORED;

ALTER TABLE orders_clean
ADD COLUMN delivery_status VARCHAR(10)
GENERATED ALWAYS AS (
  CASE 
    WHEN delivery_delay_days IS NULL THEN 'Unknown'
    WHEN delivery_delay_days < 0 THEN 'Early'
    WHEN delivery_delay_days = 0 THEN 'On Time'
    WHEN delivery_delay_days > 0 THEN 'Late'
  END
) STORED;

ALTER TABLE orders_clean
ADD COLUMN is_late INT
GENERATED ALWAYS AS (
  CASE 
    WHEN delivery_delay_days IS NULL THEN NULL
    WHEN delivery_delay_days > 0 THEN 1
    ELSE 0
  END
) STORED;





-- ====================================================================================================
-- 12. Business Analysis Queries
-- ====================================================================================================


-- Total Revenue
SELECT SUM(price + freight_value) AS total_revenue
FROM order_items;

-- Revenue by product category
SELECT
  COALESCE(pct.product_category_name_english, p.product_category_name, 'unknown') AS category,
  SUM(oi.price + oi.freight_value) AS total_revenue
FROM order_items oi
JOIN products p
ON oi.product_id = p.product_id
LEFT JOIN product_category_name_translation pct
ON p.product_category_name = pct.product_category_name
GROUP BY category
ORDER BY total_revenue DESC;

-- Revenue by seller
SELECT s.seller_id, SUM(oi.price + oi.freight_value) AS total_sales
FROM order_items oi
JOIN sellers s
ON oi.seller_id = s.seller_id
GROUP BY s.seller_id
ORDER BY total_sales DESC;

-- Payment distribution
SELECT payment_type, COUNT(*) AS payment_count
FROM order_payments
GROUP BY payment_type
ORDER BY payment_count DESC;

-- Revenue by customer location
SELECT c.customer_state, SUM(oi.price + oi.freight_value) AS total_revenue
FROM order_items oi
JOIN orders_clean o
ON oi.order_id = o.order_id
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;



-- Average delivery time by seller
SELECT s.seller_id, AVG(oc.delivery_time_days) AS avg_delivery_days
FROM order_items oi
JOIN orders_clean oc
ON oi.order_id = oc.order_id
JOIN sellers s
ON oi.seller_id = s.seller_id
WHERE oc.delivery_time_days IS NOT NULL
GROUP BY s.seller_id
ORDER BY avg_delivery_days DESC;

-- Average delivery time by seller (filtered)
-- Excludes extreme outliers (>60 days) for more representative KPIs
SELECT s.seller_id, AVG(oc.delivery_time_days) AS avg_delivery_days
FROM order_items oi
JOIN (
  SELECT *,
    CASE 
      WHEN delivery_time_days > 60 THEN 1
      ELSE 0
    END AS is_outlier
  FROM orders_clean
) oc
  ON oi.order_id = oc.order_id
JOIN sellers s
  ON oi.seller_id = s.seller_id
WHERE 
  oc.delivery_time_days IS NOT NULL
  AND oc.is_outlier = 0
GROUP BY s.seller_id
ORDER BY avg_delivery_days DESC;

-- Delivery status count
SELECT DISTINCT delivery_status, COUNT(*) as cnt
FROM orders_clean
GROUP BY delivery_status
ORDER BY cnt DESC;

-- Percentage of deliveries late
SELECT ROUND(AVG(is_late) * 100, 2) AS late_delivery_pct
FROM orders_clean;

-- State-level delivery performance analysis
-- Identified significant regional disparities in delivery efficiency by comparing average delivery time and late rates.
-- Applied a minimum order threshold (>=100) to ensure statistical reliability and avoid noise from low-volume states.
-- Found that states such as AL, MA, and SE consistently exhibit the highest late delivery rates, indicating potential logistics inefficiencies or infrastructure constraints.
SELECT
  c.customer_state,
  COUNT(*) AS total_orders,
  AVG(oc.delivery_time_days) AS avg_delivery_days,
  SUM(oc.is_late) AS late_deliveries,
  SUM(oc.is_late) / COUNT(*) AS late_rate
FROM orders_clean oc
JOIN customers c
  ON oc.customer_id = c.customer_id
WHERE oc.delivery_time_days IS NOT NULL
GROUP BY c.customer_state
HAVING COUNT(*) >= 100
ORDER BY late_rate DESC;

-- City-level breakdown for high-risk states
-- Identified Maceió (AL) and São Luís (MA) as key drivers of late deliveries, with late rates exceeding 19% at moderate-to-high order volumes.
-- Observed that remote cities such as Manaus and Macapá had significantly longer delivery times (>26 days) but low late rates (<3%),
-- Distinguished between operational inefficiencies vs geographic constraints:
--   • High late rates + moderate/high volume → operational issues (actionable)
--   • Long delivery times + low late rates → expectation-adjusted regions (non-critical)
-- Enabled more precise targeting of logistics improvements at city-level rather than broad state-level assumptions.
SELECT
  c.customer_state,
  c.customer_city,
  COUNT(*) AS total_orders,
  AVG(oc.delivery_time_days) AS avg_delivery_days,
  SUM(oc.is_late) AS late_deliveries,
  SUM(oc.is_late) / COUNT(*) AS late_rate
FROM orders_clean oc
JOIN customers c
  ON oc.customer_id = c.customer_id
WHERE oc.delivery_time_days IS NOT NULL
  AND c.customer_state IN ('AL', 'MA', 'SE', 'AP', 'AM')
GROUP BY c.customer_state, c.customer_city
HAVING COUNT(*) >= 30
ORDER BY late_rate DESC;

-- Seller-level performance analysis within high-risk regions
-- Evaluated whether late deliveries are driven by specific sellers or broader regional issues.
-- Identified underperforming sellers with disproportionately high late rates relative to peers in the same state.
-- Introduced order volume threshold (>=15) to focus on relatively active sellers.
-- Insight supports targeted interventions (e.g., seller penalties or logistics partner review).
SELECT 
    s.seller_id,
    c.customer_state,
    COUNT(*) AS total_orders,
    AVG(oc.delivery_time_days) AS avg_delivery_days,
    SUM(oc.is_late) AS late_deliveries,
    SUM(oc.is_late) * 1.0 / COUNT(*) AS late_rate
FROM orders_clean oc
JOIN order_items oi ON oc.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
JOIN customers c ON oc.customer_id = c.customer_id
WHERE oc.delivery_time_days IS NOT NULL
  AND c.customer_state IN ('AL', 'MA', 'SE', 'AP', 'AM')
GROUP BY s.seller_id, c.customer_state
HAVING COUNT(*) >= 15
ORDER BY late_rate DESC;

-- Delivery time distribution analysis
-- Segmented orders into delivery time buckets to analyse performance patterns.
-- Found that the majority of orders fall within the 8–14 day range, indicating longer-than-expected delivery cycles.
-- Identified a sharp increase in late deliveries in the 15+ day bucket, suggesting a threshold where delays become highly likely.
-- Observed near-zero late deliveries in shorter delivery windows (0–7 days), indicating strong performance for fast shipments.
SELECT 
    CASE 
        WHEN delivery_time_days <= 3 THEN '0-3 days'
        WHEN delivery_time_days <= 7 THEN '4-7 days'
        WHEN delivery_time_days <= 14 THEN '8-14 days'
        ELSE '15+ days'
    END AS delivery_bucket,
    COUNT(*) AS total_orders,
    SUM(is_late) AS late_orders
FROM orders_clean
WHERE delivery_time_days IS NOT NULL
GROUP BY delivery_bucket
ORDER BY total_orders DESC;

-- Final Step: Measure each state's contribution to total late deliveries
-- This identifies which regions drive the largest share of overall delays

WITH state_late AS (
  SELECT 
    c.customer_state,
    SUM(oc.is_late) AS late_deliveries
  FROM orders_clean oc
  JOIN customers c
    ON oc.customer_id = c.customer_id
  GROUP BY c.customer_state
),
total_late AS (
  SELECT SUM(is_late) AS total_late_deliveries
  FROM orders_clean
)
SELECT 
  s.customer_state,
  s.late_deliveries,
  s.late_deliveries * 1.0 / t.total_late_deliveries AS contribution_rate
FROM state_late s
CROSS JOIN total_late t
ORDER BY contribution_rate DESC;


-- Revenue by state (customer segment)
SELECT
    c.customer_state,
    COUNT(DISTINCT oc.order_id) AS total_orders,
    SUM(oi.price + oi.freight_value) AS total_revenue,
    AVG(oi.price + oi.freight_value) AS avg_order_value
FROM orders_clean oc
JOIN order_items oi 
    ON oc.order_id = oi.order_id
JOIN customers c 
    ON oc.customer_id = c.customer_id
GROUP BY c.customer_state
HAVING COUNT(DISTINCT oc.order_id) >= 50
ORDER BY total_revenue DESC;

-- Revenue vs delivery performance by state
SELECT
    c.customer_state,
    SUM(oi.price + oi.freight_value) AS total_revenue,
    COUNT(DISTINCT oc.order_id) AS total_orders,
    AVG(oc.delivery_time_days) AS avg_delivery_days,
    SUM(oc.is_late) AS late_orders,
    SUM(oc.is_late) * 1.0 / COUNT(DISTINCT oc.order_id) AS late_rate
FROM orders_clean oc
JOIN order_items oi 
    ON oc.order_id = oi.order_id
JOIN customers c 
    ON oc.customer_id = c.customer_id
WHERE oc.delivery_time_days IS NOT NULL
GROUP BY c.customer_state
HAVING COUNT(DISTINCT oc.order_id) >= 50
ORDER BY total_revenue DESC;

-- High-revenue states impacted by delays
WITH state_revenue AS (
    SELECT
        c.customer_state,
        SUM(oi.price + oi.freight_value) AS total_revenue,
        COUNT(DISTINCT oc.order_id) AS total_orders,
        AVG(oc.delivery_time_days) AS avg_delivery_days,
        SUM(oc.is_late) AS late_orders,
        SUM(oc.is_late) * 1.0 / COUNT(DISTINCT oc.order_id) AS late_rate
    FROM orders_clean oc
    JOIN order_items oi 
        ON oc.order_id = oi.order_id
    JOIN customers c 
        ON oc.customer_id = c.customer_id
    WHERE oc.delivery_time_days IS NOT NULL
    GROUP BY c.customer_state
    HAVING COUNT(DISTINCT oc.order_id) >= 50
)
SELECT *
FROM state_revenue
WHERE total_revenue >= (
    SELECT PERCENTILE_CONT(0.75) 
    WITHIN GROUP (ORDER BY total_revenue)
    FROM state_revenue
)
ORDER BY total_revenue DESC;
-- High-revenue states using quartile ranking (compatible version)
WITH state_revenue AS (
    SELECT
        c.customer_state,
        SUM(oi.price + oi.freight_value) AS total_revenue,
        COUNT(DISTINCT oc.order_id) AS total_orders,
        AVG(oc.delivery_time_days) AS avg_delivery_days,
        SUM(oc.is_late) AS late_orders,
        SUM(oc.is_late) * 1.0 / COUNT(DISTINCT oc.order_id) AS late_rate
    FROM orders_clean oc
    JOIN order_items oi 
        ON oc.order_id = oi.order_id
    JOIN customers c 
        ON oc.customer_id = c.customer_id
    WHERE oc.delivery_time_days IS NOT NULL
    GROUP BY c.customer_state
    HAVING COUNT(DISTINCT oc.order_id) >= 50
),
ranked AS (
    SELECT *,
           NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile
    FROM state_revenue
)
SELECT *
FROM ranked
WHERE revenue_quartile = 1
ORDER BY total_revenue DESC;















-- ====================================================================================================
-- 13. Table to export for Python
-- ====================================================================================================

SELECT
  o.order_id,
  o.customer_id,
  -- timestamps
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  -- delivery features
  o.delivery_time_days,
  o.delivery_delay_days,
  o.order_fulfilment_days,
  o.delivery_status,
  o.is_late,
  -- order metrics
  SUM(oi.price) AS order_total,
  SUM(oi.freight_value) AS total_freight,
  COUNT(oi.product_id) AS items_count,
  COUNT(DISTINCT oi.seller_id) AS seller_count,
  -- payment (pre-aggregated)
  op.total_payment,
  op.payment_types_count,
  -- review (pre-aggregated)
  r.avg_review_score,
  -- customer info
  c.customer_zip_code_prefix
FROM orders_clean o
JOIN order_items oi 
ON o.order_id = oi.order_id
LEFT JOIN (
  SELECT order_id,
         SUM(payment_value) AS total_payment,
         COUNT(DISTINCT payment_type) AS payment_types_count
  FROM order_payments
  GROUP BY order_id
) op 
ON o.order_id = op.order_id
LEFT JOIN (
  SELECT order_id,
         AVG(review_score) AS avg_review_score
  FROM order_reviews
  GROUP BY order_id
) r 
ON o.order_id = r.order_id
LEFT JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY 
  o.order_id,
  o.customer_id,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  o.delivery_time_days,
  o.delivery_delay_days,
  o.order_fulfilment_days,
  o.delivery_status,
  o.is_late,
  op.total_payment,
  op.payment_types_count,
  r.avg_review_score,
  c.customer_zip_code_prefix;





SELECT
  o.order_id,
  o.customer_id,
  -- timestamps
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  -- delivery features
  o.delivery_time_days,
  o.delivery_delay_days,
  o.order_fulfilment_days,
  o.delivery_status,
  o.is_late,
  -- order metrics
  SUM(oi.price) AS order_total,
  SUM(oi.freight_value) AS total_freight,
  COUNT(oi.product_id) AS items_count,
  COUNT(DISTINCT oi.seller_id) AS seller_count,
  -- payment (pre-aggregated)
  op.total_payment,
  op.payment_types_count,
  -- review (pre-aggregated)
  r.avg_review_score,
  -- customer info
  c.customer_zip_code_prefix
FROM orders_clean o
JOIN order_items oi 
ON o.order_id = oi.order_id
LEFT JOIN (
  SELECT order_id,
         SUM(payment_value) AS total_payment,
         COUNT(DISTINCT payment_type) AS payment_types_count
  FROM order_payments
  GROUP BY order_id
) op 
ON o.order_id = op.order_id
LEFT JOIN (
  SELECT order_id,
         AVG(review_score) AS avg_review_score
  FROM order_reviews
  GROUP BY order_id
) r 
ON o.order_id = r.order_id
LEFT JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY 
  o.order_id,
  o.customer_id,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  o.delivery_time_days,
  o.delivery_delay_days,
  o.order_fulfilment_days,
  o.delivery_status,
  o.is_late,
  op.total_payment,
  op.payment_types_count,
  r.avg_review_score,
  c.customer_zip_code_prefix
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_final.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';