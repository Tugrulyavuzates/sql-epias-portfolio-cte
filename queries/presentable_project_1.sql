-- ==========================================
-- SETUP & CLEANING (Run once)
-- ==========================================
UPDATE piyasa_takas_fiyati
SET price_usd = CAST(REPLACE(REPLACE("PTF (USD/MWh)", '.', ''), ',', '.') AS NUMERIC);

-- ==========================================
-- ANALYTICAL QUERIES
-- ==========================================

-- Q1: Overall Price Level & Range 
SELECT 
    MIN(price_try) AS min_price, 
    MAX(price_try) AS max_price, 
    ROUND(AVG(price_try), 2) AS avg_price, 
    ROUND(STDDEV(price_try), 2) AS std_dev
FROM piyasa_takas_fiyati;
 
-- Q2: Zero-Price Hours Count
SELECT COUNT(*) AS zero_price_hours
FROM piyasa_takas_fiyati 
WHERE price_try = 0;

-- Q3: Hours of the Day Shape
SELECT 
    EXTRACT(hour FROM ts) AS hour_of_day, 
    ROUND(AVG(price_try), 2) AS mean_price, 
    COUNT(*) AS obs_count
FROM piyasa_takas_fiyati
GROUP BY EXTRACT(hour FROM ts)
ORDER BY EXTRACT(hour FROM ts);

-- Q4: Day of the Week Asymmetry
SELECT 
    EXTRACT(isodow FROM ts) AS day_of_week_num, 
    TO_CHAR(ts, 'DY') AS day_of_week_name, 
    ROUND(AVG(price_try), 2) AS mean_price
FROM piyasa_takas_fiyati
GROUP BY EXTRACT(isodow FROM ts), TO_CHAR(ts, 'DY')
ORDER BY EXTRACT(isodow FROM ts);

-- Q5: Monthly Volatility
SELECT 
    TO_CHAR(ts, 'YYYY-MM') AS year_month,
    COUNT(*) AS hourly_observations, 
    ROUND(AVG(price_try), 2) AS mean_price,
    MIN(price_try) AS min_price,
    MAX(price_try) AS max_price,
    ROUND(STDDEV(price_try), 2) AS std_dev
FROM piyasa_takas_fiyati
GROUP BY 1
ORDER BY 1 ASC;

-- Q6: Price Changes (Ramp ups/dips)
WITH price_changes AS (
    SELECT
        ts,
        EXTRACT(isodow FROM ts) AS day_of_week,
        price_try AS current_price,
        LAG(price_try) OVER (ORDER BY ts) AS previous_price,
        price_try - LAG(price_try) OVER (ORDER BY ts) AS price_change
    FROM piyasa_takas_fiyati
)
SELECT * FROM price_changes
WHERE previous_price IS NOT NULL
ORDER BY ABS(price_change) DESC
LIMIT 50;

-- Q7: 7-Day Rolling Average (Handling partial window edge cases)
WITH rolling_data AS (
    SELECT
        ts,
        price_try AS actual_price,
        ROW_NUMBER() OVER (ORDER BY ts) AS row_num,
        AVG(price_try) OVER (ORDER BY ts ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS raw_rolling_avg
    FROM piyasa_takas_fiyati
)
SELECT
    ts,
    actual_price,
    CASE 
        WHEN row_num >= 168 THEN ROUND(raw_rolling_avg, 2) 
        ELSE NULL 
    END AS smooth_7d_avg 
FROM rolling_data 
ORDER BY ts ASC;

-- Q8: Most Expensive Hours (Yearly Ranks)
WITH yearly_ranks AS (
    SELECT
        EXTRACT(year FROM ts)::INT AS year,
        EXTRACT(month FROM ts)::INT AS month,
        EXTRACT(isodow FROM ts)::INT AS day_of_week,
        ts,
        price_try AS price,
        RANK() OVER (PARTITION BY EXTRACT(year FROM ts) ORDER BY price_try DESC) AS rank_in_year
    FROM piyasa_takas_fiyati 
)
SELECT ts, year, month, day_of_week, price, rank_in_year
FROM yearly_ranks
ORDER BY rank_in_year ASC;

-- Q9: Price Distribution Quintiles
WITH quintile_data AS (
    SELECT 
        price_try,
        NTILE(5) OVER (ORDER BY price_try ASC) AS quintile
    FROM piyasa_takas_fiyati
)
SELECT 
    quintile,
    COUNT(*) AS num_hours,
    MIN(price_try) AS min_price,
    MAX(price_try) AS max_price,
    ROUND(AVG(price_try), 2) AS mean_price
FROM quintile_data
GROUP BY quintile
ORDER BY quintile ASC;

-- Q10: Sustained High-Price Runs (Recursive CTE)
WITH RECURSIVE high_prices AS (
    SELECT ts, price_try
    FROM piyasa_takas_fiyati
    WHERE price_try > (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY price_try) FROM piyasa_takas_fiyati)
),
island_starts AS (
    SELECT
        ts,
        price_try,
        ROW_NUMBER() OVER (ORDER BY ts) AS island_id
    FROM high_prices h1
    WHERE NOT EXISTS (
        SELECT 1
        FROM high_prices h2
        WHERE h2.ts = h1.ts - INTERVAL '1 hour'
    )
),
streak_builder AS (
    SELECT ts, price_try, island_id
    FROM island_starts

    UNION ALL

    SELECT h.ts, h.price_try, s.island_id
    FROM high_prices h
    INNER JOIN streak_builder s
        ON h.ts = s.ts + INTERVAL '1 hour'
)
SELECT
    MIN(ts) AS streak_start,
    MAX(ts) AS streak_end,
    COUNT(*) AS run_length_hours,
    MAX(price_try) AS max_price_during_streak,
    ROUND(AVG(price_try), 2) AS avg_price_during_streak
FROM streak_builder
GROUP BY island_id
ORDER BY run_length_hours DESC;

-- Q11: Data Quality Check - Missing Hours (Recursive CTE)
WITH RECURSIVE bounds AS (
    SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts
    FROM piyasa_takas_fiyati
),
expected_calendar AS (
    SELECT min_ts AS expected_ts, max_ts
    FROM bounds
        
    UNION ALL
    
    SELECT expected_ts + INTERVAL '1 hour', max_ts
    FROM expected_calendar
    WHERE expected_ts < max_ts
)
SELECT ec.expected_ts AS missing_hour
FROM expected_calendar ec
LEFT JOIN piyasa_takas_fiyati ptf 
    ON ec.expected_ts = ptf.ts
WHERE ptf.ts IS NULL
ORDER BY missing_hour ASC;

-- Q12: Worst Drawdowns (Corrected for Division by Zero)
WITH drawdowns AS (
    SELECT 
        ts, 
        price_try AS current_price, 
        EXTRACT(isodow FROM ts) AS day_of_week_,
        MAX(price_try) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max,
        (price_try - MAX(price_try) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) 
        / NULLIF(MAX(price_try) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) 
        * 100 AS drawdown_pct
    FROM piyasa_takas_fiyati
)
SELECT * FROM drawdowns
ORDER BY drawdown_pct ASC
LIMIT 20;