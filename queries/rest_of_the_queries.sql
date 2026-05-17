select extract(ISODOW from ts) as day_of_the_week, TO_CHAR(ts, 'DY') as to_char, ROUND(avg(price_try), 2) as mean_price
from piyasa_takas_fiyati
group by extract(ISODOW from ts), TO_CHAR(ts, 'DY')
order by extract(ISODOW from ts) 
-- Findings:
-- Weekend discount is asymmetric: Sunday averages 1,974 TL/MWh, 
-- ~19% below the weekday mean (~2,476). Saturday discount is mild 
-- (~6%), reflecting partial industrial activity. The weekly peak 
-- falls on Thursday (2,525 TL/MWh), not Friday — worth investigating 
-- in the demand-side data (industrial Thursday production peaks?).
-- 
-- Implication: weekend hedging strategies should treat Saturday and 
-- Sunday as separate regimes, not as a uniform "weekend" bucket. 
----- QUERY 4

select TO_CHAR(ts, 'YYYY-MM') as year_month,
count(*) as hourly_observations, 
avg(price_try) as mean_price,
min(price_try) min_price,
max(price_try) max_price,
round(stddev(price_try), 2) as "STD_DEV"
from piyasa_takas_fiyati
group by 1
order by 1 asc
-- Findings: Spring season is the toughest to forecast and price with april being the highest STDDEV which matches query 10 and 12 with number of cap binding and worst drawdown events
--- QUERY 5

with price_changes as(
select
ts,
EXTRACT(ISODOW from ts) as day_of_week,
price_try as current_price,
LAG(price_try) over (order by ts) as previous_price,
price_try - LAG(price_try) over (order by ts) as price_change

from piyasa_takas_fiyati
)
select * from price_changes
where previous_price is not null
order by ABS(price_change) desc
limit 50
/*
Biggest price jumps happen on morning ramp ups causing positive price jumps, sunday demand dips causes negative price jumps
For modelling day of the week and hour of the day interactions must be taken into account.
A model that looks fine on average might still be wildly wrong on the transition hours that actually matter for trading. 
The evaluation has to specifically check transition-hour accuracy, not just overall accuracy.
*/

--Edge case - 167 rows have an incomplete window. Postgres returns
-- a partial average over available rows.
--
---- This query: I accept the partial averages for the portfolio context.
-- The README documents this; downstream consumers can filter on row_num >= 168 
-- if they need strict full-window averages. In production I'd use the 
-- CASE WHEN version (below) which explicitly NULLs the early rows.
--Q6
with rolling_data as (
select
ts,
price_try as actual_price,
row_number() over (order by ts) as row_num,
AVG(price_try) over (order by ts rows between 167 preceding and current row) as raw_rolling_avg
from
piyasa_takas_fiyati

)
select
ts,
actual_price,
case when row_num >= 168 THEN round(raw_rolling_avg,2) else null end as smooth_7d_avg from rolling_data order by ts ASC



---Most expensive Hours per year

with yearly_ranks as (
select
extract(year from ts)::INT as year,
extract(month from ts)::INT as month,
extract(ISODOW from ts)::INT as day_of_week,
ts,
price_try as price,
rank() over (partition by extract(year from ts) order by price_try desc) as rank_in_year
from piyasa_takas_fiyati 
)
select
ts, year, month, day_of_week, price, rank_in_year
from yearly_ranks
order by rank_in_year ASC


/* Query 7 findings
 * This reveals that Turkey's day-ahead market hits a regulated price ceiling repeatedly. In 2024, dozens of hours
 * clear at exactly 3,000 tl/MWh; in 2025 the ceiling was raised to 3,400 TL/MWh
 * Price regularly peaks on summer evenings probably due to AC
 * autumn evenings
 * (1) Standard price forecasters trained on this data will struggle 
    at the upper tail; the cap creates an artificial ceiling.
(2) Hedging and storage strategies that depend on price spreads 
    need to model the cap explicitly — actual revenue from a battery 
    discharging during these hours is capped.
(3) Year-over-year comparisons of "peak prices" are misleading; the 
    ceiling moved between years.

For future projects: a derived "is_capped" flag may be useful as 
a feature, and a "shadow price" estimate (price that would have 
cleared without the cap) is a real piece of domain analysis worth
building. -------
 */




WITH quintile_data AS (
    SELECT 
        price_try,
        NTILE(5) OVER (ORDER BY price_try ASC) AS quintile
    FROM 
        piyasa_takas_fiyati
)

/*
 * minimum load bidding is observable here
 * quintile 1 have a lot more range than the others so that means the distribution is right-skewed
 * models need to account both for 0 hours and cap hours.
 * 
 */
SELECT 
    quintile,
    COUNT(*) AS num_hours,
    MIN(price_try) AS min_price,
    MAX(price_try) AS max_price,
    ROUND(AVG(price_try), 2) AS mean_price
FROM 
    quintile_data
GROUP BY 
    quintile
ORDER BY 
    quintile ASC;


-- Question 11: Data Quality: Missing Hours
-- Decision: Used a Recursive CTE to generate the expected calendar and an Anti-Join (LEFT JOIN + IS NULL) to find the gaps, as requested.

WITH RECURSIVE 
bounds AS (
    -- Step 1: Find the absolute start and end of our dataset
    SELECT 
        MIN(ts) AS min_ts, 
        MAX(ts) AS max_ts
    FROM 
        piyasa_takas_fiyati
),
expected_calendar AS (
    -- Step 2: The Anchor (Base Case)
    SELECT 
        min_ts AS expected_ts, 
        max_ts
    FROM 
        bounds
        
    UNION ALL
    
    -- Step 3: The Recursive Step (Add 1 hour iteratively)
    SELECT 
        expected_ts + INTERVAL '1 hour', 
        max_ts
    FROM 
        expected_calendar
    WHERE 
        expected_ts < max_ts
)
-- Step 4: The Anti-Join
SELECT 
    ec.expected_ts AS missing_hour
FROM 
    expected_calendar ec
LEFT JOIN 
    piyasa_takas_fiyati ptf 
ON 
    ec.expected_ts = ptf.ts
WHERE 
    ptf.ts IS NULL
ORDER BY 
    missing_hour ASC;

/*
Findings:
The dataset is complete: zero missing hours between 2024-04-27 and 
2025-04-27. This is unusual and useful — most production time-series 
datasets have gaps from API failures, DST transitions, or settlement 
holes. The clean coverage means downstream models can assume a 
uniform hourly grid without imputation.

We abandoned Daylight Saving Time in 2016 and remain 
on permanent UTC+3 year-round. This eliminates the spring-forward 
missing-hour and autumn-fallback duplicate-hour issues that plague 
European market data (EPEX, Nord Pool) and is a quiet advantage 
of working with EPİAŞ data for time-series modeling.

*/
-- 10_high_price_runs.sql
-- Question 10: Sustained High-Price Runs
-- Approach: Recursive CTE to walk consecutive extreme hours and assign each run a shared island_id.

WITH RECURSIVE high_prices AS (
    SELECT
        ts,
        price_try
    FROM piyasa_takas_fiyati
WHERE price_try > (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY price_try) FROM piyasa_takas_fiyati)-- Market cap
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

--- April 2025 contained all 51 high-price streaks in the dataset, including a 15-hour
--- stretch on April 16–17 where every hour cleared above the 95th percentile and
--- peaked at the regulatory cap (3,400 TL/MWh). Earlier months show no comparable
--- clustering, suggesting a regime shift — likely driven by hydro shortfall, fuel costs, or
--- unseasonal demand, but the specific cause would need generation-mix or weather
--- data to confirm. For any model trained on this dataset, April 2025 should be treated
--- as a distinct regime; a forecaster that averages across all months will systematically
--- underestimate spring-stress conditions in this market.

-- Q 12
--drawdown = price minus the running max up to that hour
-- I choose to use a window function because it's faster, although it's not noticeable in this db
-- For a bigger db we would notice the difference

with drawdowns as (
select ts, price_try as current_price, EXTRACT(ISODOW from TS) as day_of_week_,
MAX(price_try) over (order by ts rows between unbounded preceding and current row) as running_max,
(price_try - MAX(price_try) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) 
  / MAX(price_try) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
  * 100 AS drawdown_pct

from piyasa_takas_fiyati
)
select * from drawdowns
order by drawdown_pct asc
limit 20
/*
Findings:
The 20 worst drawdowns reaffirm the patterns from earlier queries: 
they happen on Sunday late-morning hours in spring (March 16 2025 
hit 0 TL/MWh for 4 consecutive hours, the largest single drawdown 
at -100% from the running peak). Weekend regime changes and 
renewable oversupply during low-demand Sunday hours drive the 
prices down aggressively. The intraday spread on these days (0 TL 
morning trough vs 3,000+ TL evening peak) is what makes battery 
storage economically attractive in this market.
*/
--