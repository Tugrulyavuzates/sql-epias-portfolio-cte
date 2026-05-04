-- 02_hour_of_day.sql
-- Business question: What does the daily price shape look like?
-- Solar pushes prices down midday; evening peak demand pushes them up.
-- The shape of the curve drives bidding strategies for both producers
-- and consumers.

select extract(hour from ts) as hours_of_the_day, round(avg(price_try), 2), count(*)
from piyasa_takas_fiyati
group by extract(hour from ts)
order by extract(hour from ts)

-- Findings:
-- Daily price shape shows the classic solar-influenced pattern:
-- midday dip at hour 12 (1,580 TL/MWh, ~25% below daily mean) driven 
-- by solar generation suppressing prices, and evening peak at hour 19 
-- (2,883 TL/MWh) driven by demand ramp-up after sunset.
-- 
-- Data note: 366 observations per hour suggests ~1 year of data, not 2.
-- Worth verifying date range against intended pull.