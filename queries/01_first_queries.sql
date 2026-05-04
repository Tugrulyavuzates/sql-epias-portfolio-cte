--
--select * from public.piyasa_takas_fiyati ptf;
--
--select * from piyasa_takas_fiyati ptf limit 10;

--*** 01_basic_stats.sql
-- ***Business question : What's the price leven and range of Turkish
-- ***day ahead electricity over the last 2 years?


--select "PTF (TL/MWh)"
--from piyasa_takas_fiyati ptf 
--
--
--select column_name, data_type
--from information_schema.columns
--where table_name = 'piyasa_takas_fiyati'

--alter table piyasa_takas_fiyati 
--add column price_try numeric;
--
---- Populate it from the messy text column
--UPDATE piyasa_takas_fiyati
--SET price_try = CAST(REPLACE(REPLACE("PTF (TL/MWh)", '.', ''), ',', '.') AS NUMERIC);
--

--alter table piyasa_takas_fiyati 
--add column price_usd numeric;
--
---- Populate it from the messy text column
update piyasa_takas_fiyati
SET price_usd = CAST(REPLACE(REPLACE("PTF (USD/MWh)", '.', ''), ',', '.') AS NUMERIC);
--
--alter table piyasa_takas_fiyati 
--add column price_eur numeric;
--
---- Populate it from the messy text column
--UPDATE piyasa_takas_fiyati
--SET price_eur = CAST(REPLACE(REPLACE("PTF (EUR/MWh)", '.', ''), ',', '.') AS NUMERIC);

SELECT
    COUNT(*) AS total_hours,
    AVG(price_try ) AS mean_price
FROM piyasa_takas_fiyati;

select * from piyasa_takas_fiyati ptf 

select count(*) as total_rows,
count(price_try) as price_try_filled,
count(*) filter (where price_try is null) as price_try_nulls,
MIN(price_try) as min_price,
MAX(price_try) as max_price
from piyasa_takas_fiyati;

alter table piyasa_takas_fiyati add column ts timestamp;

update piyasa_takas_fiyati ptf 
set ts = TO_TIMESTAMP("Tarih" || ' ' || "Saat", 'DD.MM.YYYY HH24:MI');


select "Tarih", "Saat", ts, price_try
from piyasa_takas_fiyati
order by ts 

--What's the price level and range of Turkish day-ahead electricity over the last 2 years?


select count(*) from piyasa_takas_fiyati where price_try is not null 


select min(price_try), max(price_try), round(avg(price_try), 2) as avg_price, round(stddev(price_try), 2) as std
from piyasa_takas_fiyati as PTF
 

---zero price results made me suspicious, i need to check again


select * from piyasa_takas_fiyati






select count(*) as zero_price_hours
from piyasa_takas_fiyati 
where price_try = 0

--- only 7 zero hours, plausible for over-supply moments

--now to check max price to see if it's possible

select max(price_try) from piyasa_takas_fiyati ptf

--possibly a market cap *



select ts, price_try from piyasa_takas_fiyati
where price_try IN ((Select min(price_try) from piyasa_takas_fiyati), (select max(price_try) from piyasa_takas_fiyati))
order by price_try desc

--This gives me an idea on the price behaves during the day.

select piyasa_takas_fiyati.ts, round(avg(price_try), 2) as avg_price from piyasa_takas_fiyati
group by 1
order by 1, 2

--Level 1 to see distribution across two years hourly average

select "Saat", round(avg(price_try), 2) as avg_price from piyasa_takas_fiyati
group by 1
order by 1, 2

select COUNT(*), min(ts), max(ts), min("Saat") from piyasa_takas_fiyati
where price_try is not null



