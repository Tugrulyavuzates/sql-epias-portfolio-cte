# sql-epias-portfolio-cte

# SQL Portfolio — EPİAŞ Day-Ahead Market

Window functions, CTEs, and recursive CTEs against 2 years of Turkish day-ahead electricity market clearing prices.

## Status
🏁 Completed 

## Key Business Insights

Asymmetric Weekend Regimes: Saturday demand largely mirrors weekday industrial activity, but Sunday mornings experience severe price drawdowns—often hitting 0 TL/MWh due to renewable oversupply against low demand. Hedging models must isolate Sundays rather than using a generic "weekend" bucket.

Spring Volatility & Regulatory Caps: April 2025 exhibited extreme volatility, including 51 consecutive hours where prices bound to the 3,400 TL/MWh regulatory cap. This regime shift—driven by the grid relying on expensive emergency peaker plants—means models trained on uniform annual data will systematically fail during spring-stress conditions.

Timezone Data Advantages: Because Turkey remains on permanent UTC+3 (no Daylight Saving Time), the EPİAŞ dataset has zero missing or duplicated hours. This eliminates the spring-forward/autumn-fallback imputation issues that plague European market data (EPEX, Nord Pool).
