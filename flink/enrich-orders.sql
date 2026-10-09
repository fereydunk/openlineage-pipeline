-- Flink SQL: derive `orders-enriched` from `db2-orders`.
-- CREATE TABLE AS SELECT (CTAS) creates the target topic + its schema from the
-- projection, so we don't pre-create orders-enriched and we avoid the implicit
-- `key BYTES` column mismatch that a bare INSERT INTO an existing table hits.
--
-- Column names are UPPERCASE because they come from Db2 column names via the
-- Avro converter. Adds a RISK_TIER derived from AMOUNT_USD.
CREATE TABLE `orders-enriched` AS
SELECT
    ORDER_ID,
    CUSTOMER,
    ITEM,
    QUANTITY,
    AMOUNT_USD,
    CASE
        WHEN AMOUNT_USD > 2000 THEN 'HIGH'
        WHEN AMOUNT_USD > 500  THEN 'MEDIUM'
        ELSE 'LOW'
    END AS RISK_TIER
FROM `db2-orders`;
