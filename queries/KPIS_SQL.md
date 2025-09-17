# Queries do Projeto

- Query principal indicadores de receita e transacões 

## Código SQL
```sql

CREATE OR REPLACE TEMPORARY VIEW dados_2025 AS

SELECT * 
FROM dataset_transacciones__2__parquet 
WHERE YEAR(transaction_date) = 2025;


WITH kpis_diarios AS (
SELECT 
'DIARIO' as granularidade,
 transaction_date as data_referencia,
YEAR(transaction_date) as ano,
MONTH(transaction_date) as mes,
DAY(transaction_date) as dia,
DAYOFWEEK(transaction_date) as dia_semana,
WEEKOFYEAR(transaction_date) as semana,
ROUND(SUM(transaction_fee), 2) as receita,
ROUND(SUM(product_amount), 2) as tpv,
ROUND((SUM(transaction_fee) / SUM(product_amount)) * 100, 3) as take_rate,
COUNT(DISTINCT user_id) as usuarios_ativos,
ROUND(SUM(product_amount) / COUNT(*), 2) as ticket_medio,
COUNT(*) as transacoes,
COUNT(DISTINCT merchant_name) as merchants_ativos,
ROUND(SUM(cashback), 2) as cashback,
NULL as crescimento_receita_pct,
NULL as crescimento_tpv_pct
    
FROM dados_2025

GROUP BY transaction_date, YEAR(transaction_date), MONTH(transaction_date), 
DAY(transaction_date), DAYOFWEEK(transaction_date), WEEKOFYEAR(transaction_date)
),

kpis_mensais AS (
SELECT 
'MENSAL' as granularidade,
LAST_DAY(CONCAT(YEAR(transaction_date), '-', LPAD(MONTH(transaction_date), 2, '0'), '-01')) as data_referencia,
YEAR(transaction_date) as ano,
MONTH(transaction_date) as mes,
NULL as dia,
NULL as dia_semana,
NULL as semana,
ROUND(SUM(transaction_fee), 2) as receita,
ROUND(SUM(product_amount), 2) as tpv,
ROUND((SUM(transaction_fee) / SUM(product_amount)) * 100, 3) as take_rate,
COUNT(DISTINCT user_id) as usuarios_ativos,
ROUND(SUM(product_amount) / COUNT(*), 2) as ticket_medio,
COUNT(*) as transacoes,
NULL as merchants_ativos,
NULL as cashback,
ROUND(
((SUM(transaction_fee) - LAG(SUM(transaction_fee)) OVER (ORDER BY YEAR(transaction_date), MONTH(transaction_date))) 
/ LAG(SUM(transaction_fee)) OVER (ORDER BY YEAR(transaction_date), MONTH(transaction_date))) * 100, 2
) as crescimento_receita_pct,
ROUND(
((SUM(product_amount) - LAG(SUM(product_amount)) OVER (ORDER BY YEAR(transaction_date), MONTH(transaction_date))) 
/ LAG(SUM(product_amount)) OVER (ORDER BY YEAR(transaction_date), MONTH(transaction_date))) * 100, 2
) as crescimento_tpv_pct
    
FROM dados_2025

GROUP BY YEAR(transaction_date), MONTH(transaction_date)
)


SELECT * FROM kpis_diarios
UNION ALL
SELECT * FROM kpis_mensais
ORDER BY granularidade, data_referencia
```

- Query de usuários e transacões por categoria

## Código SQL
```sql
SELECT 
YEAR(transaction_date) as ano,
MONTH(transaction_date) as mes,
CONCAT(YEAR(transaction_date), '-', LPAD(MONTH(transaction_date), 2, '0')) as ano_mes,
date_format(transaction_date, 'MMMM') as nome_mes,
 transaction_date as data_referencia
product_category,
ROUND(SUM(transaction_fee), 2) as receita_categoria,
ROUND(SUM(product_amount), 2) as volume_categoria,
COUNT(*) as transacoes_categoria,
COUNT(DISTINCT user_id) as usuarios_categoria,
ROUND(AVG(product_amount), 2) as ticket_medio_categoria,
ROUND((SUM(transaction_fee) / SUM(product_amount)) * 100, 3) as take_rate_categoria,
ROUND(
(SUM(transaction_fee) / SUM(SUM(transaction_fee)) OVER (PARTITION BY YEAR(transaction_date), MONTH(transaction_date))) * 100, 2
) as participacao_receita_pct,
ROW_NUMBER() OVER (
PARTITION BY YEAR(transaction_date), MONTH(transaction_date) 
ORDER BY SUM(transaction_fee) DESC
) as ranking_mes,
ROUND(
((SUM(transaction_fee) - LAG(SUM(transaction_fee)) OVER (PARTITION BY product_category ORDER BY YEAR(transaction_date), MONTH(transaction_date))) 
/ LAG(SUM(transaction_fee)) OVER (PARTITION BY product_category ORDER BY YEAR(transaction_date), MONTH(transaction_date))) * 100, 2
) as crescimento_categoria_pct

FROM dataset_transacciones__2__parquet

GROUP BY YEAR(transaction_date), MONTH(transaction_date), date_format(transaction_date, 'MMMM'), product_category

ORDER BY ano, mes, receita_categoria DESC
```


