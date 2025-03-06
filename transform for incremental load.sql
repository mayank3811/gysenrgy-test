# this is transform for incremental load
# we check how many values are already present and only incert vales that are new. This can be done after creating the main table using the code in mview_weekly_sales.

insert into `gsynergy-452808.gsynergy.mview_weekly_sales`
SELECT
  hp.sku_label,ft.sku_id,
  SUM(sales_units) sales_units,
  SUM(sales_dollars) sales_dollars,
  SUM(discount_dollars) discount_dollars, fsclwk_id
FROM
  `gsynergy-452808.gsynergy.fact-transactions` ft
JOIN
  `gsynergy-452808.gsynergy.hier-clnd` hc
ON
  ft.fscldt_id = hc.fscldt_id
JOIN
  `gsynergy-452808.gsynergy.hier-prod` hp
ON
  ft.sku_id = hp.sku_id
  where fsclwk_id not in (select distinct fsclwk_id from `gsynergy-452808.gsynergy.mview_weekly_sales`)
GROUP BY
  pos_site_id,
  ft.sku_id,
  fsclwk_id,
  price_substate_id,
  type,
  hp.sku_label
order by fsclwk_id
