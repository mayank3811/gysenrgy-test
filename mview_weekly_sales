# big query query
# this query was used to create the initial table. This query creates the required table and laso loads values in the table.
# the colum sku_label is added to identify which label corrosponds to which data and sku_id is the forign key for the table.
# fsclwk_id column can be used as a mark to identify how many weeks data is loaded.

CREATE OR REPLACE TABLE
  `gsynergy-452808.gsynergy.mview_weekly_sales` AS
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
GROUP BY
  pos_site_id,
  ft.sku_id,
  fsclwk_id,
  price_substate_id,
  type,
  hp.sku_label
order by fsclwk_id
