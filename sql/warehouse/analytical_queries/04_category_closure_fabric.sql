-- Equivalente produtivo no Fabric: consulta a closure table pré-calculada em Spark/dbt.
select
    ancestor.category_name as ancestor_category,
    descendant.category_name as descendant_category,
    bridge.depth
from gold.bridge_category_hierarchy as bridge
inner join gold.dim_category as ancestor on bridge.ancestor_category_key = ancestor.category_key
inner join gold.dim_category as descendant on bridge.descendant_category_key = descendant.category_key;

