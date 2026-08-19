-- Olist possui categorias planas. O self-link depth=0 mantém o contrato da bridge
-- e permite adicionar uma taxonomia externa sem alterar os fatos.
select
    category_key as ancestor_category_key,
    category_key as descendant_category_key,
    0 as depth,
    cast(1 as bit) as is_self
from {{ ref('dim_category') }}

