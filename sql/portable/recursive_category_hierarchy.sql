-- Referência portátil para PostgreSQL/DuckDB. Fabric Warehouse não suporta CTE recursiva.
with recursive category_tree as (
    select category_id, parent_category_id, category_name, 0 as depth,
           cast(category_name as varchar) as category_path
    from category_hierarchy
    where parent_category_id is null

    union all

    select child.category_id, child.parent_category_id, child.category_name,
           parent.depth + 1,
           parent.category_path || ' > ' || child.category_name
    from category_hierarchy as child
    inner join category_tree as parent on child.parent_category_id = parent.category_id
)
select * from category_tree;

