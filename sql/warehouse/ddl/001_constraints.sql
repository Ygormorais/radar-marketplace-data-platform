-- Constraints informam o modelo semântico, mas não são enforced pelo Fabric Warehouse.
alter table gold.dim_date
add constraint pk_dim_date primary key nonclustered (date_key) not enforced;

alter table gold.dim_customer
add constraint pk_dim_customer primary key nonclustered (customer_key) not enforced;

alter table gold.dim_seller
add constraint pk_dim_seller primary key nonclustered (seller_key) not enforced;

alter table gold.dim_product
add constraint pk_dim_product primary key nonclustered (product_key) not enforced;

alter table gold.fct_order_item
add constraint pk_fct_order_item primary key nonclustered (order_item_key) not enforced;

alter table gold.fct_order_item
add constraint fk_order_item_customer foreign key (customer_key)
references gold.dim_customer (customer_key) not enforced;

alter table gold.fct_order_item
add constraint fk_order_item_seller foreign key (seller_key)
references gold.dim_seller (seller_key) not enforced;

alter table gold.fct_order_item
add constraint fk_order_item_product foreign key (product_key)
references gold.dim_product (product_key) not enforced;

