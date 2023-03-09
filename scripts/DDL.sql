CREATE TABLE DDS.CATEGORY
(
category_id int NOT NULL, 
category_nm varchar(256) NOT NULL
)
STORED AS ORC
location 'dds/data/category'
; 

CREATE TABLE DDS.SUPPLIER
(
supplier_id int NOT NULL, 
supplier_nm varchar(256) NOT NULL
)
STORED AS ORC
location 'dds/data/supplier'
; 

CREATE TABLE DDS.SKU
(
sku_id int NOT NULL, 
category_id int NOT NULL,
sku_nm varchar(256) NOT NULL
)
STORED AS ORC
location 'dds/data/sku'
; 

CREATE TABLE DDS.SALES_POINT
(
sales_point_id int NOT NULL, 
sales_point varchar(256) NOT NULL,
city varchar(256) NOT NULL,
region varchar(256) NOT NULL,
address varchar(256) NOT NULL
)
STORED AS ORC
location 'dds/data/sales_point'
; 

CREATE TABLE DDS.SALES
(
year int NOT NULL, 
week int NOT NULL,
sales_point_id int NOT NULL,
supplier_id int NOT NULL,
category_id int NOT NULL,
sku_id int NOT NULL,
Sales int NOT NULL,
SlsQty_Colli int NOT NULL,
SlsQty_Pc int NOT NULL,
SlsQty_Kg int NOT NULL,
Price_Colli_N_Min int NOT NULL,
Price_NNBP_Colli_N_Min int NOT NULL
)
STORED AS ORC
location 'dds/data/sales'
; 
