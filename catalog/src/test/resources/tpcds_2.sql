insert into ds_sets(id, name, default_datasource_id) values(10, 'ds_10', 5);

insert into data_sources(name, type, url, ds_set_id, datasource_type) values ('CANONICAL', 'H2', '087B44B33A051FD17A7EF8D204093446', 10, 'JDBC');
insert into data_sources(name, type, url, ds_set_id, datasource_type) values ('CUBES', 'H2', '087B44B33A051FD17A7EF8D204093446', 10, 'JDBC');
insert into data_sources(name, type, url, ds_set_id, datasource_type) values ('VIEWS', 'H2', '087B44B33A051FD17A7EF8D204093446', 10, 'JDBC');
insert into data_sources(name, type, url, ds_set_id, datasource_type) values ('TEST_10', 'H2', '087B44B33A051FD17A7EF8D204093446', 10, 'QuboleDb');

insert into jdbc_sources(id, username, password) values(5, '96D37AB243836DBE4D804658D0862476', 'AA698221A5A25D0EDCAF50A3AD8EB6F4');
insert into jdbc_sources(id, username, password) values(6, '96D37AB243836DBE4D804658D0862476', 'AA698221A5A25D0EDCAF50A3AD8EB6F4');
insert into jdbc_sources(id, username, password) values(7, '96D37AB243836DBE4D804658D0862476', 'AA698221A5A25D0EDCAF50A3AD8EB6F4');
insert into quboledb_sources(id, auth_token, dbtap_id) values(8, '087B44B33A051FD17A7EF8D204093446', 1234);

insert into cubes(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`, `schema_name`,
`table_name`, `grouping_column`)
    VALUES('web_returns_cubes', 'Web returns', 0,
    'select 1 from canonical.public.web_returns as w join canonical.public.item as i on w.wr_item_sk = i.i_item_sk join canonical.public.customer as c on w.wr_refunded_cdemo_sk = c.c_customer_sk join canonical.public.date_dim as dd on w.wr_returned_date_sk = dd.d_date_sk join canonical.public.customer_demographics cd on c.c_current_cdemo_sk = cd.cd_demo_sk',
    10, 6, 'PUBLIC', 'WEB_RETURNS_CUBE', 'GROUPING__ID');

insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Item Id', 2, '', 'i', 'i_item_id', 'I_ITEM_ID', 0);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Gender', 2, '', 'cd', 'cd_gender', 'CD_GENDER', 5);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Marital Status', 2, '', 'cd', 'cd_marital_status', 'CD_MARITAL_STATUS', 6);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Education Status', 2, '', 'cd', 'cd_education_status', 'CD_EDUCATION_STATUS', 7);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Year', 2, '', 'dd', 'd_year', 'D_YEAR', 1);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Quarter', 2, '', 'dd', 'd_qoy', 'D_QOY', 2);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Month', 2, '', 'dd', 'd_moy', 'D_MOY', 3);
insert into dimensions(`name`, `cube_id`, `schema_name`, `table_name`, `column_name`, `cube_column_name`, `dimension_order`)
    values('Date', 2, '', 'dd', 'd_date', 'D_DATE', 4);


insert into measures(`name`, `cube_id`, `column_name`, `function`, `cube_column_name`)
    values ('Net Loss', 2, 'wr_net_loss', 'sum', 'TOTAL_NET_LOSS');
insert into measures(`name`, `cube_id`, `column_name`, `function`, `cube_column_name`)
    values ('Avergae Net Loss', 2, 'wr_net_loss', 'avg', 'AVERAGE_NET_LOSS');
insert into measures(`name`, `cube_id`, `column_name`, `function`, `cube_column_name`)
    values ('Min Return Amount', 2, 'wr_return_amt', 'min', 'MIN_RETURN_AMOUNT');
insert into measures(`name`, `cube_id`, `column_name`, `function`, `cube_column_name`)
    values ('Max Return Amount', 2, 'wr_return_amt', 'max', 'MAX_RETURN_AMOUNT');
insert into measures(`name`, `cube_id`, `column_name`, `function`, `cube_column_name`)
    values ('Web Page Count', 2, 'wr_web_page_sk', 'count', 'WEB_PAGE_COUNT');

insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('warehouse_part', 'Warehouse Partition', 0,
    'select * from canonical.public.warehouse as wr where  wr.w_warehouse_sq_ft > 100',
    10, 7, 'PUBLIC', 'WAREHOUSE_PARTITION');

insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('web_sales_part', 'WebSales Partition', 0,
    'select * from canonical.public.web_sales as w where w.ws_quantity < 5 AND (w.ws_net_profit  > 2000 OR w.ws_wholesale_cost > 10000)',
    10, 7, 'PUBLIC', 'WEB_SALES_PARTITION');


insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('customer_address_part_10', 'Customer Address Partition', 0,
    'select * from canonical.public.customer_address as c where c.ca_street_name=''commercialstreet'' OR c.ca_zip = ''560073''',
    10, 7, 'PUBLIC', 'CUSTOMER_ADDRESS_PARTITION');

