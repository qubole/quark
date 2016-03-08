INSERT INTO `ds_sets` VALUES (1,'tpcds',1);
INSERT INTO `data_sources` VALUES (1,'H2','C57B50F43E7DD875EB469EAB259A6920E553F7DE7DA115DD640C44545659A6E8B0AB93C9E5A76B99A17D6D9A45E6E42E',1,0,'CANONICAL','JDBC',NULL,NULL,NULL),
(2,'H2','C57B50F43E7DD875EB469EAB259A69202424D633DAAE6B8711FAFE1BA3D1EFEBC182FDB1ADD8653EF17EF47F3B24A9F8C717530F41F320757B4AA1BFAF11C42E',1,0,'CUBES','JDBC',NULL,NULL,NULL),
(3,'H2','C57B50F43E7DD875EB469EAB259A69200E843F21C72177830D1C00082117C5C4C182FDB1ADD8653EF17EF47F3B24A9F8C717530F41F320757B4AA1BFAF11C42E',1,0,'VIEWS','JDBC',NULL,NULL,NULL);
INSERT INTO `jdbc_sources` VALUES (1,'C4AEED1962856B338AAB1E93E5706564','C717530F41F320757B4AA1BFAF11C42E'),
(2,'C4AEED1962856B338AAB1E93E5706564','C717530F41F320757B4AA1BFAF11C42E'),
(3,'C4AEED1962856B338AAB1E93E5706564','C717530F41F320757B4AA1BFAF11C42E');

INSERT INTO `cubes` VALUES (1,'web_returns_cubes','Web returns',0,'select 1 from canonical.public.web_returns as w join canonical.public.item as i on w.wr_item_sk = i.i_item_sk join canonical.public.customer as c on w.wr_refunded_cdemo_sk = c.c_customer_sk join canonical.public.date_dim as dd on w.wr_returned_date_sk = dd.d_date_sk join canonical.public.customer_demographics cd on c.c_current_cdemo_sk = cd.cd_demo_sk',2,'PUBLIC','WEB_RETURNS_CUBE','GROUPING__ID',1),
(2,'store_sales_cube','Store sales',0,'select 1 from canonical.public.store_sales as ss join canonical.public.item as i on ss.ss_item_sk = i.i_item_sk join canonical.public.customer as c on ss.ss_customer_sk = c.c_customer_sk join canonical.public.date_dim as dd on ss.ss_sold_date_sk = dd.d_date_sk join canonical.public.customer_demographics cd on ss.ss_cdemo_sk = cd.cd_demo_sk join canonical.public.promotion p on ss.ss_promo_sk = p.p_promo_sk join canonical.public.household_demographics hd on ss.ss_hdemo_sk = hd.hd_demo_sk join canonical.public.store s on ss.ss_store_sk = s.s_store_sk join canonical.public.time_dim td on ss.ss_sold_time_sk = td.t_time_sk',1,'PUBLIC','STORE_SALES_CUBE','GROUPING__ID',1),
(3,'store_returns_cube','Store Returns',0,'select 1 from canonical.public.store_returns as sr join canonical.public.date_dim as dd on sr.sr_returned_date_sk = dd.d_date_sk join canonical.public.item i on sr.sr_item_sk = i.i_item_sk join canonical.public.store st on sr.sr_store_sk = st.s_store_sk join canonical.public.customer c on sr.sr_customer_sk = c.c_customer_sk',1,'PUBLIC','STORE_RETURNS_COMPUTED','GROUPING__ID',1);

INSERT INTO `dimensions` VALUES (1,'Item Id',1,'','i','i_item_id','I_ITEM_ID',0,NULL),
(2,'Gender',1,'','cd','cd_gender','CD_GENDER',5,NULL),
(3,'Marital Status',1,'','cd','cd_marital_status','CD_MARITAL_STATUS',6,NULL),
(4,'Education Status',1,'','cd','cd_education_status','CD_EDUCATION_STATUS',7,NULL),
(5,'Year',1,'','dd','d_year','D_YEAR',1,NULL),
(6,'Quarter',1,'','dd','d_qoy','D_QOY',2,NULL),
(7,'Month',1,'','dd','d_moy','D_MOY',3,NULL),
(8,'Date',1,'','dd','d_date','D_DATE',4,NULL),
(9,'Item Id',2,'','i','i_item_id','I_ITEM_ID',0,NULL),
(10,'Year',2,'','dd','d_year','D_YEAR',2,NULL),
(11,'Quarter',2,'','dd','d_qoy','D_QOY',3,NULL),
(12,'Month',2,'','dd','d_moy','D_MOY',4,NULL),
(13,'Date',2,'','dd','d_date','D_DATE',5,NULL),
(14,'Customer Id',2,'','c','c_customer_id','C_CUSTOMER_ID',1,NULL),
(15,'Gender',2,'','cd','cd_gender','CD_GENDER',6,NULL),
(16,'Marital Status',2,'','cd','cd_marital_status','CD_MARITAL_STATUS',7,NULL),
(17,'Education Status',2,'','cd','cd_education_status','CD_EDUCATION_STATUS',8,NULL),
(18,'Year',3,'','dd','d_year','d_year',2,NULL),
(19,'Quarter',3,'','dd','d_qoy','d_qoy',3,NULL),
(20,'Month',3,'','dd','d_moy','d_moy',4,NULL),
(21,'Date',3,'','dd','d_date','d_date',5,NULL),
(22,'Item Id',3,'','i','i_item_id','i_item_id',6,NULL),
(23,'Store Id',3,'','st','s_store_id','s_store_id',7,NULL),
(24,'Store Name',3,'','st','s_store_name','s_store_name',8,NULL),
(25,'Customer Id',3,'','c','c_customer_id','c_customer_id',9,NULL),
(26,'Customer Key',3,'','sr','sr_customer_sk','sr_customer_sk',0,NULL),
(27,'Store Key',3,'','sr','sr_store_sk','sr_store_sk',1,NULL);

INSERT INTO `measures` VALUES (1,'Net Loss',1,'wr_net_loss','sum','TOTAL_NET_LOSS'),
(2,'Avergae Net Loss',1,'wr_net_loss','avg','AVERAGE_NET_LOSS'),
(3,'Min Return Amount',1,'wr_return_amt','min','MIN_RETURN_AMOUNT'),
(4,'Max Return Amount',1,'wr_return_amt','max','MAX_RETURN_AMOUNT'),
(5,'Web Page Count',1,'wr_web_page_sk','count','WEB_PAGE_COUNT'),
(6,'Total Extended Price',2,'ss_ext_sales_price','sum','sum_extended_sales_price'),
(7,'Total Sales Price',2,'ss_sales_price','sum','sum_sales_price'),
(8,'Total Net Profit',2,'ss_net_profit','sum','sum_net_profit'),
(9,'Total Wholesale Cost',2,'ss_wholesale_cost','sum','sum_wholesale_cost'),
(10,'Total Coupon Amt',2,'ss_coupon_amt','sum','sum_coupon_amt'),
(11,'Total List Price',2,'ss_list_price','sum','sum_list_price'),
(12,'Total Net Loss',3,'sr_net_loss','sum','total_net_loss'),
(13,'Total Return Amount',3,'sr_return_amt','sum','total_return_amt');


insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('warehouse_part', 'Warehouse Partition', 0,
    'select * from canonical.public.warehouse as wr where  wr.w_warehouse_sq_ft > 100',
    1, 3, 'PUBLIC', 'WAREHOUSE_PARTITION');
insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('web_sales_part', 'WebSales Partition', 0,
    'select * from canonical.public.web_sales as w where w.ws_quantity < 5 AND (w.ws_net_profit  > 2000 OR w.ws_wholesale_cost > 10000)',
    1, 3, 'PUBLIC', 'WEB_SALES_PARTITION');
insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('customer_address_part', 'Customer Address Partition', 0,
    'select * from canonical.public.customer_address as c where c.ca_street_name=''commercialstreet'' OR c.ca_zip = ''560073''',
    1, 3, 'PUBLIC', 'CUSTOMER_ADDRESS_PARTITION');
insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`, `destination_id`,
`schema_name`, `table_name`)
    VALUES('web_site_part', 'Web Site Partition', 0,
    'select web_site_sk, web_rec_start_date, web_county, web_tax_percentage from canonical.public.web_site where web_rec_start_date > ''2015-06-29'' AND ((web_county = ''en'') or (web_county = ''fr'') or (web_county = ''ja'') or (web_county = ''de'') or (web_county = ''ru''))',
    1, 3, 'PUBLIC', 'WEB_SITE_PARTITION');