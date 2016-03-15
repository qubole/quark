Prerequisites
=============
* [Start](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-hive.html) an EMR
 cluster with Apache Hive installed.
* [Setup](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-dev-create-metastore-outside.html) a Hive Metastore on AWS RDS.


Load data in Hive
=================
Execute the SQL in the file 
[create-hive-tables.sql](https://github.com/qubole/quark/blob/master/docs/examples/create-hive-tables.sql) 
on your Apache Hive Cluster.
      
      
Load data into Redshift
=======================

Run the following command in psql:

    create table store_sales_cube(
      d_year INT, 
      d_moy INT, 
      d_qoy INT, 
      d_date timestamp, 
      cd_gender varchar(10), 
      cd_marital_status varchar(10), 
      cd_education_status varchar(50), 
      s_store_id varchar(100), 
      s_store_name varchar(100), 
      grouping_id varchar(100), 
      sum_wholesale_cost double precision, 
      sum_list_price double precision, 
      sum_sales_price double precision, 
      sum_extended_price double precision, 
      sum_coupon_amt double precision, 
      sum_net_profit double precision);

    create table store_sales_partition(
      ss_sold_date_sk           int,
      ss_sold_time_sk           int,
      ss_item_sk                int,
      ss_customer_sk            int,
      ss_cdemo_sk               int,
      ss_hdemo_sk               int,
      ss_addr_sk                int,
      ss_store_sk               int,
      ss_promo_sk               int,
      ss_ticket_number          int,
      ss_quantity               int,
      ss_wholesale_cost         float,
      ss_list_price             float,
      ss_sales_price            float,
      ss_ext_discount_amt       float,
      ss_ext_sales_price        float,
      ss_ext_wholesale_cost     float,
      ss_ext_list_price         float,
      ss_ext_tax                float,
      ss_coupon_amt             float,
      ss_net_paid               float,
      ss_net_paid_inc_tax       float,
      ss_net_profit             float);
      
Load data into this table using the following command:

    copy store_sales_cube
      from 's3://public-qubole/datasets/quark_example/store_sales_cube.csv'
      credentials 'aws_access_key_id=****;aws_secret_access_key=****'; 

    copy store_sales_partition
      from 's3://public-qubole/datasets/quark-example/store_sales_partition/'
      credentials 'aws_access_key_id=***;aws_secret_access_key=***' csv

Set up SQLLine
==============

Copy jline-xxx.jar, sqlline-xxx.jar, and quark jdbc driver jar into a directory, for the 
purpose of this example, the directory name is "sqlLine"

    cd quark
    mkdir sqlline
    cd sqlline
    wget http://central.maven.org/maven2/sqlline/sqlline/1.1.9/sqlline-1.1.9.jar
    wget http://central.maven.org/maven2/jline/jline/2.13/jline-2.13.jar

Install quark jdbc driver jar
-----------------------------

    mkdir quark
    cd quark
    git clone git@bitbucket.org:qubole/quark.git quark-src
    cd quark-src
    mvn install -DskipTests

copy the jar: quark-jdbc/target/quark-jdbc-*.jar to our sqlLine directory.

Install Hive JDBC jars
----------------------

    cd sqlline
    wget https://amazon-odbc-jdbc-drivers.s3.amazonaws.com/public/AmazonHiveJDBC_1.0.4.1004.zip
    unzip AmazonHiveJDBC_1.0.4.1004.zip
    cd AmazonHiveJDBC_1.0.4.1004
    
For more information refer to the [EMR documentation](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/HiveJDBCDriver.html)

Install Postgres JDBC jar
-------------------------

    cd sqlline
    wget https://jdbc.postgresql.org/download/postgresql-9.4-1206-jdbc42.jar    

Install MySQL Jar
-----------------
Visit the MySQL JDBC [download page](http://dev.mysql.com/downloads/connector/j/). Download the
jar and copy it to the `sqlline` directory.

Quark JDBC Jar Configuration
============================
Edit [quark-config.json](https://github.com/qubole/quark/blob/master/docs/examples/quark-config.json)
 and fill in credentials for AWS RedShift and Apache Hive in `dataSources` array. Store the file 
 in a location accessible by the Quark JDBC driver.

Start up Sqlline
================

    java -Djava.ext.dirs=/home/user/sqlLine/ sqlline.SqlLine
    !connect jdbc:quark:quark-config.json com.qubole.quark.jdbc.QuarkDriver

Views
=====
In the above example AWS Redshift contains the view `store_sales_partition` for the table 
`tpcds_orc_500.store_sales`. It described by the following WHERE clause: 
    
    ss_sold_date_sk >= 2452640 and ss_customer_sk > 3 and ss_customer_sk < 20

Quark redirects the following query to the view in AWS Redshift even though the table in the sql 
query `hive.tpcds_orc_500.store_sales` is a Apache Hive table.

    select * from hive.tpcds_orc_500.store_sales where ss_sold_date_sk >= 2452640 and ss_customer_sk > 3 and ss_customer_sk < 20

Cubes
=====
The cube setup in this example is described in more detail in the [CUBE blog post.](https://www.qubole.com/blog/product/cube-keyword-in-apache-hive/)
Redshift contains the cube table for a star-schema join on the fact table store_sales.

Queries:

    select d_year, d_qoy, sum(ss_sales_price)  as sales 
    from hive.tpcds_orc_500.store_sales_2002_plus 
      join hive.tpcds_orc_500.date_dim on ss_sold_date_sk = d_date_sk 
    group by d_year, d_qoy;

The above query runs on AWS Redshift and returns the following rows: 

    +-------------+-------------+---------------------------+
    |   d_year    |    d_qoy    |           sales           |
    +-------------+-------------+---------------------------+
    | 2002        | 1           | 1.43591514E9              |
    | 2003        | 1           | 1.02997072E8              |
    | 2002        | 2           | 1.36243994E9              |
    | 2002        | 3           | 2.52206029E9              |
    | 2002        | 4           | 4.13493248E9              |
    +-------------+-------------+---------------------------+


The following query however cannot be answered by the cube in RedShift and is executed on Hive.

    select d_year, d_dom, sum(ss_sales_price) 
    from hive.tpcds_orc_500.store_sales_2002_plus
    join hive.tpcds_orc_500.date_dim on ss_sold_date_sk = d_date_sk group by d_year, d_dom;

