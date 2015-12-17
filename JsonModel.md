<!--
{% comment %}
  Copyright (c) 2015. Qubole Inc
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
{% endcomment %}
-->

Introduction
============
Metadata for Quark is represented with JSON. This file describes the structure of the JSON.

Elements
========

Root
----
    {
        version: '1.0',
        dataSources: [ DataSource... ],
        relSchema: { RelSchema ... }
    }
    
`version` (optional) if present should be equal to `1.0`.  
`dataSources` is a list of DataSource elements. Each element describes attributes about a 
`dataSource`.  
`relSchema` captures the relationships between tables in `DataSources`. `Cubes` and `Views` are 
supported right now.

DataSource
----------
Occurs within `root.dataSources`.

    {
        name:    'MYSQLDB'
        factory: 'com.qubole.quark.plugins.jdbc.JdbcFactory'
        url:     'jdbc://..../'
        default: 'true'
    }

`name` Name of the DataSource. `name` is used as the wrapper schema for all schemas and tables in
 this DataSource.   
`factory` Factory class to create DataSources. The class should implement 
`com.qubole.quark.DataSourceFactory`. Factories available out of the box are:  
   * `com.qubole.quark.plugins.jdbc.JdbcFactory` - Creates data sources that connect using a JDBC driver.
   * `com.qubole.quark.plugins.qubole.QuboleFactory` - Creates data sources that are hosted by QDS
`url` URL of the data source.  
`default` The default data source is used to determine the default schema.

JdbcDataSource
--------------
Like `DataSource` occurs within `root.DataSources`

    {
        type: 'MYSQL'
        username: 'user'
        password: 'pwd'
    }

`type` Type of database. Supported databases out of the box are:  
* EMR (Apache Hive on EMR)
* H2
* MYSQL
* REDSHIFT

`username` Username
`password` Password

QuboleDataSource
----------------
TODO

RelSchema
=========
Contains relationships between tables. The tables may be hosted in different data sources. Two 
types of relationships are supported.

    {
        views: [View ... ]
        cubes: [Cube ... ]
    }

`views` Describes materialized views on a table in one of the data sources. The materialized view
 maybe stored in a different data source.  
`cubes` Describes cubes generated on star schema join among tables in one of the data sources. 
The cube maybe stored in a different data source.     
    
Views
-----
Occurs in `root.relSchema`. Similar to materialized views in databases.

    {
        name: 'warehouse_big`
        query: 'select * from hive.tpcds.warehouse as wr where wr.w_warehouse_sq_ft > 100'
        destination: 'VIEWS'
        schema: 'PUBLIC'
        table: 'WAREHOUSE_PARTITION'
    }
    
`name` Name of the view.  
`query` Query that describes the materialized view.  
`destination` Data source where the materialized view is stored.  
`schema` Schema of the table where the materialized view is stored.
`table`  Name of the table where the materialized view is stored.
    
Cubes
-----
Occurs in `root.relSchema`.
    
    {
        name: 'web_returns_cube`
        query: 'select 1 from canonical.public.web_returns as w join canonical.public.item ...'
        destination: 'CUBES'
        schema: 'PUBLIC'
        table: 'WEB_RETURNS_CUBE'
        groupingColumn: 'GROUPING__ID'
        dimensions: [Dimension ...]
        measures: [Measure ...]
        groups:   [Group ...]
    }
    
`name` Name of the cube.  
`query` Query that describes the cube.  
`destination` Data source where the cube is stored.  
`schema` Schema of the table where the cube is stored.
`table`  Name of the table where the cube is stored.
`groupingColumn` Column that stores the number corresponding to the GROUPING bit vector 
associated with the row. 

Dimension
---------
Occurs in `root.relSchema.cubes`.

    {
        schema: '',
        table: 'i'
        column: 'i_item_id',
        cubeColumn: 'I_ITEM_ID',
        dimensionOrder: 0,
        name: 'Item Id',
        parent: null,
    }

`schema_name` Schema of the source table.  
`table_name`  Table name or alias of the source table.  
`column_name` Column name in the source table.  
`cube_column_name` Column in the cube table.  
`dimension_order` Ordinal number in the dimension list.  
`name` A descriptive name for the dimension. It should be unique. The name is used to identify 
parents and children in a dimension hierarchy.    
`parent` Cube Column Name of the parent dimension if its part of a hierarchy.  
     
Measure
-------
Occurs in `root.relSchema.cubes`
     
    {
        column: 'wr_net_loss',
        cubeColumn: 'TOTAL_NET_LOSS',
        function: 'sum',
    }
    
`column` Name of the column in the fact table.
`cubeColumn` Name of the column in the cube table.
`function` Aggregate function on the column in the fact table. Supported 
      
Group
-----
Occurs in `root.relSchema.cubes` 
   
    