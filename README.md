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

[![Build Status](https://travis-ci.org/qubole/quark.svg)](https://travis-ci.org/qubole/quark)

Introduction
============

[![Join the chat at https://gitter.im/qubole/quark](https://badges.gitter.im/qubole/quark.svg)](https://gitter.im/qubole/quark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Quark (/ˈkwɔrk/ or /ˈkwɑrk/) simplifies and optimizes access to data for data analysts by 
managing relationships between tables across all databases in an organization. Quark defines 
materialized views and olap cubes, using them to route queries between tables stored in different 
databases. Materialized views simplify the management of cold and hot data between a data lake 
and a data warehouse. OLAP cubes optimize reports by rerouting queries to cubes stored in a fast 
database. Quark is distributed as a JDBC jar and will work with most tools that integrate through
 JDBC. 

Mailing List: quark-dev@googlegroups.com  
Subscribe: quark-dev+subscribe@googlegroups.com  
Unsubscribe: quark-dev+unsubscribe@googlegroups.com  
Gitter: https://gitter.im/qubole/quark  

Installation
============
If you haven't already installed Java8 HOTSPOT and maven, please install them.

Quick Start
===========
Install Quark JDBC from Source
-------------------------
    mkdir quark
    cd quark
    git clone git@bitbucket.org:qubole/quark.git quark-src
    cd quark-src
    mvn package
    ls -l quark-jdbc/target/quark-jdbc-*.jar
    cd -
    
Install Quark JDBC from Maven
-----------------------------
Find the latest version from [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Cquark-jdbc)

    mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get \
        -DrepoUrl=-DrepoUrl=http://repo1.maven.org/maven2 \
        -Dartifact=com.qubole:quark-jdbc:<version>

Get jar from mvn repository on the local drive `~/.m2/repositories/com/qubole/quark-jdbc`.
        
Configure Quark
---------------
Quark is configured through a JSON Model. The model definition is available in `JsonModel.md`

A simple example to access Redshift and Mysql is shown below. You can replace the `dataSources`
with any JDBC driver.

There are more examples in `examples/dataSources.json`

    {
      "dataSources":[
        {
          "type":"REDSHIFT",
          “url":"redshift_db_url",
          “username":"xyz",
          “password":"xyz",
          "name":"redshift",
          "default":"true",
          "factory":"com.qubole.quark.plugins.jdbc.JdbcFactory"
        },
        {
          "type":"MYSQL",
          "url”:"mysql_db_url",
          "username”:"xyz",
          "password”:"xyz",
          "name":"mysql",
          "default":"false",
          "factory":"com.qubole.quark.plugins.jdbc.JdbcFactory"
        }
       ],
    }

Connect through SQLLine
-----------------------

    # Copy jline-xxx.jar, sqlline-xxx.jar, and quark jdbc driver jar into directory.
    cd quark
    mkdir sqlline
    cd sqlline
    wget http://central.maven.org/maven2/sqlline/sqlline/1.1.9/sqlline-1.1.9.jar
    wget http://central.maven.org/maven2/jline/jline/2.13/jline-2.13.jar
    cp ../quark-src/quark-jdbc/target/quark-jdbc-*.jar .
    cd -
    
    #Start SQLLine

    java -classpath "/home/user/sqlLine/*" sqlline.SqlLine

    # Connect to Quark

    !connect jdbc:quark:model.json com.qubole.quark.jdbc.QuarkDriver

*Note: no password is required*

Run Queries
-----------
Quark organizes schemas and tables from each database in its own namespace. The name of the 
namespace is the same as the `name` attribute. Using the example 
above, all tables in `REDSHIFT` database are available as `redshift.<schema>.<table>`. Similarly,
 a table in `MYSQL` database is available as `mysql.<schema>.<table>`.

You can run SQL queries on these tables using SQLLine prompt.
 
Next Steps
==========
 
To really appreciate the utility of Quark, define relationships between tables in different 
databases. A table could be a materialized view or a cube that stores aggregations. In the 
`examples` directory, there are instructions to define relationships between tables in Hive (EMR) 
and Redshift or between SQL engines provided by Qubole. 


Developer Setup
===============
Quark now uses Qubole's fork of [incubator-calcite](https://github.com/qubole/incubator-calcite). 
Checkout calcite and install jars. The following code will install `1.5.0-qds-r6-SNAPSHOT`

    git clone git@github.com:qubole/incubator-calcite.git
    cd incubator-calcite
    git checkout qds-1.5
    mvn install -DskipTests
    
Checkout and compile Quark

Make changes in incubator-calcite
---------------------------------

You have to create a dev branch, change versions, develop and merge the changes once you are done.
 
    git checkout -b dev_branch qds-1.5
    change version to 1.5.0-qds-dev_branch in all pom.xml files. (We need to improve this)
    change version in quark-calcite/pom.xml
    
Once you merge your changes back to `qds-1.5`, bump up the versions in all pom.xml files.

Release a new version of Quark
==============================

Releases are always created from `master`. During development, `master` 
has a version like `X.Y.Z-SNAPSHOT`. 
 
To create a release, the version has to be changed, compile, deploy and 
bump the development version.
 
    # Change version as per http://semver.org/
    mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
    git commit -m "Prepare release X.Y.Z" -a
    git tag -a X.Y.Z -a "A useful comment here"
    git push
    git push --tags
    # SSH to build machine if required
    #Deploy to Artifactory
    mvn deploy
    #Set new development version.
    mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false
    git commit -m "Set Development Version to X.Y.(Z+1)" -a
 
