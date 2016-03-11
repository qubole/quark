Installation
============
If you haven't already installed Java8 HOTSPOT and maven, please install them.

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
===============
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
=======================

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
===========
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