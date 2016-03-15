Qubole Hive
===========

    {
      "name":"QUBOLE",
      "token":"...",
      "factory":"com.qubole.quark.plugins.qubole.QuboleFactory",
      "QuboleDb":"HIVE",
      "endpoint":"https://api.qubole.com/api",
      "default":"true"
    }

Qubole DbTap
============
    {
      "name":"QUBOLE",
      "token":"...",
      "factory":"com.qubole.quark.plugins.qubole.QuboleFactory",
      "QuboleDb":"DBTAP",
      "dbtapid":"...",
      "endpoint":"https://api.qubole.com/api/",
      "default":"true"
    }


MySQL
=====

    {
      "type":"MYSQL",
      "url":"jdbc:mysql://url/database",
      "factory":"com.qubole.quark.plugins.jdbc.JdbcFactory",
      "username":"...",
      "password":"...",
      "name":"MYSQL",
      "default": "true"
    }

Redshift
========

    {
       "type":"REDSHIFT",
        “url":"redshift_db_url",
        “username":"xyz",
        “password":"xyz",
        "name":"redshift",
        "default":"true",
        "factory":"com.qubole.quark.plugins.jdbc.JdbcFactory"
    }

Oracle
======

    {
      "type":"ORACLE",
      "url":"jdbc:oracle:oci8:@...:1521:ORCL",
      "factory":"com.qubole.quark.plugins.jdbc.JdbcFactory",
      "username":"...",
      "password":"...",
      "name":"ORACLE",
      "default":"true"
    }

