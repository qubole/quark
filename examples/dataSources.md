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

