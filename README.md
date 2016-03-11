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
[![Join the chat at https://gitter.im/qubole/quark](https://badges.gitter.im/qubole/quark.svg)](https://gitter.im/qubole/quark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Introduction
============

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

Documentation
=============
For a quick start guide and detailed documentation refer to [doc](http://qubole-quark.readthedocs.org/en/latest/)

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
    #Deploy to Maven Central
    mvn deploy -P release
    #Set new development version.
    mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false
    git commit -m "Set Development Version to X.Y.(Z+1)" -a
 
