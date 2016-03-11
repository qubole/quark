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
 
