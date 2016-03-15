#!/bin/bash

# $1: image name
# $2: image version
# $3: name of the container

# Setup Java
sudo apt-get -y update && sudo apt-get -y install openjdk-7-jdk && export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH

# Setup Maven
sudo apt-get -y install wget && wget -P . http://a.mbbsindia.com/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && tar -xvf apache-maven-3.3.9-bin.tar.gz -C .

# Install Zeppelin
sudo apt-get -y install git && git clone https://github.com/apache/incubator-zeppelin.git ../incubator-zeppelin

# Get Quark Jar
apache-maven-3.3.9/bin/mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get -DrepoUrl=-DrepoUrl=http://repo1.maven.org/maven2 -Dartifact=com.qubole:quark-jdbc:4.2.0

# Build Zeppelin
apache-maven-3.3.9/bin/mvn -f ../incubator-zeppelin/pom.xml clean package -DskipTests

# Copy the zeppelin jars
mkdir incubator-zeppelin
cp -r ../incubator-zeppelin/interpreter ./incubator-zeppelin/interpreter
cp -r ../incubator-zeppelin/bin ./incubator-zeppelin/bin
cp -r ../incubator-zeppelin/conf ./incubator-zeppelin/conf
cp -r ../incubator-zeppelin/notebook ./incubator-zeppelin/notebook
mkdir -p ./incubator-zeppelin/zeppelin-web/target/
cp -r ../incubator-zeppelin/zeppelin-web/target/zeppelin-web*.war ./incubator-zeppelin/zeppelin-web/target/
mkdir -p ./incubator-zeppelin/zeppelin-zengine/target/lib
mkdir -p ./incubator-zeppelin/zeppelin-server/target/lib
cp -r ../incubator-zeppelin/zeppelin-zengine/target/lib ./incubator-zeppelin/zeppelin-zengine/target/lib
cp -r ../incubator-zeppelin/zeppelin-server/target/lib ./incubator-zeppelin/zeppelin-server/target/lib
#cp ../incubator-zeppelin/zeppelin-server/target/zeppelin-server-0.6.0-incubating-SNAPSHOT.jar ./incubator-zeppelin/zeppelin-server/target
cp -r ../incubator-zeppelin/zeppelin-server/target/*.jar ./incubator-zeppelin/zeppelin-server/target

# Copy quark jar 
sudo cp /$HOME/.m2/repository/com/qubole/quark-jdbc/4.2.0/quark-jdbc-4.2.0.jar ./incubator-zeppelin/interpreter/jdbc/

# Build the image
sudo docker build -t $1:$2 .

# run with start zeppelin cmd -- take care container does not stop
sudo docker run -d --name=$3 -p 0.0.0.0:8080:8080 $1:$2 tail -f /dev/null

sudo docker exec -it $3 apk add bash

sudo docker exec -it $3 /home/incubator-zeppelin/bin/zeppelin-daemon.sh start

