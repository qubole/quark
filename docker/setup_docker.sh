#!/bin/bash

# $1: image name
# $2: image version
# $3: name of the container

# Setup Java
sudo apt-get -y update && sudo apt-get -y install openjdk-7-jdk && export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH

# Setup Maven
sudo apt-get -y install wget && wget -P /$HOME http://a.mbbsindia.com/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && tar -xvf /$HOME/apache-maven-3.3.9-bin.tar.gz -C /$HOME/

# Install Zeppelin
sudo apt-get -y install git && git clone https://github.com/apache/incubator-zeppelin.git /$HOME/src/incubator-zeppelin

# Get Quark Jar
/$HOME/apache-maven-3.3.9/bin/mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get -DrepoUrl=-DrepoUrl=http://repo1.maven.org/maven2 -Dartifact=com.qubole:quark-jdbc:4.2.0

# Build Zeppelin
cd /$HOME/src/incubator-zeppelin
/$HOME/apache-maven-3.3.9/bin/mvn clean package -DskipTests

# Copy quark jar -- to current location, where dockerfile is!
sudo cp /$HOME/.m2/repository/com/qubole/quark-jdbc/4.2.0/quark-jdbc-4.2.0.jar /$HOME/src/incubator-zeppelin/interpreter/jdbc/

#Copy Zeppelin Jars to the folder containing Dockerfile

# Build the image
#sudo docker build -t $1:$2 .

# run with start zeppelin cmd -- take care container does not stop
#sudo docker run -d --name=$3 -p 0.0.0.0:8080:8080 $1:$2 tail -f /dev/null

#sudo docker exec -it $3 /home/ubuntu/src/incubator-zeppelin/bin/zeppelin-daemon.sh start


