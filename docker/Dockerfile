FROM ubuntu:14.04

MAINTAINER Priyanka Kukreja <priyankak@qubole.com>

# Installing Java
RUN sudo apt-get -y update && sudo apt-get -y install openjdk-7-jdk && export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH

# Installing Maven
RUN sudo apt-get -y install wget && wget -P /home/ubuntu http://a.mbbsindia.com/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && tar -xvf /home/ubuntu/apache-maven-3.3.9-bin.tar.gz -C /home/ubuntu/

# Installing Zeppelin
RUN sudo apt-get -y install git && git clone https://github.com/apache/incubator-zeppelin.git /home/ubuntu/src/incubator-zeppelin

# Getting Quark Jar
RUN /home/ubuntu/apache-maven-3.3.9/bin/mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get -DrepoUrl=-DrepoUrl=http://repo1.maven.org/maven2 -Dartifact=com.qubole:quark-jdbc:4.2.0

# Building Zeppelin
WORKDIR /home/ubuntu/src/incubator-zeppelin
RUN /home/ubuntu/apache-maven-3.3.9/bin/mvn clean package -DskipTests

# Copy the Quark Jar
RUN cp /root/.m2/repository/com/qubole/quark-jdbc/4.2.0/quark-jdbc-4.2.0.jar /home/ubuntu/src/incubator-zeppelin/interpreter/jdbc/

# Start the Zeppelin Server 
WORKDIR /home/ubuntu/src/incubator-zeppelin/
CMD ZEPPELIN_CLASSPATH="/home/ubuntu/src/incubator-zeppelin/interpreter/jdbc/quark-jdbc-4.2.0.jar" bin/zeppelin-daemon.sh start
