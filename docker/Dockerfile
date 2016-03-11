FROM alpine:3.3

MAINTAINER Priyanka Kukreja <priyankak@qubole.com>

# Installing Java
RUN sudo apt-get -y update && sudo apt-get -y install openjdk-7-jdk && export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH

# COPY Zeppelin Jars
RUN mkdir $HOME/incubator-zeppelin
COPY  incubator-zeppelin/interpreter $HOME/incubator-zeppelin
COPY incubator-zeppelin/bin $HOME/incubator-zeppelin
COPY incubator-zeppelin/conf $HOME/incubator-zeppelin
COPY incubator-zeppelin/notebook $HOME/incubator-zeppelin/example-notebook
RUN mkdir -p $HOME/incubator-zeppelin/zeppelin-web
COPY  incubator-zeppelin/zeppelin-web/target/zeppelin-web*.war $HOME/incubator-zeppelin/zeppelin-web/
#mkdir -p $HOME/incubator-zeppelin/zeppelin-docs
#COPY  incubator-zeppelin/zeppelin-docs/target/zeppelin-api-ui*.war $HOME/incubator-zeppelin/zeppelin-docs/
RUN mkdir -p $HOME/incubator-zeppelin/zeppelin-zengine/target/
RUN mkdir -p $HOME/incubator-zeppelin/zeppelin-server/target/
COPY incubator-zeppelin/zeppelin-zengine/target/lib $HOME/incubator-zeppelin/zeppelin-zengine/target/
COPY incubator-zeppelin/zeppelin-server/target/lib $HOME/incubator-zeppelin/zeppelin-server/target/
COPY incubator-zeppelin/zeppelin-server/target/*.jar $HOME/incubator-zeppelin/zeppelin-server/target/lib/
#COPY incubator-zeppelin/R $HOME/incubator-zeppelin
#COPY incubator-zeppelin/qubole_assembly/hustler $HOME/incubator-zeppelin

# Start Zeppelin Server
#sudo docker run -d --name=$3 -p 0.0.0.0:8080:8080 $1:$2 tail -f /dev/null

#sudo docker exec -it $3 /home/ubuntu/src/incubator-zeppelin/bin/zeppelin-daemon.sh start

