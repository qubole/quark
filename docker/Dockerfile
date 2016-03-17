FROM alpine:3.3

MAINTAINER Priyanka Kukreja <priyankak@qubole.com>

# Installing Java
RUN apk --update add openjdk8 && export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk && export PATH=$JAVA_HOME/bin:$PATH

# COPY Zeppelin Jars
RUN mkdir /home/incubator-zeppelin
COPY  incubator-zeppelin/interpreter /home/incubator-zeppelin/interpreter
COPY incubator-zeppelin/bin /home/incubator-zeppelin/bin
COPY incubator-zeppelin/conf /home/incubator-zeppelin/conf
COPY incubator-zeppelin/notebook /home/incubator-zeppelin/notebook
RUN mkdir -p /home/incubator-zeppelin/zeppelin-web
COPY  incubator-zeppelin/zeppelin-web/target  /home/incubator-zeppelin/zeppelin-web
RUN mkdir -p /home/incubator-zeppelin/zeppelin-zengine/target/
RUN mkdir -p /home/incubator-zeppelin/zeppelin-server/target/
COPY incubator-zeppelin/zeppelin-zengine/target/lib /home/incubator-zeppelin/zeppelin-zengine/target
COPY incubator-zeppelin/zeppelin-server/target/lib /home/incubator-zeppelin/zeppelin-server/target
COPY incubator-zeppelin/zeppelin-server/target /home/incubator-zeppelin/zeppelin-server/target/lib

WORKDIR /home/incubator-zeppelin/
CMD ZEPPELIN_CLASSPATH="/home/incubator-zeppelin/interpreter/jdbc/quark-jdbc-4.2.0.jar" bin/zeppelin-daemon.sh start
