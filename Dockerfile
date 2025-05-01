FROM openjdk:11-jdk-slim

ENV ZOOKEEPER_VERSION=3.9.3

# Zookeeper 다운로드 및 설치
RUN apt-get update && apt-get install -y wget bash \
    && wget https://downloads.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz \
    && tar -xzf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz \
    && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /usr/local/zookeeper \
    && mkdir -p /var/lib/zookeeper

# 설정파일 zoo.cfg 작성
RUN echo "tickTime=2000" > /usr/local/zookeeper/conf/zoo.cfg && \
    echo "dataDir=/var/lib/zookeeper" >> /usr/local/zookeeper/conf/zoo.cfg && \
    echo "clientPort=2181" >> /usr/local/zookeeper/conf/zoo.cfg && \
    # 4-letter-word 명령 허용
    echo "4lw.commands.whitelist=*" >> /usr/local/zookeeper/conf/zoo.cfg

ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

EXPOSE 2181

# 컨테이너가 실행되면 Zookeeper를 실행
CMD ["/usr/local/zookeeper/bin/zkServer.sh", "start-foreground"]