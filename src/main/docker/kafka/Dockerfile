FROM wurstmeister/kafka:0.8.2.2

ADD start-kafka.sh /usr/bin/start-kafka.sh
RUN chmod a+x /usr/bin/start-kafka.sh

EXPOSE 9092

ENV KAFKA_ADVERTISED_HOST_NAME=kafka
ENV KAFKA_CREATE_TOPICS="some-topic:1:1"
