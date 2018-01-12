FROM golang

WORKDIR /

ENV GOPATH /home/jenkins/go
ENV PATH $PATH:$GOPATH/bin

RUN mkdir -p $GOPATH/bin

RUN apt-get -y update \
 && apt-get -y install default-jre-headless

RUN curl -s http://apache.hippo.nl/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | tar xvz

RUN echo "#!/bin/sh\n/kafka_2.11-0.11.0.0/bin/kafka-topics.sh \"\$@\"" > /usr/local/bin/kafka-topics \
 && chmod +x /usr/local/bin/kafka-topics
RUN echo "#!/bin/sh\n/kafka_2.11-0.11.0.0/bin/kafka-consumer-groups.sh \"\$@\"" > /usr/local/bin/kafka-consumer-groups \
 && chmod +x /usr/local/bin/kafka-consumer-groups

WORKDIR $GOPATH

RUN curl https://glide.sh/get | sh
RUN go get -u github.com/alecthomas/gometalinter
RUN go get -u github.com/DATA-DOG/godog/cmd/godog
RUN gometalinter --install --update
