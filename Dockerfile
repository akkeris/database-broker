FROM golang:1.11-alpine
RUN apk update
RUN apk add openssl ca-certificates git make
RUN wget https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 -O /usr/bin/dep
RUN chmod +x /usr/bin/dep
WORKDIR /go/src/github.com/akkeris/database-broker
COPY . .
RUN dep ensure
RUN make
RUN mkdir -p /opt/servicebroker/
RUN cp ./servicebroker /opt/servicebroker/servicebroker
WORKDIR /opt/servicebroker/
CMD /opt/servicebroker/servicebroker -insecure -logtostderr=1 -stderrthreshold 0 
