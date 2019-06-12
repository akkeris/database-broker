FROM golang:1.12-alpine
RUN apk update
RUN apk add openssl ca-certificates git make build-base postgresql
WORKDIR /go/src/github.com/akkeris/database-broker
COPY . .
ENV GO111MODULE=on
RUN make
CMD ./servicebroker -insecure -logtostderr=1 -stderrthreshold 0 
