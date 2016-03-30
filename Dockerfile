FROM golang:1.6

RUN apt-get update -qq
RUN apt-get install -qqy libsqlite3-dev
