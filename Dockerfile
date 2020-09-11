FROM gradle:6.4-jdk11 AS build
COPY ./ .
RUN gradle build
