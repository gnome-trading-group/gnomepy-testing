# Multi-stage Dockerfile for running both Java and Python market data clients

FROM maven:3.9.4-eclipse-temurin-17 AS java-base

WORKDIR /app/java

ARG GITHUB_ACTOR
ARG GITHUB_TOKEN

RUN mkdir -p /root/.m2
# When testing locally, make sure to copy the m2 folder into the local repo's on any local updates
# Within the current directory:
# $ cp -r ~/.m2 .
COPY .m2 /root/.m2

COPY docker/pom.xml /app/java/pom.xml
COPY docker/settings.xml /app/java/settings.xml
RUN mvn dependency:copy-dependencies -DoutputDirectory=/app/java/lib

FROM --platform=linux/amd64 python:3.13-slim

RUN apt-get update && apt-get install -y openjdk-21-jdk-headless netcat-traditional

WORKDIR /app

COPY --from=java-base /app/java/lib /app/java/lib

RUN pip install --no-cache-dir poetry && poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* README.md ./

COPY gnomepy_testing ./gnomepy_testing

RUN poetry install --no-interaction --no-ansi

COPY docker/run_comparison.sh /app/run_comparison.sh
RUN chmod +x /app/run_comparison.sh

RUN mkdir -p /app/output

ENV PYTHONUNBUFFERED=1
ENV JAVA_CLASSPATH=/app/java/lib/*

CMD ["/app/run_comparison.sh"]

