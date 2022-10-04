VERSION 0.6
FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15
WORKDIR /app

sbt:
    COPY project project
    COPY build.sbt .
    RUN sbt update

build:
    FROM +sbt
    COPY docker docker
    COPY src src
    COPY project project
    RUN sbt test:compile

jar:
    FROM +build
    RUN sbt "set test in assembly := {}" assembly
    SAVE ARTIFACT TARGET/scala-2.12/spark-clickhouse-plugin-assembly-*.jar AS LOCAL ./jars

test:
    FROM +build
    COPY docker-compose.yml build.sbt .
    COPY docker/wait.sh .
    WITH DOCKER --compose docker-compose.yml
       RUN ./wait.sh && sbt test it:test
    END
