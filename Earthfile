VERSION 0.6
FROM openjdk:11.0.13-jdk
WORKDIR /app

sbt:
   RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
       echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
       curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
       apt-get update && \
       apt-get install sbt
   COPY build.sbt ./
   COPY project project
   RUN touch a.scala && sbt compile && rm a.scala


build:
    FROM +sbt
    COPY src src
    RUN sbt assembly
    SAVE ARTIFACT TARGET/scala-2.12/spark-clickhouse-plugin-assembly-*.jar /jars

tests:
    FROM +sbt
    COPY docker-compose.yml ./
    COPY docker docker
    COPY src src
    WITH DOCKER --compose docker-compose.yml
        RUN sbt test && sbt it:test
    END
