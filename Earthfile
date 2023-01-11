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

release:
    FROM +build
    RUN sbt "set test in assembly := {}" release with-defaults

testimage:
    FROM earthly/dind:alpine
    COPY Dockerfile .
    COPY src src
    COPY project project
    COPY build.sbt build.sbt
    RUN sbt test:compile
    FROM DOCKERFILE .
    SAVE IMAGE sbt_ch_plugin:latest

test:
    FROM earthly/dind:alpine
    RUN apk add curl
    COPY docker docker
    COPY docker-compose.yml build.sbt .
    COPY docker/wait.sh .
    WITH DOCKER --load sbt_ch_plugin:latest=+testimage --compose docker-compose.yml
        RUN ./wait.sh &&  docker run --rm --name sbt-clickhouse-plugin --network ch_network sbt_ch_plugin:latest sbt ++2.12.15 test it:test
    END

    WITH DOCKER --load sbt_ch_plugin:latest=+testimage --compose docker-compose.yml
        RUN ./wait.sh &&  docker run --rm --name sbt-clickhouse-plugin --network ch_network sbt_ch_plugin:latest sbt ++2.13.10 test it:test
    END
