FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15
WORKDIR /app
COPY project project
COPY build.sbt .
RUN sbt update
COPY src src

# CMD sbt test it:test