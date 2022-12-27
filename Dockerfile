FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15
WORKDIR /app
COPY project project
ENV JAVA_TOOL_OPTIONS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
COPY build.sbt .
RUN sbt update
COPY src src
