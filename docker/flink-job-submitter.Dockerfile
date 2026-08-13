FROM maven:3.9.9-eclipse-temurin-11 AS build

WORKDIR /build
COPY pom.xml .
COPY flink-jobs/pom.xml flink-jobs/pom.xml
RUN mvn -pl flink-jobs -am dependency:go-offline

COPY flink-jobs flink-jobs
RUN mvn -pl flink-jobs -am clean package -DskipTests

FROM flink:1.17.1-scala_2.12-java11

COPY --from=build /build/flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-jobs.jar
COPY docker/submit_flink_job.sh /opt/flink/bin/submit_flink_job.sh
RUN chmod +x /opt/flink/bin/submit_flink_job.sh

ENTRYPOINT ["/opt/flink/bin/submit_flink_job.sh"]
