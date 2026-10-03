# Builds the job jar and bakes it into a Flink image, so the job manager and
# the task manager run the exact same artifact with no volume juggling.
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build
# Resolve dependencies against the pom alone first; a source change then
# rebuilds in seconds instead of re-downloading the dependency tree.
COPY flink-job/pom.xml /build/pom.xml
RUN mvn -B -q dependency:go-offline || true

COPY proto /proto
COPY flink-job/src /build/src
RUN mvn -B -q package -DskipTests

FROM flink:1.20.1-scala_2.12-java17

# usrlib is where Flink's application mode looks for the user jar.
COPY --from=build /build/target/delay-pipeline-1.0.0.jar /opt/flink/usrlib/delay-pipeline.jar
