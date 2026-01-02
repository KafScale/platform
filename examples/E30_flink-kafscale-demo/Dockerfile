FROM maven:3.9-eclipse-temurin-11 AS deps
WORKDIR /app
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

FROM maven:3.9-eclipse-temurin-11 AS jar
WORKDIR /app
COPY --from=deps /root/.m2 /root/.m2
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests -Pno-resources package

FROM eclipse-temurin:11-jre AS runtime
WORKDIR /app
COPY --from=jar /app/target/flink-kafscale-demo.jar app.jar
COPY src/main/resources /app/config
ENV KAFSCALE_PROFILE=cluster
ENV KAFSCALE_CONFIG_DIR=/app/config
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
