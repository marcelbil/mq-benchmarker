# Stage 1: Build with Maven and Java 17
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Run with secure Red Hat UBI9 JRE 17
FROM eclipse-temurin:17.0.10_7-jre-ubi9-minimal
WORKDIR /app
COPY --from=build /app/target/mq-benchmarker-*.jar app.jar
# Expose port 8080 for the Web UI
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]