# Stage 1: Build met Maven en Java 17
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Run met veilige Red Hat UBI9 JRE 17
FROM eclipse-temurin:17.0.10_7-jre-ubi9-minimal
WORKDIR /app
COPY --from=build /app/target/artemis-benchmark-0.0.4.jar app.jar
# Expose poort 8080 voor de Web UI
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]