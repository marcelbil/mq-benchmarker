# ==========================================
# STAGE 1: Build (Met Maven & Java 21 LTS)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# ==========================================
# STAGE 2: Run (Distroless / Zero OS-vulnerabilities)
# ==========================================
FROM gcr.io/distroless/java21-debian12:nonroot

WORKDIR /app
COPY --from=build /app/target/mq-benchmarker-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]