# ==========================================
# STAGE 1: Build (Met Maven & Java 25)
# ==========================================
FROM maven:3.9.14-eclipse-temurin-25-alpine AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# ==========================================
# STAGE 2: Run (Minimal Alpine JRE 25)
# ==========================================
FROM eclipse-temurin:26-jre-alpine
WORKDIR /app
RUN apk update && apk upgrade --no-cache
RUN addgroup -S mqbenchmarker && adduser -S mqbenchmarker -G mqbenchmarker
USER mqbenchmarker
COPY --from=build /app/target/mq-benchmarker-*.jar app.jar
# Expose port 8080 for the Web UI
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]