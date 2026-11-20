# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY ledger-service/pom.xml ledger-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f ledger-service/pom.xml dependency:go-offline
COPY ledger-service/src ledger-service/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f ledger-service/pom.xml package -DskipTests \
 && java -Djarmode=tools -jar ledger-service/target/ledger-service.jar extract --layers --launcher --destination /app

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S ledger && adduser -S ledger -G ledger
WORKDIR /app
COPY --from=build /app/dependencies/ ./
COPY --from=build /app/spring-boot-loader/ ./
COPY --from=build /app/snapshot-dependencies/ ./
COPY --from=build /app/application/ ./
USER ledger
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "org.springframework.boot.loader.launch.JarLauncher"]
