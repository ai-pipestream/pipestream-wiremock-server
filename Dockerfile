# Use JIB to build the image, this Dockerfile is just for documentation or manual builds if needed
# The build.gradle is configured to use Jib which is preferred.

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy the built jar (this assumes you ran ./gradlew build)
COPY build/libs/pipestream-wiremock-server-*-all.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
