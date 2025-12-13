####
# This Dockerfile is used in order to build a container that runs the Pipestream WireMock Server
#
# Before building the container image run:
#
# ./gradlew build -x test
#
# Then, build the image with:
#
# docker build -f src/main/docker/Dockerfile -t pipestream-wiremock-server .
#
# Then run the container using:
#
# docker run -i --rm -p 8080:8080 -p 50052:50052 pipestream-wiremock-server
#
###

FROM registry.access.redhat.com/ubi9-minimal:9.5

# Install Java 21 runtime, unzip for extraction, and curl for healthcheck
RUN microdnf install -y \
    java-21-openjdk-headless \
    unzip \
    curl \
    ca-certificates \
    && microdnf update -y \
    && microdnf clean all \
    && rm -rf /var/cache/yum

ENV LANGUAGE='en_US:en' \
    JAVA_OPTS_APPEND="-Djava.awt.headless=true"

# Use standard app directory like Jib
WORKDIR /app
RUN chown 185:185 /app

# Copy the shadow JAR
COPY --chown=185:185 build/libs/pipestream-wiremock-server-*-all.jar /app/app.jar

# Explode the JAR to allow file-system based resource loading if needed
USER 185
RUN unzip -q /app/app.jar -d /app && rm /app/app.jar

EXPOSE 8080 50052

# Health check using WireMock admin endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/__admin/health || exit 1

# Run with classpath pointing to current directory (exploded app)
ENTRYPOINT ["java", "-cp", ".", "ai.pipestream.wiremock.server.Main"]