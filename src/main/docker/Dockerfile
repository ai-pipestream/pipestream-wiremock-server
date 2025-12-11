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

# Install Java 21 runtime (headless for smaller size) and update CVEs
RUN microdnf install -y \
    java-21-openjdk-headless \
    ca-certificates \
    && microdnf update -y \
    && microdnf clean all \
    && rm -rf /var/cache/yum

ENV LANGUAGE='en_US:en' \
    JAVA_OPTS_APPEND="-Djava.awt.headless=true"

# Create deployment directory
RUN mkdir -p /deployments && chown -R 185:185 /deployments

# Copy the shadow JAR (contains all dependencies) and resources
COPY --chown=185:185 build/libs/*.jar /deployments/
COPY --chown=185:185 build/libs/resources/ /deployments/resources/

EXPOSE 8080 50052
USER 185

# Health check using WireMock admin endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/__admin/health || exit 1

ENTRYPOINT ["java", "-cp", "/deployments/*:/deployments/lib/*:/deployments/resources/*", "ai.pipestream.wiremock.server.Main"]