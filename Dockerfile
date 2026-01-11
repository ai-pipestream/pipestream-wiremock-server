# Stage 1: Build
FROM eclipse-temurin:25-jdk-alpine AS builder

# Install build dependencies:
# - git: Required by ai.pipestream.proto-toolchain to clone proto repositories
# - libstdc++ & gcompat: Required by native binaries like buf and protoc on Alpine
RUN apk add --no-cache git libstdc++ gcompat

# Set the working directory
WORKDIR /project

# Copy Gradle wrapper and configuration files for dependency caching
COPY gradlew .
COPY gradle/ gradle/
COPY build.gradle settings.gradle gradle.properties ./

# Pre-fetch dependencies to improve build performance
RUN ./gradlew --version --no-daemon

# Copy the source code
COPY src/ src/

# Build the Shadow JAR (Fat JAR)
ARG APP_VERSION=latest
RUN ./gradlew shadowJar -Pversion=${APP_VERSION} --no-daemon --info

# Stage 2: Runtime
FROM eclipse-temurin:25-jre-alpine

LABEL maintainer="Pipestream Engine Team"
LABEL project="pipestream-wiremock-server"

# Set runtime working directory
WORKDIR /app

# Create a non-root user for security
RUN addgroup -S pipestream && adduser -S pipestream -G pipestream
USER pipestream

# Runtime configuration via Build Arguments
ARG JAVA_OPTS="-Xms256m -Xmx512m -Djava.security.egd=file:/dev/./urandom"
ENV JAVA_OPTS=${JAVA_OPTS}

# Copy only the necessary artifact from the builder stage
COPY --from=builder --chown=pipestream:pipestream /project/build/libs/pipestream-wiremock-server-*-all.jar app.jar

# Expose standard WireMock port and gRPC port
EXPOSE 8080 50052

# Start the application
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar app.jar"]
