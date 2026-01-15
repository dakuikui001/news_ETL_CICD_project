FROM quay.io/astronomer/astro-runtime:12.6.0

USER root

# 1. 安装 Java、Chromium 和对应的 Driver
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    chromium \
    chromium-driver \
    && apt-get clean

# 2. 显式指定 ARM64 架构的 Java 路径 (解决 M1/M2/M3 报错问题)
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
ENV PATH="${PATH}:${JAVA_HOME}/bin"

USER astro