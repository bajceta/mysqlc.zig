FROM debian:bullseye-slim AS base
RUN apt-get update && apt-get install -y \
libssl-dev \
libzstd-dev \
libmariadb-dev && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*
WORKDIR /build
CMD ["/bin/bash"]

FROM base AS builder
RUN apt-get update && apt-get install -y \
pkg-config \
build-essential \
inotify-tools \
curl && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*
RUN curl https://raw.githubusercontent.com/tristanisham/zvm/master/install.sh | bash
RUN /bin/bash -c "source /root/.profile && zvm i master"
ENV PATH="${PATH}:/root/.zvm/bin"
ENV ZIG_GLOBAL_CACHE_DIR="/tmp"
ENV ZIG_LOCAL_CACHE_DIR="/tmp"
RUN zig version
WORKDIR /build
CMD ["/bin/bash"]

FROM builder AS test
COPY ./build.zig .
COPY ./build.zig.zon .
COPY ./src ./src
RUN --mount=type=cache,target=/build/zig-cache zig build test

FROM test AS build
RUN --mount=type=cache,target=/build/zig-cache zig build 

FROM base AS final
WORKDIR /app
COPY --from=build /build/zig-out/bin/zig-api .
CMD ["/app/zig-api"]
