FROM rust:bullseye AS build
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:11-slim
COPY --from=build /build/target/release/at-stats-ingest /usr/bin
RUN apt update && apt install -y ca-certificates
CMD ["at-stats-ingest"]
