FROM rust:1.88.0-alpine3.22 AS build

RUN set -ex && \
    apk add --no-progress --no-cache \
        musl-dev pkgconf openssl-dev openssl-libs-static make cmake g++

WORKDIR /app
COPY Cargo.toml Cargo.lock /app/
COPY ./src /app/src
RUN cargo build --release

FROM alpine:3.22 AS run

RUN apk add --no-progress --no-cache tzdata

ENV UID=65532
ENV GID=65532
ENV USER=nonroot
ENV GROUP=nonroot

RUN addgroup -g $GID $GROUP && \
    adduser --shell /sbin/nologin --disabled-password \
    --no-create-home --uid $UID --ingroup $GROUP $USER

WORKDIR /app/
COPY --from=build /app/target/release/adt-to-fhir ./
COPY ./app.yaml ./
USER $USER

ENTRYPOINT ["/app/adt-to-fhir"]
