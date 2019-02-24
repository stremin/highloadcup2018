FROM rust:1.32.0

COPY src /app/src
COPY Cargo.toml /app/

WORKDIR /app

RUN cargo build --release && cp target/release/hlc2018 . && rm -rf target

ENV RUST_BACKTRACE=1
ENV RUST_LOG="hlc2018=info"
CMD ["./hlc2018", "80", "/tmp/data/", "--no-stats", "--cache=on"]
