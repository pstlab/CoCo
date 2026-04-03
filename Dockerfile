# --- Stage 1: The Builder ---
FROM rust:1.93-slim-bookworm AS builder

RUN apt-get update && apt-get install -y build-essential libclang-dev git wget unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN git clone --branch investigating_rust https://github.com/ratioSolver/COCO.git .

RUN wget -O clips_642.zip https://sourceforge.net/projects/clipsrules/files/CLIPS/6.4.2/clips_core_source_642.zip/download && \
    unzip clips_642.zip -d clips_temp && \
    mkdir -p clips_source && \
    mv clips_temp/clips_core_source_642/core/* clips_source/ && \
    rm -rf clips_temp clips_642.zip

RUN cargo build --release

# --- Stage 2: The Final Image ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/server /usr/local/bin/coco

EXPOSE 3000

CMD ["coco"]