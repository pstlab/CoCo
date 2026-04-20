# --- Stage 1: The Backend Builder ---
FROM rust:1.95-slim-bookworm AS backend-builder

RUN apt-get update && apt-get install -y build-essential libclang-dev git wget unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN git clone https://github.com/pstlab/CoCo.git .

RUN wget -O clips_642.zip https://sourceforge.net/projects/clipsrules/files/CLIPS/6.4.2/clips_core_source_642.zip/download && \
    unzip clips_642.zip -d clips_temp && \
    mkdir -p clips_source && \
    mv clips_temp/clips_core_source_642/core/* clips_source/ && \
    rm -rf clips_temp clips_642.zip

RUN cargo build --release --features "server ollama"

# --- Stage 2: Frontend Builder ---
FROM node:20-slim AS frontend-builder

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN git clone https://github.com/pstlab/CoCo.git .

WORKDIR /usr/src/app/gui

RUN npm install && npm run build:playground

# --- Stage 3: The Final Image ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=backend-builder /usr/src/app/target/release/coco-reasoner /usr/local/bin/coco
COPY --from=frontend-builder /usr/src/app/gui/playground/dist /usr/local/bin/gui

WORKDIR /usr/local/bin

EXPOSE 3000

CMD ["coco"]