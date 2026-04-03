# CoCo: Combined Deduction and Abduction Reasoner

![Build Status](https://github.com/pstlab/CoCo/actions/workflows/rust.yml/badge.svg)
[![codecov](https://codecov.io/gh/pstlab/CoCo/branch/main/graph/badge.svg)](https://codecov.io/gh/pstlab/CoCo)

**CoCo** (Combined deduCtiOn and abduCtiOn) is a dual-process cognitive architecture written in Rust. It combines a CLIPS-based rule engine with timeline planning to support both deduction and abduction in dynamic environments.

Highlights
- Hybrid reasoning: deduction + abduction
- Rust core for performance and safety
- CLIPS integration for rules and pattern matching
- Simple web interface for interaction and visualization

## Quick start

Run the server locally:

```bash
cargo run
```

By default the server listens on `http://0.0.0.0:3000` and serves the web UI.

## Installation

### Prerequisites
CoCo links to the CLIPS v6.4.2 C library. Install CLIPS as follows:

1. Download and unpack:

```bash
wget -O clips_core_source_642.zip https://sourceforge.net/projects/clipsrules/files/CLIPS/6.4.2/clips_core_source_642.zip/download
unzip clips_core_source_642.zip
```

2. Build the core library:

```bash
cd clips_core_source_642/core
make release
```

3. Either point `CLIPS_SOURCE_DIR` at the extracted source, or install the static library system-wide:

```bash
export CLIPS_SOURCE_DIR=$(pwd)
# or
sudo cp libclips.a /usr/local/lib/
sudo ldconfig
```

## Examples: loading Classes, Objects and Rules

Below are minimal examples that show JSON payloads (useful for APIs or config files).

1) Class example (JSON)

```json
{
   "name": "Person",
   "parents": ["Agent"],
   "static_properties": {
      "age": { "type": "int", "default": 30, "min": 0, "max": 150 },
      "name": { "type": "string", "default": "Unknown" }
   },
   "dynamic_properties": {
      "mood": { "type": "symbol", "allowed_values": ["happy", "neutral", "sad"], "default": "neutral" }
   }
}
```

2) Object example (JSON)

```json
{
   "classes": ["Person"],
   "properties": {
      "age": 28,
      "name": "Alice"
   }
}
```

3) Rule example (JSON)

```json
{
   "name": "greet",
   "content": "(defrule greet (Person (name ?n)) => (printout t \\\"Hello \\\" ?n \\\"!\\\" crlf))"
}
```

## Contributing

Contributions are welcome! Please open issues for bugs or feature requests, and submit pull requests for improvements.

## License

CoCo is licensed under the MIT License. See [LICENSE](LICENSE) for details.