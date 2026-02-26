# Interface Engine

## HL7 PDF Compression Engine

### Config format (`config.yaml`)

```yaml
listen_host: 0.0.0.0
listen_port: 2575

forward_host: 127.0.0.1
forward_port: 3575
forward_use_mllp: true

paths:
  inbound_hl7_dir: data/inbound_hl7
  original_pdf_dir: data/original_pdf
  compressed_pdf_dir: data/compressed_pdf
  outbound_hl7_dir: data/outbound_hl7
```

### Run command

```bash
python hl7_engine.py
```

### Expected behavior

1. Listens on `listen_host:listen_port` for inbound HL7 over TCP (MLLP-framed or plain payload).
2. Stores inbound HL7 into a timestamped file in `paths.inbound_hl7_dir`.
3. Parses HL7 segments, selects the last `OBX`, and extracts/decode the Base64 PDF from `OBX-5`.
4. Writes original PDF to `paths.original_pdf_dir`, compresses it using `pypdf`, and writes compressed PDF to `paths.compressed_pdf_dir`.
5. Base64-encodes compressed PDF, replaces the last `OBX-5` payload, and writes outbound HL7 into `paths.outbound_hl7_dir`.
6. Forwards the outbound HL7 to `forward_host:forward_port` (MLLP framing controlled by `forward_use_mllp`).
