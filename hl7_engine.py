import base64
import binascii
import logging
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from pypdf import PdfReader, PdfWriter

MLLP_START = b"\x0b"  # VT
MLLP_END = b"\x1c\x0d"  # FS + CR


@dataclass
class EngineConfig:
    listen_host: str
    listen_port: int
    forward_host: str
    forward_port: int
    forward_use_mllp: bool
    inbound_hl7_dir: Path
    original_pdf_dir: Path
    compressed_pdf_dir: Path
    outbound_hl7_dir: Path


def load_config(path: str = "config.yaml") -> EngineConfig:
    with open(path, "r", encoding="utf-8") as file:
        cfg = yaml.safe_load(file)

    paths: Dict[str, str] = cfg.get("paths", {})
    required_path_keys = [
        "inbound_hl7_dir",
        "original_pdf_dir",
        "compressed_pdf_dir",
        "outbound_hl7_dir",
    ]

    missing_paths = [key for key in required_path_keys if key not in paths]
    if missing_paths:
        raise ValueError(f"Missing required path keys in config.yaml: {missing_paths}")

    config = EngineConfig(
        listen_host=cfg["listen_host"],
        listen_port=int(cfg["listen_port"]),
        forward_host=cfg["forward_host"],
        forward_port=int(cfg["forward_port"]),
        forward_use_mllp=bool(cfg.get("forward_use_mllp", True)),
        inbound_hl7_dir=Path(paths["inbound_hl7_dir"]),
        original_pdf_dir=Path(paths["original_pdf_dir"]),
        compressed_pdf_dir=Path(paths["compressed_pdf_dir"]),
        outbound_hl7_dir=Path(paths["outbound_hl7_dir"]),
    )
    ensure_directories(config)
    return config


def ensure_directories(config: EngineConfig) -> None:
    for path in [
        config.inbound_hl7_dir,
        config.original_pdf_dir,
        config.compressed_pdf_dir,
        config.outbound_hl7_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def timestamped_file(directory: Path, prefix: str, extension: str) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    return directory / f"{prefix}_{ts}.{extension}"


def receive_hl7_message(host: str, port: int, timeout_seconds: int = 15) -> Tuple[bytes, bool]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((host, port))
            server.listen(1)
            logging.info("Listening for inbound HL7 on %s:%s", host, port)

            conn, addr = server.accept()
            with conn:
                logging.info("Accepted inbound connection from %s:%s", *addr)
                conn.settimeout(timeout_seconds)

                chunks: List[bytes] = []
                while True:
                    try:
                        data = conn.recv(4096)
                    except socket.timeout:
                        logging.info("Socket read timeout reached; assuming message complete")
                        break
                    if not data:
                        break
                    chunks.append(data)
                    combined = b"".join(chunks)
                    if MLLP_END in combined:
                        break

                payload = b"".join(chunks)
                if not payload:
                    raise ValueError("Received empty inbound payload")

                clean_payload, was_mllp = strip_mllp_framing(payload)
                return clean_payload, was_mllp
    except OSError as exc:
        raise ConnectionError(f"Failed to bind/receive HL7 payload on {host}:{port}: {exc}") from exc


def strip_mllp_framing(payload: bytes) -> Tuple[bytes, bool]:
    stripped = payload.strip(b"\x00\n")
    if stripped.startswith(MLLP_START):
        stripped = stripped[1:]
        if MLLP_END in stripped:
            stripped = stripped.split(MLLP_END, 1)[0]
        return stripped, True

    if payload.endswith(MLLP_END):
        return payload[: -len(MLLP_END)], True

    return payload, False


def parse_hl7_segments(message_text: str) -> Tuple[List[str], str, str]:
    segments = [s for s in message_text.split("\r") if s]
    if not segments or not segments[0].startswith("MSH"):
        raise ValueError("Invalid HL7 message: missing MSH segment")

    field_sep = segments[0][3]
    encoding = segments[0].split(field_sep)[1] if len(segments[0].split(field_sep)) > 1 else "^~\\&"
    component_sep = encoding[0] if encoding else "^"
    return segments, field_sep, component_sep


def find_last_obx_index(segments: List[str]) -> int:
    obx_indices = [i for i, seg in enumerate(segments) if seg.startswith("OBX")]
    if not obx_indices:
        raise ValueError("Invalid HL7 message: no OBX segments found")
    return obx_indices[-1]


def decode_obx_pdf(obx_segment: str, field_sep: str, component_sep: str) -> Tuple[bytes, str]:
    fields = obx_segment.split(field_sep)
    if len(fields) <= 5:
        raise ValueError("Invalid OBX segment: OBX-5 payload field is missing")

    obx5 = fields[5]
    if not obx5:
        raise ValueError("Invalid OBX segment: OBX-5 payload is empty")

    candidate_values = [obx5]
    components = obx5.split(component_sep) if component_sep in obx5 else []
    candidate_values.extend([c for c in components if c])

    best_error: Optional[Exception] = None
    for candidate in sorted(candidate_values, key=len, reverse=True):
        try:
            decoded = base64.b64decode(candidate, validate=True)
            if decoded.startswith(b"%PDF"):
                return decoded, candidate
            if candidate == obx5:
                return decoded, candidate
        except (binascii.Error, ValueError) as exc:
            best_error = exc

    raise ValueError(f"Failed to decode OBX-5 Base64 payload: {best_error}")


def compress_pdf(original_pdf: Path, output_pdf: Path) -> None:
    try:
        reader = PdfReader(str(original_pdf))
        writer = PdfWriter()

        for page in reader.pages:
            if hasattr(page, "compress_content_streams"):
                page.compress_content_streams()
            writer.add_page(page)

        if hasattr(writer, "compress_identical_objects"):
            writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)

        with output_pdf.open("wb") as out_stream:
            writer.write(out_stream)
    except Exception as exc:
        raise RuntimeError(f"PDF compression failed: {exc}") from exc


def replace_obx_payload(obx_segment: str, new_b64: str, old_payload: str, field_sep: str, component_sep: str) -> str:
    fields = obx_segment.split(field_sep)
    obx5 = fields[5]

    if old_payload == obx5:
        fields[5] = new_b64
    else:
        components = obx5.split(component_sep)
        replaced = False
        for i, comp in enumerate(components):
            if comp == old_payload:
                components[i] = new_b64
                replaced = True
                break
        if not replaced:
            raise ValueError("Could not locate original Base64 payload component in OBX-5")
        fields[5] = component_sep.join(components)

    return field_sep.join(fields)


def send_outbound_message(host: str, port: int, message_bytes: bytes, use_mllp: bool) -> None:
    framed = MLLP_START + message_bytes + MLLP_END if use_mllp else message_bytes
    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(framed)
        logging.info("Forwarded outbound HL7 message to %s:%s", host, port)
    except OSError as exc:
        raise ConnectionError(f"Failed to connect/send outbound HL7 to {host}:{port}: {exc}") from exc


def process_once(config: EngineConfig) -> None:
    inbound_bytes, inbound_was_mllp = receive_hl7_message(config.listen_host, config.listen_port)

    inbound_path = timestamped_file(config.inbound_hl7_dir, "inbound_hl7", "hl7")
    inbound_path.write_bytes(inbound_bytes)
    logging.info("Wrote inbound HL7 payload to %s", inbound_path)

    try:
        message_text = inbound_bytes.decode("utf-8")
    except UnicodeDecodeError:
        message_text = inbound_bytes.decode("latin-1")

    segments, field_sep, component_sep = parse_hl7_segments(message_text)
    obx_index = find_last_obx_index(segments)

    pdf_bytes, original_b64_value = decode_obx_pdf(segments[obx_index], field_sep, component_sep)

    original_pdf_path = timestamped_file(config.original_pdf_dir, "original", "pdf")
    original_pdf_path.write_bytes(pdf_bytes)
    logging.info("Wrote extracted original PDF to %s", original_pdf_path)

    compressed_pdf_path = timestamped_file(config.compressed_pdf_dir, "compressed", "pdf")
    compress_pdf(original_pdf_path, compressed_pdf_path)
    logging.info("Wrote compressed PDF to %s", compressed_pdf_path)

    compressed_b64 = base64.b64encode(compressed_pdf_path.read_bytes()).decode("ascii")

    segments[obx_index] = replace_obx_payload(
        segments[obx_index],
        compressed_b64,
        original_b64_value,
        field_sep,
        component_sep,
    )

    outbound_text = "\r".join(segments)
    if message_text.endswith("\r"):
        outbound_text += "\r"
    outbound_bytes = outbound_text.encode("utf-8")

    outbound_path = timestamped_file(config.outbound_hl7_dir, "outbound_hl7", "hl7")
    outbound_path.write_bytes(outbound_bytes)
    logging.info("Wrote outbound HL7 payload to %s", outbound_path)

    send_outbound_message(
        config.forward_host,
        config.forward_port,
        outbound_bytes,
        config.forward_use_mllp if inbound_was_mllp else config.forward_use_mllp,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        config = load_config()
        process_once(config)
    except Exception:
        logging.exception("HL7 engine failed")
        raise


if __name__ == "__main__":
    main()
