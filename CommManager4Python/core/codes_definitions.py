from __future__ import annotations
import socket, struct, threading
from dataclasses import dataclass
from typing import Optional, Tuple, Literal, Protocol
from abc import ABC, abstractmethod


class CMessageCodec(Protocol):
    """
    Protocol for message serialization.
    Implementations must produce and parse bytes for (msg_type, timestamp, payload).
    """
    def pack(self, msg_type: int, timestamp_s: float, payload: bytes, add_size: bool=False) -> bytes: ...
    def unpack(self, data: bytes, has_size: bool=False) -> tuple[int, float, bytes]: ...

class CHeaderCodec:
    """
    Header format:
      - uint8  : msg_type
      - float64: timestamp (seconds)
      - [opt] uint64: payload_size
    Uses network byte order for portability.
    """
    def pack(self, msg_type: int, timestamp_s: float, payload: bytes, add_size: bool=False) -> bytes:
        if not (0 <= int(msg_type) <= 255):
            raise ValueError("msg_type must fit uint8")
        header = struct.pack("!Bd", int(msg_type), float(timestamp_s))
        if add_size:
            header += struct.pack("!Q", len(payload))
        return header + payload

    def unpack(self, data: bytes, has_size: bool=False) -> tuple[int, float, bytes]:
        base = struct.calcsize("!Bd")
        if len(data) < base:
            raise ValueError("buffer too small for base header")
        msg_type, ts = struct.unpack("!Bd", data[:base])
        off = base
        if has_size:
            if len(data) < off + 8:
                raise ValueError("buffer too small for size field")
            (size,) = struct.unpack("!Q", data[off:off+8])
            off += 8
            if len(data) < off + size:
                raise ValueError("buffer smaller than declared payload")
            payload = data[off:off+size]
        else:
            payload = data[off:]
        return msg_type, ts, payload
