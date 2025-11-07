from __future__ import annotations
import socket, struct, threading
from dataclasses import dataclass
from typing import Optional, Tuple, Literal, Protocol
from abc import ABC, abstractmethod
from CommManager4Python.core.codes_definitions import CMessageCodec, CHeaderCodec


@dataclass(frozen=True)
class CSocketConfig:
    """
    Immutable socket configuration.
    """
    address: str
    port: int
    recv_buf: int = 65536
    reuse_addr: bool = True


class CSocketHandler(ABC):
    """
    Abstract base for socket handlers.
    Holds shared config and codec. Subclasses own their concrete socket(s).
    """
    def __init__(self,
                 address: str,
                 port: int,
                 byte_ordering: Literal["little", "big"]="little",
                 cfg: Optional[CSocketConfig]=None,
                 codec: Optional[CMessageCodec]=None) -> None:
        """
        :param address: Host/IP to bind or connect.
        :param port: Port number.
        :param byte_ordering: Kept for API parity; codec uses network order by default.
        :param cfg: Optional prebuilt configuration. If provided, overrides address/port.
        :param codec: Message codec. Defaults to CSimpleHeaderCodec.
        """
        self.byte_ordering: Literal["little", "big"] = byte_ordering
        self.cfg = cfg or CSocketConfig(address=address, port=port)
        self.codec = codec or CHeaderCodec()

    @abstractmethod
    def close(self) -> None:
        """
        Close all owned sockets and release resources.
        """
        ...

class CTCPServer(CSocketHandler):
    """
    TCP server endpoint.
    Listens, accepts clients, and dispatches connection handlers.
    """
    def __init__(self,
                 address: str,
                 port: int,
                 backlog: int=128,
                 **kwargs) -> None:
        """
        :param backlog: Listen backlog.
        Other params inherited from CSocketHandler.
        """
        super().__init__(address, port, **kwargs)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.cfg.reuse_addr:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.cfg.address, self.cfg.port))
        s.listen(backlog)
        self._listen_sock = s

    def accept(self) -> Tuple[socket.socket, Tuple[str, int]]:
        """
        Blocking accept for a single client.
        :return: (connected_socket, client_address)
        """
        return self._listen_sock.accept()

    def serve_forever(self, handler) -> None:
        """
        Accept connections and run handler(conn, addr) in a thread per client.
        :param handler: Callable taking (conn: socket.socket, addr: tuple).
        """
        while True:
            conn, addr = self.accept()
            threading.Thread(target=handler, args=(conn, addr), daemon=True).start()

    def close(self) -> None:
        """
        Stop listening.
        """
        self._listen_sock.close()

class CTCPClient(CSocketHandler):
    """
    TCP client endpoint.
    Manages a single connected socket.
    """
    def __init__(self,
                 address: str,
                 port: int,
                 **kwargs) -> None:
        """
        Connect to the remote TCP server at initialization.
        """
        super().__init__(address, port, **kwargs)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.cfg.address, self.cfg.port))
        self._sock = s

    def send_msg(self, msg_type: int, timestamp: float, payload: bytes, add_size: bool=False) -> None:
        """
        Serialize and send a message using the configured codec.
        """
        packet = self.codec.pack(msg_type, timestamp, payload, add_size)
        self._sock.sendall(packet)

    def recv_any(self) -> bytes:
        """
        Receive up to recv_buf bytes. For framed protocols, implement length-prefix reads.
        """
        return self._sock.recv(self.cfg.recv_buf)

    def recv_exact(self, nbytes: int) -> bytes:
        """
        Receive exactly nbytes or raise on EOF.
        """
        chunks, got = [], 0
        while got < nbytes:
            b = self._sock.recv(nbytes - got)
            if not b:
                raise ConnectionError("peer closed")
            chunks.append(b); got += len(b)
        return b"".join(chunks)

    def close(self) -> None:
        """
        Close the TCP connection.
        """
        self._sock.close()

class CUDPNode(CSocketHandler):
    """
    UDP endpoint.
    Works as a server (bind only) or a fixed-peer client (optional connect()).
    """
    def __init__(self,
                 address: str,
                 port: int,
                 peer: Optional[Tuple[str, int]]=None,
                 **kwargs) -> None:
        """
        :param peer: Optional fixed peer. If provided, enables send()/recv() without address.
        """
        super().__init__(address, port, **kwargs)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self.cfg.reuse_addr:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.cfg.address, self.cfg.port))
        if peer:
            s.connect(peer)
        self._sock = s

    def recvfrom(self) -> Tuple[bytes, Tuple[str, int]]:
        """
        Receive one datagram and the sender address.
        """
        return self._sock.recvfrom(self.cfg.recv_buf)

    def sendto(self, payload: bytes, addr: Tuple[str, int]) -> None:
        """
        Send one datagram to the specified address.
        """
        self._sock.sendto(payload, addr)

    def send_msg(self, msg_type: int, timestamp: float, payload: bytes,
                 addr: Optional[Tuple[str, int]]=None, add_size: bool=False) -> None:
        """
        Serialize and send a message. If addr is None, the socket must be connected to a peer.
        """
        packet = self.codec.pack(msg_type, timestamp, payload, add_size)
        if addr is None:
            self._sock.send(packet)
        else:
            self._sock.sendto(packet, addr)

    def close(self) -> None:
        """
        Close the UDP socket.
        """
        self._sock.close()

def MakeSocketHandler(kind: Literal["tcp_server", "tcp_client", "udp"],
                       address: str,
                       port: int,
                       **kwargs) -> CSocketHandler:
    """
    Factory that returns the proper subclass following your naming.
    """
    if kind == "tcp_server":
        return CTCPServer(address, port, **kwargs)
    if kind == "tcp_client":
        return CTCPClient(address, port, **kwargs)
    if kind == "udp":
        return CUDPNode(address, port, **kwargs)
    raise ValueError(f"Unknown kind: {kind}")