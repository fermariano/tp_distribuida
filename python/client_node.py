import argparse
import os
import random
import sys
import threading
import time
from concurrent import futures
from dataclasses import dataclass
from typing import Dict, List, Tuple

import grpc
from google.protobuf import empty_pb2

import printing_pb2
import printing_pb2_grpc
from lamport import LamportClock

sys.path.append(os.path.dirname(__file__))

State = type("State", (), {"RELEASED": "RELEASED", "WANTED": "WANTED", "HELD": "HELD"})

@dataclass
class Deferred:
    client_id: int
    ts: int
    req_num: int
    event: threading.Event

class MutualExclusionServicer(printing_pb2_grpc.MutualExclusionServiceServicer):
    def __init__(self, node: "ClientNode") -> None:
        self.node = node

    def RequestAccess(self, request, context):

        self.node.clock.on_receive(request.lamport_timestamp)

        with self.node.lock:
            defer = self.node.state == State.HELD or (
                self.node.state == State.WANTED
                and self.node.has_priority_over(
                    request.client_id, request.lamport_timestamp
                )
            )
            if defer:
                ev = threading.Event()
                self.node.deferred.append(
                    Deferred(
                        request.client_id,
                        request.lamport_timestamp,
                        request.request_number,
                        ev,
                    )
                )
                print(
                    f"[Client {self.node.id}] Deferring to {request.client_id} (ts={request.lamport_timestamp}) state={self.node.state}"
                )

        if defer:
            ev.wait()

        ts = self.node.clock.on_send()
        return printing_pb2.AccessResponse(access_granted=True, lamport_timestamp=ts)

    def ReleaseAccess(self, request, context):
        self.node.clock.on_receive(request.lamport_timestamp)
        print(f"[Client {self.node.id}] Peer {request.client_id} released (req#{request.request_number}) ts={request.lamport_timestamp}")
        return empty_pb2.Empty()


class ClientNode:
    def __init__(self, id_: int, port: int, printer_target: str, peer_targets: List[str]
    ) -> None:
        self.id = id_
        self.port = port
        self.printer_target = printer_target
        self.peer_targets = [t for t in peer_targets if not t.endswith(f":{port}")]

        self.clock = LamportClock()
        self.state = State.RELEASED
        self.current_req_ts = -1
        self.current_req_num = 0

        self.lock = threading.Lock()
        self.deferred: List[Deferred] = []

        self._server = None
        self._printer_channel = None
        self._printer_stub = None
        self._peer_channels: Dict[str, grpc.Channel] = {}
        self._peer_stubs: Dict[str, printing_pb2_grpc.MutualExclusionServiceStub] = {}
        self._rng = random.Random()

    def start(self):

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        printing_pb2_grpc.add_MutualExclusionServiceServicer_to_server(
            MutualExclusionServicer(self), self._server
        )
        self._server.add_insecure_port(f"[::]:{self.port}")
        self._server.start()
        print(f"[Client {self.id}] MutualExclusionService on port {self.port}")

        self._printer_channel = grpc.insecure_channel(self.printer_target)
        self._printer_stub = printing_pb2_grpc.PrintingServiceStub(
            self._printer_channel
        )

        for target in self.peer_targets:
            ch = grpc.insecure_channel(target)
            self._peer_channels[target] = ch
            self._peer_stubs[target] = printing_pb2_grpc.MutualExclusionServiceStub(ch)

        t = threading.Thread(target=self._auto_print_loop, daemon=True)
        t.start()

        self._server.wait_for_termination()

    def _auto_print_loop(self):
        while True:
            try:
                delay = 2 + self._rng.randrange(0, 3)
                time.sleep(delay)
                msg = f"Mensagem do cliente {self.id} @ {int(time.time())}"
                self.enter_cs_and_print(msg)
            except Exception as e:
                print(f"[Client {self.id}] Error in print loop: {e}")

    def has_priority_over(self, other_id: int, other_ts: int) -> bool:

        if self.current_req_ts < other_ts:
            return True
        if self.current_req_ts > other_ts:
            return False
        return self.id < other_id

    def enter_cs_and_print(self, message: str):
        with self.lock:
            self.state = State.WANTED
            self.current_req_num += 1
            self.current_req_ts = self.clock.on_send()
            reqnum = self.current_req_num
            ts = self.current_req_ts
            print(f"[Client {self.id}] Requesting CS ts={ts} req#{reqnum}")

        req = printing_pb2.AccessRequest(
            client_id=self.id, lamport_timestamp=ts, request_number=reqnum
        )

        for target, stub in self._peer_stubs.items():
            resp = stub.RequestAccess(req)
            self.clock.on_receive(resp.lamport_timestamp)

        with self.lock:
            self.state = State.HELD
            print(f"[Client {self.id}] Entering CS ts={self.clock.value}")

        presp = self._printer_stub.SendToPrinter(
            printing_pb2.PrintRequest(
                client_id=self.id,
                message_content=message,
                lamport_timestamp=self.clock.on_send(),
                request_number=reqnum,
            )
        )
        self.clock.on_receive(presp.lamport_timestamp)
        print(f"[Client {self.id}] Printed OK: {presp.confirmation_message}")

        with self.lock:
            self.state = State.RELEASED
            to_release = list(self.deferred)
            self.deferred.clear()
        for d in to_release:
            d.event.set()

        rel = printing_pb2.AccessRelease(
            client_id=self.id,
            lamport_timestamp=self.clock.on_send(),
            request_number=reqnum,
        )
        for stub in self._peer_stubs.values():
            try:
                stub.ReleaseAccess(rel)
            except Exception:
                pass
        print(f"[Client {self.id}] Released CS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--server", type=str, required=True, help="printer host:port")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--clients", type=str, default="")
    args = parser.parse_args()

    peers = [c for c in args.clients.split(",") if c]
    node = ClientNode(args.id, args.port, args.server, peers)
    node.start()


if __name__ == "__main__":
    main()
