import argparse
import random
import sys
import time
from concurrent import futures
import printing_pb2
import printing_pb2_grpc
import grpc
import os

sys.path.append(os.path.dirname(__file__))

# servidor burro tadinho... ele so precisa de ajuda 

class PrintingService(printing_pb2_grpc.PrintingServiceServicer):
    def __init__(self) -> None:
        self._rng = random.Random()

    def SendToPrinter(self, request, context):
        ts = request.lamport_timestamp # pega o relogio do mano lamport
        cid = request.client_id 
        msg = request.message_content
        print(f"[TS: {ts}] CLIENTE {cid}: {msg}") # manda o conteudo da msg
        time.sleep(2 + self._rng.randrange(0, 2)) #pras msg nao ficar sendo printadas uma em cima da outra... tenha calma
        return printing_pb2.PrintResponse(
            success=True,
            confirmation_message=f"Printed at {int(time.time())}",
            lamport_timestamp=ts,
        )

def serve(port: int) -> None: # setar a porta ne... vai entrar por onde
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    printing_pb2_grpc.add_PrintingServiceServicer_to_server(PrintingService(), server) 
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Printer] Listening on port {port}")
    server.wait_for_termination()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()
    serve(args.port)

if __name__ == "__main__":
    main()

