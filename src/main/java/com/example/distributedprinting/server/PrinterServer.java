package com.example.distributedprinting.server;

import com.example.distributedprinting.proto.PrintRequest;
import com.example.distributedprinting.proto.PrintResponse;
import com.example.distributedprinting.proto.PrintingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Instant;
import java.util.Random;

public class PrinterServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50051;
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            }
        }

        Server server = ServerBuilder.forPort(port)
                .addService(new PrintingService())
                .build()
                .start();
        System.out.println("[Printer] Listening on port " + port);
        server.awaitTermination();
    }

    static class PrintingService extends PrintingServiceGrpc.PrintingServiceImplBase {
        private final Random random = new Random();

        @Override
        public void sendToPrinter(PrintRequest request, StreamObserver<PrintResponse> responseObserver) {
            long ts = request.getLamportTimestamp();
            int id = request.getClientId();
            String msg = request.getMessageContent();
            System.out.printf("[TS: %d] CLIENTE %d: %s%n", ts, id, msg);
            try {
                Thread.sleep(2000 + random.nextInt(1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            PrintResponse resp = PrintResponse.newBuilder()
                    .setSuccess(true)
                    .setConfirmationMessage("Printed at " + Instant.now())
                    .setLamportTimestamp(ts)
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}

