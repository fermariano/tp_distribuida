package com.example.distributedprinting.client;

import com.example.distributedprinting.proto.*;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class ClientNode {
    private final int id;
    private final int port;
    private final String printerTarget;
    private final List<String> peerTargets; // e.g., host:port

    private final LamportClock clock = new LamportClock();
    private volatile State state = State.RELEASED;
    private long currentRequestTs = -1;
    private int currentRequestNum = 0;

    private final Object lock = new Object();
    private final List<Deferred> deferredQueue = new ArrayList<>();

    private Server server;
    private ManagedChannel printerChannel;
    private PrintingServiceGrpc.PrintingServiceBlockingStub printerStub;

    private final Map<String, ManagedChannel> peerChannels = new ConcurrentHashMap<>();
    private final Map<String, MutualExclusionServiceGrpc.MutualExclusionServiceFutureStub> peerFutureStubs = new ConcurrentHashMap<>();
    private final Map<String, MutualExclusionServiceGrpc.MutualExclusionServiceBlockingStub> peerBlockingStubs = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public ClientNode(int id, int port, String printerTarget, List<String> peerTargets) {
        this.id = id;
        this.port = port;
        this.printerTarget = printerTarget;
        this.peerTargets = new ArrayList<>(peerTargets);
    }

    public void start() throws IOException, InterruptedException {
        // Start gRPC server for MutualExclusionService
        this.server = ServerBuilder.forPort(port)
                .addService(new MutualExclusionServiceImpl())
                .build()
                .start();
        System.out.println("[Client " + id + "] MutualExclusionService on port " + port);

        // Create printer client stub
        this.printerChannel = ManagedChannelBuilder.forTarget(printerTarget).usePlaintext().build();
        this.printerStub = PrintingServiceGrpc.newBlockingStub(printerChannel);

        // Create peer client stubs
        for (String target : peerTargets) {
            ManagedChannel ch = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            peerChannels.put(target, ch);
            peerFutureStubs.put(target, MutualExclusionServiceGrpc.newFutureStub(ch));
            peerBlockingStubs.put(target, MutualExclusionServiceGrpc.newBlockingStub(ch));
        }

        // Schedule automatic print jobs
        scheduler.scheduleWithFixedDelay(this::attemptPrintJob, 1000, 3000, TimeUnit.MILLISECONDS);

        // Keep running
        server.awaitTermination();
    }

    private void attemptPrintJob() {
        try {
            String message = "Mensagem do cliente " + id + " @ " + System.currentTimeMillis();
            enterCriticalSectionAndPrint(message);
        } catch (Exception e) {
            System.err.println("[Client " + id + "] Error in print job: " + e);
        }
    }

    private boolean hasPriorityOver(AccessRequest incoming) {
        // this returns true if THIS node's request has priority over incoming
        // Priority by (timestamp, id) lexicographic order: smaller wins
        if (currentRequestTs < incoming.getLamportTimestamp()) return true;
        if (currentRequestTs > incoming.getLamportTimestamp()) return false;
        return this.id < incoming.getClientId();
    }

    private void enterCriticalSectionAndPrint(String message) throws Exception {
        // Request phase
        List<ListenableFuture<AccessResponse>> futures = new ArrayList<>();
        int reqNum;
        long ts;
        synchronized (lock) {
            state = State.WANTED;
            reqNum = ++currentRequestNum;
            ts = clock.onSend();
            currentRequestTs = ts;
            System.out.printf("[Client %d] Requesting CS ts=%d req#%d%n", id, ts, reqNum);
        }

        AccessRequest req = AccessRequest.newBuilder()
                .setClientId(id)
                .setLamportTimestamp(ts)
                .setRequestNumber(reqNum)
                .build();

        for (Map.Entry<String, MutualExclusionServiceGrpc.MutualExclusionServiceFutureStub> e : peerFutureStubs.entrySet()) {
            ListenableFuture<AccessResponse> fut = e.getValue().requestAccess(req);
            futures.add(fut);
        }

        // Wait for all peer grants
        for (ListenableFuture<AccessResponse> f : futures) {
            AccessResponse ar = f.get(); // blocks until peer grants
            clock.onReceive(ar.getLamportTimestamp());
        }

        // Enter CS
        synchronized (lock) {
            state = State.HELD;
            System.out.printf("[Client %d] Entering CS ts=%d%n", id, clock.get());
        }

        // Send to printer (dumb server)
        PrintRequest printReq = PrintRequest.newBuilder()
                .setClientId(id)
                .setLamportTimestamp(clock.onSend())
                .setRequestNumber(reqNum)
                .setMessageContent(message)
                .build();
        PrintResponse printResp = printerStub.sendToPrinter(printReq);
        clock.onReceive(printResp.getLamportTimestamp());
        System.out.printf("[Client %d] Printed OK: %s%n", id, printResp.getConfirmationMessage());

        // Exit CS: grant all deferred, broadcast release
        List<Deferred> toGrant;
        synchronized (lock) {
            state = State.RELEASED;
            toGrant = new ArrayList<>(deferredQueue);
            deferredQueue.clear();
        }
        for (Deferred d : toGrant) {
            sendGrantResponse(d);
        }

        AccessRelease rel = AccessRelease.newBuilder()
                .setClientId(id)
                .setLamportTimestamp(clock.onSend())
                .setRequestNumber(reqNum)
                .build();

        // Fire-and-forget to peers
        for (MutualExclusionServiceGrpc.MutualExclusionServiceBlockingStub stub : peerBlockingStubs.values()) {
            try {
                stub.releaseAccess(rel);
            } catch (Exception ex) {
                // ignore errors on broadcast
            }
        }

        System.out.printf("[Client %d] Released CS%n", id);
    }

    private void sendGrantResponse(Deferred d) {
        try {
            long ts = clock.onSend();
            AccessResponse resp = AccessResponse.newBuilder()
                    .setAccessGranted(true)
                    .setLamportTimestamp(ts)
                    .build();
            d.observer.onNext(resp);
            d.observer.onCompleted();
        } catch (Exception e) {
            // observer may be cancelled if requester dropped
        }
    }

    private class MutualExclusionServiceImpl extends MutualExclusionServiceGrpc.MutualExclusionServiceImplBase {
        @Override
        public void requestAccess(AccessRequest request, StreamObserver<AccessResponse> responseObserver) {
            boolean defer;
            synchronized (lock) {
                clock.onReceive(request.getLamportTimestamp());
                defer = (state == State.HELD) || (state == State.WANTED && hasPriorityOver(request));
                if (defer) {
                    deferredQueue.add(new Deferred(request, responseObserver));
                    System.out.printf("[Client %d] Deferring to %d (ts=%d) state=%s%n", id, request.getClientId(), request.getLamportTimestamp(), state);
                } else {
                    long ts = clock.onSend();
                    AccessResponse resp = AccessResponse.newBuilder()
                            .setAccessGranted(true)
                            .setLamportTimestamp(ts)
                            .build();
                    responseObserver.onNext(resp);
                    responseObserver.onCompleted();
                    System.out.printf("[Client %d] Granted to %d immediately%n", id, request.getClientId());
                }
            }
        }

        @Override
        public void releaseAccess(AccessRelease request, StreamObserver<com.google.protobuf.Empty> responseObserver) {
            clock.onReceive(request.getLamportTimestamp());
            // No special action needed here per RA; releasing peer just for logging.
            System.out.printf("[Client %d] Peer %d released (req#%d) ts=%d%n", id, request.getClientId(), request.getRequestNumber(), request.getLamportTimestamp());
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private static class Deferred {
        final AccessRequest request;
        final StreamObserver<AccessResponse> observer;

        Deferred(AccessRequest request, StreamObserver<AccessResponse> observer) {
            this.request = request;
            this.observer = observer;
        }
    }

    public static void main(String[] args) throws Exception {
        int id = -1;
        int port = 0;
        String server = null;
        List<String> clients = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id":
                    id = Integer.parseInt(nextArg(args, ++i, "--id requires value"));
                    break;
                case "--server":
                    server = nextArg(args, ++i, "--server requires value (host:port)");
                    break;
                case "--port":
                    port = Integer.parseInt(nextArg(args, ++i, "--port requires value"));
                    break;
                case "--clients":
                    String list = nextArg(args, ++i, "--clients requires value");
                    if (!list.isBlank()) {
                        clients.addAll(Arrays.asList(list.split(",")));
                    }
                    break;
            }
        }

        if (id < 0 || port == 0 || server == null) {
            System.err.println("Usage: java ClientNode --id <int> --server <host:port> --port <int> --clients <host:port,host:port,...>");
            System.exit(1);
        }

        // Remove self from peers if included
        clients.removeIf(t -> t.endsWith(":" + port));

        ClientNode node = new ClientNode(id, port, server, clients);
        node.start();
    }

    private static String nextArg(String[] args, int idx, String err) {
        if (idx >= args.length) throw new IllegalArgumentException(err);
        return args[idx];
    }
}

