# Sistema de Impressão Distribuída (Java gRPC)

Implementação em Java do trabalho prático: servidor de impressão "burro" e clientes inteligentes que coordenam acesso exclusivo via Ricart–Agrawala com relógios lógicos de Lamport.

## Requisitos

- Java 17+
- Gradle instalado (ou use seu IDE com suporte a Gradle)

## Estrutura

- `src/main/proto/printing.proto`: Interface gRPC (PrintingService + MutualExclusionService)
- Servidor burro: `com.example.distributedprinting.server.PrinterServer`
- Cliente inteligente: `com.example.distributedprinting.client.ClientNode`
- Relógio de Lamport: `com.example.distributedprinting.client.LamportClock`

## Build

Gerar classes Java a partir dos .proto e compilar:

```
gradle build
```

O plugin `protobuf` cuida do codegen gRPC.

## Execução (exemplo com 3 clientes)

Terminal 1 – Servidor de Impressão (porta 50051):

```
scripts\run_printer.bat 50051
```

Terminal 2 – Cliente 1 (porta 50052):

```
scripts\run_client.bat 1 50052 localhost:50051 localhost:50053,localhost:50054
```

Terminal 3 – Cliente 2 (porta 50053):

```
scripts\run_client.bat 2 50053 localhost:50051 localhost:50052,localhost:50054
```

Terminal 4 – Cliente 3 (porta 50054):

```
scripts\run_client.bat 3 50054 localhost:50051 localhost:50052,localhost:50053
```

Os clientes executam um servidor gRPC (MutualExclusionService) na sua porta e fazem chamadas:

- para o servidor "burro" (PrintingService)
- para os pares (MutualExclusionService)

## Como funciona

- Ricart–Agrawala: ao querer imprimir, um cliente muda para estado WANTED, incrementa seu relógio (Lamport) e envia `RequestAccess` a todos os pares. O servidor do par pode:
  - Responder imediatamente (grant) se estiver em `RELEASED` ou se não tiver prioridade no empate `WANTED` (ordem por `(timestamp, id)`).
  - Adiar a resposta (mantém a chamada aberta) se estiver `HELD` ou tiver prioridade (estado `WANTED` com `(timestamp, id)` menor). Quando o par sai da seção crítica, responde todos os pedidos adiados.
- Se todos os pares responderem (grants), o cliente entra na seção crítica e envia `SendToPrinter` para o servidor burro.
- Ao sair, o cliente libera todos os adiados e ainda envia `ReleaseAccess` (broadcast) para logging/sinalização.

O servidor burro apenas imprime `[TS: {timestamp}] CLIENTE {id}: {mensagem}`, aguarda 2–3s, e confirma.

## Observações

- Implementação usa unary RPC com resposta adiada no `RequestAccess`, aderindo ao protocolo sugerido.
- O `ReleaseAccess` é usado apenas como notificação (não interfere na concessão, que ocorre ao sair da seção crítica).
- Ports padrão do exemplo: impressora `50051`, clientes `50052+`.

