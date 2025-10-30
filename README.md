# Sistema de Impressão Distribuída (Python gRPC)

Implementação em Python do trabalho prático: servidor de impressão “burro” e clientes inteligentes que coordenam acesso exclusivo via Ricart–Agrawala com relógios lógicos de Lamport.

## Requisitos

- Python 3.10+

## Estrutura

- `python/printing.proto`: Interface gRPC (PrintingService + MutualExclusionService)
- Servidor burro: `python/printer_server.py`
- Cliente inteligente: `python/client_node.py`
- Relógio de Lamport: `python/lamport.py`
- Scripts (Windows): `scripts/*.bat`

## Instalação

1) Instale as dependências Python:

```
pip install -r python/requirements.txt
```

2) Gere os stubs gRPC para Python (uma vez):

```
scripts\gen_proto_py.bat
```

## Execução (3 clientes)

- Terminal 1 – Servidor de Impressão (porta 50051):

```
scripts\run_printer_py.bat 50051
```

- Terminal 2 – Cliente 1 (porta 50052):

```
scripts\run_client_py.bat 1 50052 localhost:50051 localhost:50053,localhost:50054
```

- Terminal 3 – Cliente 2 (porta 50053):

```
scripts\run_client_py.bat 2 50053 localhost:50051 localhost:50052,localhost:50054
```

- Terminal 4 – Cliente 3 (porta 50054):

```
scripts\run_client_py.bat 3 50054 localhost:50051 localhost:50052,localhost:50053
```
ATENÇÃO! Prof pode ser que dê um errinho quando começar a iniciar os clientes porque pra funcionar certinho precisa iniciar todos ok... mas só iniciar todos que funciona ta...

## Como funciona

- Ricart–Agrawala: ao querer imprimir, um cliente muda para estado WANTED, incrementa seu relógio (Lamport) e envia `RequestAccess` a todos os pares. O servidor do par pode:
  - Responder imediatamente (grant) se estiver em `RELEASED` ou se não tiver prioridade no empate `WANTED` (ordem `(timestamp, id)`).
  - Adiar a resposta (mantém a chamada aberta) se estiver `HELD` ou tiver prioridade (estado `WANTED` com `(timestamp, id)` menor). Ao sair da seção crítica, responde todos os pedidos adiados.
- Se todos responderem (grants), o cliente entra na seção crítica e envia `SendToPrinter` para o servidor burro.
- Ao sair, o cliente libera todos os adiados e envia `ReleaseAccess` (broadcast) para logging/sinalização.

O servidor burro apenas imprime `[TS: {timestamp}] CLIENTE {id}: {mensagem}`, aguarda ~2–3s, e confirma.

## Observações

- Unary RPC com resposta adiada no `RequestAccess`, aderindo ao protocolo sugerido.
- `ReleaseAccess` é só notificação (não interfere na concessão).
- Portas padrão do exemplo: impressora `50051`, clientes `50052+`.

