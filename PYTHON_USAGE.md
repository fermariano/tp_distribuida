# Execução em Python (Ricart-Agrawala + Lamport)

## 1) Dependências

- Python 3.10+
- Instale libs:

```
pip install -r python/requirements.txt
```

## 2) Gerar stubs gRPC (uma vez)

```
scripts\gen_proto_py.bat
```

## 3) Executar

- Terminal 1 (Servidor de Impressão):
```
scripts\run_printer_py.bat 50051
```

- Terminal 2 (Cliente 1):
```
scripts\run_client_py.bat 1 50052 localhost:50051 localhost:50053,localhost:50054
```

- Terminal 3 (Cliente 2):
```
scripts\run_client_py.bat 2 50053 localhost:50051 localhost:50052,localhost:50054
```

- Terminal 4 (Cliente 3):
```
scripts\run_client_py.bat 3 50054 localhost:50051 localhost:50052,localhost:50053
```

O servidor "burro" apenas imprime `[TS: {timestamp}] CLIENTE {id}: {mensagem}`, aguarda ~2–3s e confirma. Os clientes implementam Ricart–Agrawala com relógios de Lamport.

