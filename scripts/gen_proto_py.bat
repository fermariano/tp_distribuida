@echo off
setlocal
REM Generate Python gRPC stubs from proto
python -m grpc_tools.protoc -Ipython --python_out=python --grpc_python_out=python python/printing.proto
if %ERRORLEVEL% NEQ 0 (
  echo Failed to generate Python gRPC code. Ensure grpcio-tools is installed: pip install -r python\requirements.txt
  exit /b 1
)
echo Generated Python gRPC code in python\printing_pb2*.py

