@echo off
setlocal
set PORT=%1
if "%PORT%"=="" set PORT=50051
if not exist python\printing_pb2.py (
  call scripts\gen_proto_py.bat || exit /b 1
)
python python\printer_server.py --port %PORT%
