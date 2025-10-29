@echo off
setlocal
if "%1"=="" (
  echo Usage: run_client_py.bat ^<id^> ^<port^> ^<printer_host:port^> ^<peer1:port,peer2:port,...^>
  exit /b 1
)
set ID=%1
set PORT=%2
set SERVER=%3
set CLIENTS=%4
if not exist python\printing_pb2.py (
  call scripts\gen_proto_py.bat || exit /b 1
)
python python\client_node.py --id %ID% --server %SERVER% --port %PORT% --clients %CLIENTS%

