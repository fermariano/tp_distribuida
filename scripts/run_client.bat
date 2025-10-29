@echo off
setlocal
if "%1"=="" (
  echo Usage: run_client.bat ^<id^> ^<port^> ^<printer_host:port^> ^<peer1:port,peer2:port,...^>
  exit /b 1
)
set ID=%1
set PORT=%2
set SERVER=%3
set CLIENTS=%4
where gradle >nul 2>nul
if %ERRORLEVEL%==0 (
  gradle run -PmainClass=com.example.distributedprinting.client.ClientNode --args="--id %ID% --server %SERVER% --port %PORT% --clients %CLIENTS%"
) else (
  echo Gradle not found. Please install Gradle or use an IDE.
  exit /b 1
)

