@echo off
setlocal
set PORT=%1
if "%PORT%"=="" set PORT=50051
where gradle >nul 2>nul
if %ERRORLEVEL%==0 (
  gradle run -PmainClass=com.example.distributedprinting.server.PrinterServer --args="--port %PORT%"
) else (
  echo Gradle not found. Please install Gradle or use an IDE.
  exit /b 1
)

