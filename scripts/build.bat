@echo off
setlocal
where gradle >nul 2>nul
if %ERRORLEVEL%==0 (
  gradle build
) else (
  echo Gradle not found. Please install Gradle or use an IDE.
  exit /b 1
)

