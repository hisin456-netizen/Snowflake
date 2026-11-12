@echo off
rem Snowflake MCP wrapper. Loads SNOWFLAKE_* from the repo-root .env, then starts the server.
rem Keep this file ASCII only (cmd.exe mis-parses UTF-8 Korean). Print nothing: stdout is the MCP channel.

setlocal
set "ROOT=D:\projects\airflow-lab"

for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ROOT%\.env") do (
  if /i "%%a"=="SNOWFLAKE_ACCOUNT" set "SNOWFLAKE_ACCOUNT=%%b"
  if /i "%%a"=="SNOWFLAKE_USER" set "SNOWFLAKE_USER=%%b"
  if /i "%%a"=="SNOWFLAKE_PAT" set "SNOWFLAKE_PASSWORD=%%b"
)

rem Role and warehouse are not secrets. A PAT does not evaluate secondary roles, so pin a single role.
set "SNOWFLAKE_ROLE=ROLE_MCP_RO"
set "SNOWFLAKE_WAREHOUSE=DW_WH"

rem config.yaml has Korean comments; make Python read files as UTF-8 instead of cp949.
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

"C:\Users\LG\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uvx.exe" snowflake-labs-mcp --service-config-file "%ROOT%\.claude\snowflake-mcp\config.yaml"
