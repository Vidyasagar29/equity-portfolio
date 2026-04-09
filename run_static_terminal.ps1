$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectDir

python export_terminal_data.py
python -m http.server 8765 --directory terminal
