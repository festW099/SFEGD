$content = @'
#!/bin/sh
set -e

echo "Starting bot..."

if [ ! -f "main.py" ]; then
    echo "Error: main.py not found!"
    exit 1
fi

exec python main.py
'@

[System.IO.File]::WriteAllText("$PWD\entrypoint.sh", $content.Replace("`r`n", "`n"))