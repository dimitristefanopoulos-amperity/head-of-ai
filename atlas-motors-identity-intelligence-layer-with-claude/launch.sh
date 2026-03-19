#!/bin/bash
cd "$(dirname "$0")"

if [ ! -f .env ]; then
    echo ""
    echo "  ⚠  No .env file found."
    echo "  Copy .env.example to .env and fill in your credentials:"
    echo ""
    echo "    cp .env.example .env"
    echo ""
    exit 1
fi

pip install -q -r requirements.txt --break-system-packages 2>/dev/null
python3 app.py
