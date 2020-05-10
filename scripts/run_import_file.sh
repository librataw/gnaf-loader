#!/bin/bash

sudo -u postgres psql -d places -f ./create_tables.sql &

while true
do
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    python3 ../gnaf_loader/import.py &
    wait
done
