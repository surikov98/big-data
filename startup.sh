#!/usr/bin/env bash
export PYTHONUNBUFFERED=1
uwsgi --http 0.0.0.0:5000 --wsgi-file app_main.py --callable app --threads $WORKERS
