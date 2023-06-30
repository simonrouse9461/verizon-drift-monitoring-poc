#!/usr/bin/env bash

/opt/homebrew/anaconda3/bin/python create_database_tables.py "$1"
/opt/homebrew/anaconda3/bin/python create_dashboard_json.py "$1"
