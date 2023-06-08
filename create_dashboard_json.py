#%%
from metrics import MetricRegistry

import dotenv
dotenv.load_dotenv()

import os
import sys
import json
import yaml
import requests

#%%
GRAFANA_ENDPOINT = "http://localhost:3000/"
GRAFANA_TOKEN = os.environ["GRAFANA_TOKEN"]
POSTGRES_ENDPOINT = "my-postgresql-release.default.svc.cluster.local:5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

#%%
application_config = yaml.safe_load(open(sys.argv[1]))
application_id = application_config["application_metadata"]["id"]
application_name = application_config["application_metadata"]["name"]

#%%
response = requests.request(
    "POST", GRAFANA_ENDPOINT + "api/datasources",
    headers={
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {GRAFANA_TOKEN}'
    },
    data=json.dumps({
        "uid": application_id,
        "name": application_name,
        "type": "postgres",
        "access": "proxy",
        "url": POSTGRES_ENDPOINT,
        "user": POSTGRES_USER,
        "database": f"application_{application_id}",
        "basicAuth": False,
        "isDefault": False,
        "jsonData": {
            "sslmode": "disable"
        },
        "secureJsonData": {
            "password": POSTGRES_PASSWORD
        }
    })
)

print(response.text)

#%%
response = requests.request(
    "POST", GRAFANA_ENDPOINT + "api/folders",
    headers={
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {GRAFANA_TOKEN}'
    },
    data=json.dumps({
        "uid": application_id,
        "title": application_name
    })
)

print(response.text)

#%%
for dashboard_config in application_config["dashboards"]:
    dashboard_name = dashboard_config["name"]

    panels = []
    for metric_config in dashboard_config["metrics"]:
        metric_name = metric_config["name"]
        metric_layout = metric_config["layout"]
        metric = MetricRegistry.get(metric_name)()
        panels.extend(metric.get_grafana_panel_json(
            application_id=application_id,
            layout=metric_layout
        ).values())

    dashboard_json = {
        "description": "Example python-generated dashboard",
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "links": [],
        "liveNow": False,
        "panels": panels,
        "refresh": "",
        "style": "dark",
        "time": {
            "from": "now-3d",
            "to": "now"
        },
        "timepicker": {
            "hidden": False,
            "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"],
            "time_options": ["5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"]
        },
        "timezone": "browser",
        "title": dashboard_name,
        "weekStart": ""
    }

    response = requests.request(
        "POST", GRAFANA_ENDPOINT + "api/dashboards/db",
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {GRAFANA_TOKEN}'
        },
        data=json.dumps({
            "folderUid": application_id,
            "dashboard": dashboard_json,
            "message": "dashboard updated",
            "overwrite": True
        })
    )

    print(response.text)
