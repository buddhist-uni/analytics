#!/bin/python

from datetime import datetime
try:
  from googleapiclient.discovery import build
except ModuleNotFoundError:
  print("pip install google-api-python-client")
  quit(1)

DOMAIN = "sc-domain:buddhistuniversity.net"

try:
  client = build("searchconsole", "v1")
except Exception as e:
  print(e)
  quit(1)

def get_clicks_per_link(startDate, endDate, pathContains=None, siteUrl=DOMAIN):
  if isinstance(startDate, datetime):
    startDate = startDate.strftime("%Y-%m-%d")
  if isinstance(endDate, datetime):
    endDate = endDate.strftime("%Y-%m-%d")
  request = {
    "startDate": startDate,
    "endDate": endDate,
    "dimensions": ["page"],
    "startRow": 0,
    "rowLimit": 10000,
  }
  if pathContains:
    request["dimensionFilterGroups"] = [
      {
        "groupType": "and",
        "filters": [
          {
            "dimension": "page",
            "operator": "contains",
            "expression": pathContains
          }
        ]
      }
    ]
  try:
    response = client.searchanalytics().query(
      siteUrl=siteUrl,
      body=request
    ).execute()
  except Exception as e:
    print(e)
    quit(1)
  try:
    return {
      row["keys"][0]: row["clicks"]
      for row in response["rows"]
      if row["clicks"] > 0
    }
  except KeyError:
    # When there are no "rows" this means no data for this query
    return {}
