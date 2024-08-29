#!/bin/python

import argparse
import csv
import json
import yaml
from datetime import datetime, timedelta
from pathlib import Path

import ga4


DATE_FORMAT = "%Y-%m-%d"
CONTENT_FOLDERS = [
  "articles",
  "av",
  "booklets",
  "canon",
  "essays",
  "excerpts",
  "monographs",
  "papers",
  "reference",
]

argparser = argparse.ArgumentParser()
argparser.add_argument('-d', '--dest', type=Path, default=Path("./build"))
arguments = argparser.parse_args()
content_folder = arguments.dest / "content"

with open("data/metadata.yaml", "r") as metafile:
  metadata = yaml.load(metafile, Loader=yaml.FullLoader)
last_archive_date = datetime.strptime(metadata["ga4_data"]["end_date"], DATE_FORMAT)
first_api_date = last_archive_date + timedelta(days=1)

print(f"Fetching data from GA4 since {first_api_date.strftime(DATE_FORMAT)}...")
report = ga4.report_purchasers_per_itemid(
  first_api_date.strftime(DATE_FORMAT),
  "today",
)
downloaders = ga4.report_to_dict_list(report)

print("Merging with archival data...")
with open("data/ga4_data.csv", "r") as file:
  csvreader = csv.DictReader(file)
  for row in csvreader:
    downloaders.append({
      'itemId': row['itemId'],
      'totalPurchasers': int(row['totalPurchasers']),
    })
downloaders = ga4.aggregate_duplicate_itemids(downloaders)

def is_link(itemId):
  return ":" in itemId or itemId.startswith("tags/")
link_counts = {
  r['itemId']: r['totalPurchasers']
  for r in downloaders
  if is_link(r['itemId'])
}
downloaders = {
  r['itemId']: r['totalPurchasers']
  for r in downloaders
  if not is_link(r['itemId'])
}

print("Writing data to files...")
for folder in CONTENT_FOLDERS:
  (content_folder / folder).mkdir(parents=True, exist_ok=True)
with open("data/ua_data.csv", "r") as file:
  csvreader = csv.DictReader(file)
  for row in csvreader:
    filename = content_folder / (row["Page"]+".download_count")
    downloads = int(row['SCALED EVENTS'])
    if row["Page"] in downloaders:
      downloads += downloaders[row["Page"]]
      del downloaders[row["Page"]]
    filename.write_text(str(downloads))

if len(downloaders) == 0:
  raise ValueError("No new items found beyond UA data")
for page, downloads in downloaders.items():
  filename = content_folder / (page+".download_count")
  filename.write_text(str(downloads))

(arguments.dest / "link_counts.json").write_text(json.dumps(link_counts))
