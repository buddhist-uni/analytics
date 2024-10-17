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

def get_metadata():
  with open("data/metadata.yaml", "r") as metafile:
    metadata = yaml.load(metafile, Loader=yaml.FullLoader)
  return metadata

def get_first_api_date(metadata: dict = None):
  if not metadata:
    metadata = get_metadata()
  last_archive_date = datetime.strptime(metadata["ga4_data"]["end_date"], DATE_FORMAT)
  return last_archive_date + timedelta(days=1)

def merge_new_report_with_old_data(report: ga4.RunReportResponse) -> list[dict]:
  downloaders = ga4.report_to_dict_list(report)
  with open("data/ga4_data.csv", "r") as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      downloaders.append({
        'itemId': row['itemId'],
        'totalPurchasers': int(row['totalPurchasers']),
      })
  return ga4.aggregate_duplicate_itemids(downloaders)

if __name__ == "__main__":
  argparser = argparse.ArgumentParser()
  argparser.add_argument('-d', '--dest', type=Path, default=Path("./build"))
  arguments = argparser.parse_args()
  content_folder = arguments.dest / "content"

  first_api_date = get_first_api_date()

  print(f"Fetching data from GA4 since {first_api_date.strftime(DATE_FORMAT)}...")
  report = ga4.report_purchasers_per_itemid(
    first_api_date.strftime(DATE_FORMAT),
    "today",
  )

  print("Merging with archival data...")
  downloaders = merge_new_report_with_old_data(report)
  def is_link(itemId):
    return ":" in itemId or itemId.startswith("tags/")
  # Split into link and content clicks
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
  # Merge in ancient UA Data
  with open("data/ua_data.csv", "r") as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      downloads = int(row['SCALED EVENTS'])
      if row["Page"] in downloaders:
        downloaders[row["Page"]] += downloads
      else:
        downloaders[row["Page"]] = downloads
  # Merge in Search Console data
  print("Merging in Search Console data...")
  url_to_content = json.load(open("data/content_paths.json", "r"))
  with open("data/sc_data.csv", "r") as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      downloads = int(row['clicks'])
      # The Google Search API doesn't always escape these
      url = row['URL']\
        .replace("'", "%27")\
        .replace("(", "%28")\
        .replace(")", "%29")
      if 'patanjali-yoga-sutra' in url:
        downloaders['canon/yogasutra_patanjali'] += downloads
      elif url in url_to_content:
        cp = url_to_content[url]
        if cp not in downloaders:
          downloaders[cp] = downloads
        else:
          downloaders[cp] += downloads
      else:
        print(f"  Could not find {row['URL']} in content_paths.json")

  print("Writing data to files...")
  for folder in CONTENT_FOLDERS:
    (content_folder / folder).mkdir(parents=True, exist_ok=True)
  for page, downloads in downloaders.items():
    filename = content_folder / (page+".download_count")
    filename.write_text(str(downloads))

  (content_folder / "download_counts.json").write_text(json.dumps(downloaders))
  (arguments.dest / "link_counts.json").write_text(json.dumps(link_counts))
