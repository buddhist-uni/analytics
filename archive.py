#!/bin/python

import csv
import yaml
from datetime import datetime, timedelta

import ga4
import searchconsole
from build import (
  get_metadata,
  get_first_api_date,
  get_sc_api_date,
  DATE_FORMAT,
  merge_new_report_with_old_data,
  fetch_new_bing_data,
)

metadata = get_metadata()
first_api_date = get_first_api_date(metadata)
archive_until = datetime.today() - timedelta(days=7)
if first_api_date > archive_until:
  print("No new data to archive")
  quit(0)
print(f"Fetching GA4 data from {first_api_date.strftime(DATE_FORMAT)} to {archive_until.strftime(DATE_FORMAT)}...")
report = ga4.report_purchasers_per_itemid(
  first_api_date.strftime(DATE_FORMAT),
  archive_until.strftime(DATE_FORMAT),
)

print("Merging new data into the archive...")
new_data = merge_new_report_with_old_data(report)
with open("data/ga4_data.csv", "w") as file:
  csvwriter = csv.DictWriter(file, fieldnames=['itemId', 'totalPurchasers'])
  csvwriter.writeheader()
  for row in new_data:
    csvwriter.writerow(row)
metadata['ga4_data']['end_date'] = archive_until.strftime(DATE_FORMAT)

sc_from_date = get_sc_api_date(metadata)
print(f"Fetching Search Console data from {sc_from_date.strftime(DATE_FORMAT)} to {archive_until.strftime(DATE_FORMAT)}...")
sc_data = dict()
for bucket in metadata['content_buckets']:
  sc_data.update(
    searchconsole.get_clicks_per_link(
      sc_from_date,
      archive_until,
      pathContains=bucket,
    )
  )

print("Merging new SC data into the archive...")
with open("data/sc_data.csv", "r") as file:
  csvreader = csv.DictReader(file)
  for row in csvreader:
    if row['URL'] in sc_data:
      sc_data[row['URL']] += int(row['clicks'])
    else:
      sc_data[row['URL']] = int(row['clicks'])
with open("data/sc_data.csv", "w") as file:
  csvwriter = csv.DictWriter(file, fieldnames=['URL', 'clicks'])
  csvwriter.writeheader()
  for url, clicks in sc_data.items():
    csvwriter.writerow({'URL': url, 'clicks': clicks})
metadata['sc_data']['end_date'] = archive_until.strftime(DATE_FORMAT)

print("Merging new Bing data into the archive...")
pdf_clicks = fetch_new_bing_data(metadata)
if not pdf_clicks:
  print("  No new data to merge")
else:
  new_end_date = max(p.date for p in pdf_clicks)
  metadata['bing_data']['end_date'] = new_end_date.strftime(DATE_FORMAT)
  newurls = {p.query for p in pdf_clicks}
  newbingclicks = {url: sum(p.clicks for p in pdf_clicks if p.query == url) for url in newurls}
  oldurls = set()
  oldbingclicks = dict()
  with open("data/bing_data.csv", "r") as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      oldurls.add(row['URL'])
      oldbingclicks[row['URL']] = int(row['clicks'])
  for url, clicks in newbingclicks.items():
    if url in oldurls:
      oldbingclicks[url] += clicks
    else:
      oldbingclicks[url] = clicks
  with open("data/bing_data.csv", "w") as file:
    csvwriter = csv.DictWriter(file, fieldnames=['URL', 'clicks'])
    csvwriter.writeheader()
    for url, clicks in oldbingclicks.items():
      csvwriter.writerow({'URL': url, 'clicks': clicks})

yaml.dump(
  metadata,
  open('data/metadata.yaml', 'w'),
  sort_keys=True,
  indent=2,
)
