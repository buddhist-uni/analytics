#!/bin/python

import csv
import yaml
from datetime import datetime, timedelta

import ga4
from build import (
  get_metadata,
  get_first_api_date,
  DATE_FORMAT,
  merge_new_report_with_old_data,
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
yaml.dump(metadata, open('data/metadata.yaml', 'w'))