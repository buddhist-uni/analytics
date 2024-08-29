#!/bin/python

import argparse
from pathlib import Path
import csv

argparser = argparse.ArgumentParser()
argparser.add_argument('-d', '--dest', type=Path, default=Path("./build"))
arguments = argparser.parse_args()

content_folder = arguments.dest / "content"

with open("data/ua_data.csv", "r") as file:
  csvreader = csv.DictReader(file)
  for row in csvreader:
    filename = content_folder / (row["Page"]+".download_count")
    filename.parent.mkdir(parents=True, exist_ok=True)
    filename.write_text(row["SCALED EVENTS"])
