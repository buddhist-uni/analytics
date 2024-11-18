#!/bin/python

import argparse
import asyncio
import csv
import json
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import urllib.parse
from functools import cache
from bing_webmaster_tools import Settings, BingWebmasterClient

import ga4
import searchconsole

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

# Imported from obu/strutils.py
def fully_encode_url(url):
    # Split the URL into components
    scheme, netloc, path, params, query, fragment = urllib.parse.urlparse(url)

    # Encode the path
    path = urllib.parse.quote(urllib.parse.unquote(path), safe='/')

    # Manually reconstruct the URL to avoid urllib.parse.urlunparse undoing the encoding
    result = scheme + '://' + netloc
    if path:
        result += path
    if params:
        result += ';' + params
    if query:
        result += '?' + query
    if fragment:
        result += '#' + fragment
    return result

@cache
def get_metadata():
  with open("data/metadata.yaml", "r") as metafile:
    metadata = yaml.load(metafile, Loader=yaml.FullLoader)
  return metadata

def get_first_api_date(metadata: dict = None):
  if not metadata:
    metadata = get_metadata()
  last_archive_date = datetime.strptime(metadata["ga4_data"]["end_date"], DATE_FORMAT)
  return last_archive_date + timedelta(days=1)

def get_sc_api_date(metadata: dict = None):
  if not metadata:
    metadata = get_metadata()
  last_archive_date = datetime.strptime(metadata["sc_data"]["end_date"], DATE_FORMAT)
  return last_archive_date + timedelta(days=1)

async def fetch_all_bing_data():
  async with BingWebmasterClient(Settings.from_env()) as client:
    page_stats = await client.traffic.get_page_stats("https://buddhistuniversity.net")
  return page_stats

def fetch_new_bing_data(metadata: dict = None):
  if not metadata:
    metadata = get_metadata()
  page_stats = asyncio.run(fetch_all_bing_data())
  end_date = datetime.strptime(metadata['bing_data']['end_date'], DATE_FORMAT) + timedelta(days=1)
  return [p for p in page_stats if p.clicks > 0 and p.query.endswith(".pdf") and p.date >= end_date]

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

  sc_from_date = get_sc_api_date()
  print(f"Fetching data from Search Console since {sc_from_date.strftime(DATE_FORMAT)}...")
  sc_data = []
  with open("data/sc_data.csv", "r") as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      sc_data.append({
        'URL': row['URL'],
        'clicks': int(row['clicks']),
      })
  sc_to_date = datetime.now() - timedelta(days=2)
  if sc_from_date > sc_to_date:
    print("  No new data to fetch")
  else:
    new_sc_data = dict()
    for bucket in get_metadata()['content_buckets']:
      new_sc_data.update(searchconsole.get_clicks_per_link(
        sc_from_date,
        sc_to_date,
        pathContains=bucket,
      ))
    sc_data.extend({
      'URL': url,
      'clicks': int(clicks)
    } for url, clicks in new_sc_data.items())
  
  # add Bing data to SC data
  print("Fetching data from Bing Webmaster Tools...")
  pdf_clicks = fetch_new_bing_data()
  for p in pdf_clicks:
    sc_data.append({
      'URL': fully_encode_url(p.query),
      'clicks': p.clicks,
    })
  with open('data/bing_data.csv', 'r') as file:
    csvreader = csv.DictReader(file)
    for row in csvreader:
      sc_data.append({
        'URL': fully_encode_url(row['URL']),
        'clicks': int(row['clicks'])
      })

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
  print("Merging in Google and Bing Search data...")
  url_to_content = json.load(open("data/content_paths.json", "r"))
  for row in sc_data:
    downloads = row['clicks']
    # The Google Search API doesn't always escape these
    url = row['URL']\
      .replace("'", "%27")\
      .replace("(", "%28")\
      .replace(")", "%29")
    if 'patanjali-yoga-sutra' in url:
      downloaders['canon/yogasutra_patanjali'] += downloads
      continue
    if '/smallpdfs/' in url:
      url = "https://smallpdfs.buddhistuniversity.net" + url[url.index('/smallpdfs/')+10:]
    if url in url_to_content:
      cp = url_to_content[url]
      if cp not in downloaders:
        downloaders[cp] = downloads
      else:
        downloaders[cp] += downloads
    else:
      print(f"::warning title=Unknown content::Could not find {row['URL']} in content_paths.json. Rerurn obu/update_site_data.py?")

  print("Writing data to files...")
  for folder in CONTENT_FOLDERS:
    (content_folder / folder).mkdir(parents=True, exist_ok=True)
  for page, downloads in downloaders.items():
    filename = content_folder / (page+".download_count")
    filename.write_text(str(downloads))

  (content_folder / "download_counts.json").write_text(json.dumps(downloaders))
  (arguments.dest / "link_counts.json").write_text(json.dumps(link_counts))
