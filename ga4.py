#!/bin/python

from pathlib import Path
import csv
import textwrap
import os

def get_default_property_id():
  try:
    return os.environ["GA4_PROPERTY_ID"]
  except KeyError:
    print("Please set the GA4_PROPERTY_ID environment variable")
    quit(1)

try:
  from google.analytics import data_v1beta
  from google.auth.exceptions import DefaultCredentialsError
  from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    MetricType,
    RunReportRequest,
    RunReportResponse,
  )
except ModuleNotFoundError:
  print("pip install google-api-python-client google-analytics-data")
  quit(1)

try:
  client = data_v1beta.BetaAnalyticsDataClient()
except DefaultCredentialsError:
  print("""Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable
  to the path of your service account private key json file.""")
  exit(1)

def generate_metadata_files(directory: Path = Path("./"), property_id: str = None):
  directory = directory.expanduser()
  if not property_id:
    property_id = get_default_property_id()
  metadata = client.get_metadata(data_v1beta.GetMetadataRequest(
    name=f"properties/{property_id}/metadata"
  ))
  filename = directory / f"{property_id}_metrics.txt"
  with open(filename, "w") as file:
    file.write("ALL METRICS UNDERSTOOD BY THE GOOGLE ANALYTICS API\n")
    file.write("--------------------------------------------------\n")
    for metric in metadata.metrics:
      file.write(f"\n# {metric.ui_name}\n")
      file.write("\n".join(
        textwrap.wrap(
          metric.description,
          60, 
          initial_indent="  ",
          subsequent_indent="  ",
          break_on_hyphens=False,
          break_long_words=False,
        )
      ))
      file.write(f"\n\n  USE: {metric.api_name} is a {metric.category} metric of {metric.type_.name}\n")
      if len(metric.deprecated_api_names) > 0:
        file.write(f"  ({metric.api_name} used to be known as {metric.deprecated_api_names[0]})\n")
      file.write("\n")
  print("Wrote " + str(filename))
  filename = directory / f"{property_id}_dimensions.txt"
  with open(filename, "w") as file:
    file.write("ALL DIMENSIONS UNDERSTOOD BY THE GOOGLE ANALYTICS API\n")
    file.write("-----------------------------------------------------\n")
    for dimension in metadata.dimensions:
      file.write(f"\n# {dimension.ui_name}\n")
      file.write("\n".join(
        textwrap.wrap(
          dimension.description,
          60,
          initial_indent="  ",
          subsequent_indent="  ",
          break_on_hyphens=False,
          break_long_words=False,
        )
      ))
      file.write(f"\n\n  USE: {dimension.api_name} is a ")
      file.write(f"{dimension.category} dimension.\n\n")
  print("Wrote " + str(filename))

def report_purchasers_per_itemid(
  start_date: str, # YYYY-MM-DD
  end_date: str, # YYYY-MM-DD
  property_id: str = None
):
  if not property_id:
    property_id = get_default_property_id()
  request = RunReportRequest(
    property=f"properties/{property_id}",
    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    dimensions=[Dimension(name="itemId")],
    metrics=[Metric(name="totalPurchasers")],
    return_property_quota=True,
    limit=250000, # 250k should be enough for anyone (who needs pagination?)
  )
  ret = client.run_report(request)
  if ret.row_count > 250000:
    raise NotImplementedError("Please add pagination logic to report_purchasers_per_itemid")
  return ret

def report_to_dict_list(report: RunReportResponse):
  rows = []
  for row in report.rows:
    row_data = {}
    for i in range(len(row.dimension_values)):
      row_data[report.dimension_headers[i].name] = row.dimension_values[i].value
    for i in range(len(row.metric_values)):
      m = report.metric_headers[i].name
      v = row.metric_values[i].value
      match report.metric_headers[i].type_:
        case MetricType.TYPE_INTEGER | MetricType.TYPE_MILLISECONDS:
          row_data[m] = int(v)
        case MetricType.TYPE_FLOAT | MetricType.TYPE_CURRENCY:
          row_data[m] = float(v)
        case _:
          row_data[m] = v
    rows.append(row_data)
  return rows

def aggregate_duplicate_itemids(
    rows: list[dict],
) -> list[dict]:
  r"""Merges rows with the same itemId into a single row.
  
  Does not modify the original rows.
  
  ASSUMES that itemId is the only dimension AND that all other
  cols are numerical metrics which are aggregated via addition
  and that each row has the same cols.

  Matches \av/(*)#10\ with av/(1) and adds their values.
  Discards \av/*#(>10)\

  Returns: a new list of dicts
  """
  ret = []
  ids_seen = dict()
  summable_metric_cols = list(rows[0].keys())
  summable_metric_cols.remove('itemId')
  for i in range(len(rows)):
    row = rows[i]
    if row['itemId'].startswith('av/') and '#' in row['itemId']:
      if row['itemId'].endswith("#10"):
        itemId = row["itemId"][:-3]
        if itemId in ids_seen:
          # copy-on-write
          ret[ids_seen[itemId]] = ret[ids_seen[itemId]].copy()
          for c in summable_metric_cols:
            ret[ids_seen[itemId]][c] += row[c]
        else:
          ids_seen[itemId] = len(ret)
          ret.append(row.copy())
          ret[len(ret)-1]['itemId'] = itemId
    else:
      if row['itemId'] in ids_seen:
        # copy-on-write
        new_row = ret[ids_seen[row['itemId']]].copy()
        for c in summable_metric_cols:
          new_row[c] += row[c]
        ret[ids_seen[row['itemId']]] = new_row
      else:
        ids_seen[row['itemId']] = len(ret)
        ret.append(row)
  return ret

def write_dict_list_to_csv_file(
  rows: list[dict],
  filepath: Path | str,
  fieldnames: list[str] = None, # specify to guarantee order
):
  if not fieldnames:
    fieldnames = rows[0].keys()
  filepath = Path(filepath).expanduser()
  filepath.parent.mkdir(parents=True, exist_ok=True)
  with open(filepath, "w", newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
