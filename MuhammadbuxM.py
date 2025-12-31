import csv, json, io, sys, zipfile
from urllib.request import urlopen
from collections import defaultdict
from datetime import datetime

URL = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"

# Month abbreviation to number mapping
month_map = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
    'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}


def parse_date(date_str):
    """Convert 'Jan-15' to '2015-01-01'"""
    month_abbr, year = date_str.split('-')
    month = month_map.get(month_abbr, '01')
    # Handle 2-digit year
    if len(year) == 2:
        year = '20' + year if int(year) < 50 else '19' + year
    return f"{year}-{month}-01"


with urlopen(URL) as r:
    with zipfile.ZipFile(io.BytesIO(r.read())) as z:
        for name in z.namelist():
            if name.endswith('.csv'):
                with z.open(name) as f:
                    data = defaultdict(list)
                    fields_map = {}
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))

                    for row in reader:
                        # Parse date from "Jan-15" format
                        date = parse_date(row['TIME_PERIOD'])

                        # Create unique key for this series
                        key = (row['REF_AREA'], row['ENERGY_PRODUCT'],
                               row['FLOW_BREAKDOWN'], row['UNIT_MEASURE'])

                        # Add data point
                        try:
                            value = float(row['OBS_VALUE'])
                            data[key].append([date, value])

                            # Store metadata once per series
                            if key not in fields_map:
                                fields_map[key] = {
                                    'REF_AREA': row['REF_AREA'],
                                    'ENERGY_PRODUCT': row['ENERGY_PRODUCT'],
                                    'FLOW_BREAKDOWN': row['FLOW_BREAKDOWN'],
                                    'UNIT_MEASURE': row['UNIT_MEASURE'],
                                    'ASSESSMENT_CODE': row['ASSESSMENT_CODE']
                                }
                        except (ValueError, KeyError) as e:
                            # Skip rows with invalid data
                            continue

                    # Output each series as JSON
                    for key, points in data.items():
                        series = {
                            'series_id': '\\'.join(key),
                            'points': sorted(points, key=lambda x: x[0]),  # Sort by date
                            'fields': fields_map[key]
                        }
                        print(json.dumps(series))