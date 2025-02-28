import json
import re
from collections import defaultdict

# Load station data
with open('california.json', 'r') as f:
    stations = json.load(f)

# Create a lookup dictionary for station IDs to names
station_dict = {station['id']: station['name'] for station in stations}

# Parse error log and group by station and message
pattern = r"Error: Station (\d+), year (\d+): (.+)"
station_errors = defaultdict(lambda: defaultdict(list))

with open('error_log.txt', 'r') as f:
    for line in f:
        match = re.match(pattern, line)
        if match:
            station_id, year, message = match.groups()
            station_errors[station_id][message].append(year)

# Display grouped results
for station_id, message_years in station_errors.items():
    station_name = station_dict.get(station_id, "Unknown Station")
    print(f"Station {station_id} ({station_name}):")
    
    for message, years in message_years.items():
        # For "No predictions data" messages, simplify to "Missing"
        display_message = "Missing" if message == "No predictions data" else message
        years_str = ", ".join(sorted(years))
        print(f"  {display_message}: {years_str}")
    
    print()
