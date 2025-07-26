import subprocess
import json

# Run the curl command
result = subprocess.run(
    ["curl", "ipinfo.io/8.8.8.8"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

# Field name mapping (custom keys)
field_map = {
    "ip": "ip_geo",
    "hostname": "domain_geo",
    "city": "city_geo",
    "region": "region_geo",
    "country": "country_geo",
    "loc": "loc_geo",
    "org": "org_geo",
    "postal": "postal_geo",
    "timezone": "timezone_geo",
    "anycast": "anycast_geo"
}

# Check for errors
if result.returncode != 0:
    print("Error running curl:", result.stderr)
else:
    try:
        # Parse the JSON
        data = json.loads(result.stdout)

        # Print each key with mapped name (if exists)
        for key, value in data.items():
            label = field_map.get(key, key)  # Use mapped name or fallback to original
            print(f"{label}: {value}")

    except json.JSONDecodeError as e:
        print("JSON parsing error:", e)

