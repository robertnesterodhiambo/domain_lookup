import geohash

geohash_code = "spykdm7q"

# Center point
lat, lon, lat_err, lon_err = geohash.decode_exactly(geohash_code)
print(f"Center point: {lat}, {lon}")

# Bounding box
bbox = {
    "s": lat - lat_err,
    "n": lat + lat_err,
    "w": lon - lon_err,
    "e": lon + lon_err,
}
print("Bounding box:", bbox)
