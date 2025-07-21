#!/bin/bash

# === Config ===
input_file="looku_file/lookup.txt"
output_csv="raw_whois_results.csv"
whois_path="$HOME/go/bin/whois"
parallel_jobs=10  # Number of WHOIS requests to run in parallel

# === Prepare output CSV ===
if [ ! -f "$output_csv" ]; then
    echo "domain,whois_data" > "$output_csv"
fi

# === Function to run WHOIS and append to CSV safely ===
whois_lookup() {
    domain="$1"
    tmpfile=$(mktemp)

    # Skip if already processed
    if grep -q "^\"$domain\"," "$output_csv"; then
        echo "Skipping $domain (already in output)"
        return
    fi

    echo "Processing $domain..."

    result=$($whois_path "$domain" 2>&1)

    # Check for failure
    if echo "$result" | grep -qiE "error|rate|limit|refused|denied|failed"; then
        echo "$domain,ERROR" >> whois_errors.csv
        echo "Error for $domain"
        return
    fi

    # Escape quotes and newlines
    escaped=$(echo "$result" | sed ':a;N;$!ba;s/\r//g;s/\n/\\n/g' | sed 's/"/""/g')

    # Append to temp file (to avoid partial writes)
    echo "\"$domain\",\"$escaped\"" >> "$tmpfile"

    # Move temp file into output safely
    cat "$tmpfile" >> "$output_csv"
    rm "$tmpfile"
}

export -f whois_lookup
export whois_path output_csv

# === Run lookups in parallel ===
cat "$input_file" | grep -v '^#' | grep -v '^\s*$' | sort | uniq | parallel -j "$parallel_jobs" whois_lookup {}
