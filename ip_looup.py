import whois
import pandas as pd
import os

# === Config ===
output_file = "whois_results.xlsx"
domains = ["example.com", "openai.com", "python.org"]  # replace with your domain list

# === Check if file exists to load or create new DataFrame ===
if os.path.exists(output_file):
    df = pd.read_excel(output_file)
else:
    df = pd.DataFrame()

# === Process each domain ===
for domain in domains:
    try:
        w = whois.whois(domain)

        data = {
            "domain_name": w.domain_name,
            "registrar": w.registrar,
            "registrar_url": w.get("registrar_url", None),
            "reseller": w.get("reseller", None),
            "whois_server": w.whois_server,
            "referral_url": w.get("referral_url", None),
            "updated_date": str(w.updated_date) if w.updated_date else None,
            "creation_date": str(w.creation_date) if w.creation_date else None,
            "expiration_date": str(w.expiration_date) if w.expiration_date else None,
            "name_servers": ",".join(w.name_servers) if w.name_servers else None,
            "status": ",".join(w.status) if w.status else None,
            "emails": w.emails,
            "dnssec": w.dnssec,
            "name": w.name,
            "org": w.org,
            "address": w.address,
            "city": w.city,
            "state": w.state,
            "registrant_postal_code": w.get("registrant_postal_code", None),
            "country": w.country
        }

        # Append the result to dataframe
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

        # Save immediately after each domain to avoid data loss
        df.to_excel(output_file, index=False)
        print(f"Saved WHOIS for {domain}")

    except Exception as e:
        print(f"WHOIS lookup failed for {domain}: {e}")

print("✅ All lookups processed and saved.")
