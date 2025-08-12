import pandas as pd

# Read CSV exactly as-is
df = pd.read_csv("data_rdap.csv")

# Function to generate RDAP link for .de domains
def make_rdap_link(domain):
    if isinstance(domain, str) and domain.lower().endswith(".de"):
        return f"https://rdap.denic.de/domain/{domain.lower()}"
    return None  # Leave blank if not a valid .de domain

# Update or create rdap_link column
df["rdap_link"] = df["domain"].apply(make_rdap_link)

# Save back to the same file with all original columns intact
df.to_csv("data_rdap.csv", index=False)

print("✅ RDAP links updated. All original columns preserved in data_rdap.csv")

