# 🕵️ Domain Data Processing Pipeline

This project processes large-scale domain data from raw downloads to database insertion. It consists of several scripts that work in a specific sequence to download, parse, enrich, and store domain-related information.

---

## 📂 Project Structure

### 📥 **Initial Data Preparation**

These scripts handle the collection and structuring of raw data:

1. **`download.py`**
   Downloads all domain-related data files.

2. **`unzip.py`**
   Extracts downloaded archive files into `.txt` format for processing.

3. **`domaincount.py`**
   Counts all collected domains and produces a domain summary.

4. **`jsonlin.py`**
   Downloads RDAP JSON links corresponding to each domain.

5. **`rdapbuild.py`**
   Constructs and prepares RDAP URLs from the JSON data for querying.

---

### 🔍 **Domain Data Processing**

After RDAP links are ready, these scripts take over to extract and enrich domain metadata:

6. **`query.py`**
   Queries RDAP data and saves parsed results to:
   ➤ `data_rdap_parsed.csv`

7. **`subdomaincount3.py`**
   Processes the RDAP data to extract subdomain counts and outputs:
   ➤ `domain_count.csv`

8. **`nslookup.py`**
   Performs NS lookups on domains and saves results to:
   ➤ `nslookup.csv`

9. **`pagecount.py`**
   Estimates and logs webpage counts per domain into:
   ➤ `page_count.csv`

10. **`violations.js`**
    JavaScript-based script to detect content violations per domain.
    ➤ Outputs: `violations.csv`

11. **`geo2.py`**
    Geolocates IPs and domains, then compiles full enriched data into:
    ➤ `complete.csv`

---

### 🛢️ **Database Insertion**

12. **`insert.py`**
    Inserts the cleaned and enriched domain data into a MySQL or other configured database.

---


## Runing code 
Always run `Main.py`. to start collection and Runing all codes in unison 
For the application UI I have it saved in system file so it starts automatically when you restart server no need to worry about it just visit 
[Domains filter and Download](http://46.62.140.165:5000/) 