# 🌐 Domain Intelligence Platform

A scalable system that ingests, processes, and analyzes domain data — including WHOIS info, DNS records, website availability, accessibility, and subdomain counts. Supports flexible filtering through a web-based frontend.

---

## 📦 Features

- ✅ **350M+ domains** in database (with 1.2M+ daily updates)
- 🗂️ **WHOIS** and **NSLOOKUP** lookup for every domain
- 🔄 Tracks **new and deleted** domains daily
- 🌍 **Geolocation** from WHOIS and web hosting data
- 🕷️ Crawl websites and count number of pages (via sitemap or free crawlers)
- ♿ **Accessibility check** (via open source tools)
- 🌐 Count **subdomains** using open-source GitHub tools
- 🧪 Filter and search using:
  - TLD (Top-Level Domain)
  - Country from WHOIS
  - Country from Web Host
  - Page Count
  - Subdomain Count
  - Accessibility Score
- 🔍 Supports AND / OR filters, and single/multi-criteria filtering
- 🖥️ Simple Web Frontend with powerful search

---

## 🏗️ Architecture Overview

1. **Downloader**
   - Downloads domain data and deletions (daily).
   - URL provided for regular updates.

2. **PostgreSQL Database**
   - Stores all domain metadata.
   - Indexed for fast filtering.

3. **Processing Pipeline**
   - WHOIS lookup (`whois domain.com`)
   - NSLOOKUP (`nslookup domain.com`)
   - Sitemap crawling & page count
   - Accessibility score (via free GitHub tools)
   - Subdomain enumeration

4. **Web Frontend**
   - Filter interface using TLD, location, counts, and accessibility score.
   - Responsive and minimal UI.

---

## 🛠️ Technologies Used

- **PostgreSQL** – Scalable relational database.
- **Python** – Processing scripts and backend.
- **Selenium, Requests, BeautifulSoup** – Crawling.
- **whois, nslookup** – Standard Ubuntu commands.
- **subfinder, amass, assetfinder** – Open-source subdomain finders.
- **Pa11y, Lighthouse CLI** – Accessibility checking.
- **React or Flask with HTML/CSS** – Frontend (configurable).

---

## 📁 Folder Structure


