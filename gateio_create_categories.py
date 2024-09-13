# File: gateio_create_categories.py

# Define the URLs and categories
gateio_urls = {
    "https://www.gate.io/announcements/activity": "Activities",
    "https://www.gate.io/announcements/dau": "Bi-Weekly Report",
    "https://www.gate.io/announcements/institutional": "Institutional & VIP",
    "https://www.gate.io/announcements/gate-learn": "Gate Learn",
    "https://www.gate.io/announcements/delisted": "Delisting",
    "https://www.gate.io/announcements/wealth": "Gate Wealth",
    "https://www.gate.io/announcements/newlisted": "New Cryptocurrency Listings",
    "https://www.gate.io/announcements/charity": "Gate Charity",
    "https://www.gate.io/announcements/finance": "Finance",
    "https://www.gate.io/announcements/trade-match": "Trading Competitions",
    "https://www.gate.io/announcements/deposit-withdrawal": "Deposit & Withdrawal",
    "https://www.gate.io/announcements/etf": "ETF",
    "https://www.gate.io/announcements/fee": "Fee",
    "https://www.gate.io/announcements/lives": "Live",
    "https://www.gate.io/announcements/gatecard": "Gate Card",
    "https://www.gate.io/announcements/rename": "Token Rename",
    "https://www.gate.io/announcements/engine-upgrade": "Engine Upgrade",
    "https://www.gate.io/announcements/fiat": "Fiat",
    "https://www.gate.io/announcements/precision": "Precision",
    "https://www.gate.io/announcements/p2p": "P2P Trading"
}

# Create the .txt file
with open('gateio_urls.txt', 'w') as file:
    for url, category in gateio_urls.items():
        file.write(f"{url}\t{category}\n")  # Write URL and category separated by a tab

print("gateio_urls.txt file has been created.")
