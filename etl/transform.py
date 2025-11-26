from datetime import datetime

def clean_books(raw_books):
    """
    Clean data types and add metadata before loading into MySQL.
    """

    cleaned = []

    for b in raw_books:
        # Price → float
        try:
            price = float(b.get("price")) if b.get("price") not in (None, "", "0.00") else None
        except:
            price = None
        
        # Rating → float
        try:
            rating = float(b.get("rating")) if b.get("rating") not in (None, "") else None
        except:
            rating = None
        
        # Rating Count → int
        try:
            rating_count = int(b.get("rating_count")) if b.get("rating_count") not in (None, "") else None
        except:
            rating_count = None

        cleaned.append({
            "title": b.get("title"),
            "author": b.get("author"),
            "price": price,
            "rating": rating,
            "rating_count": rating_count,
            "category": "Engineering",
            "snapshot_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # UTC timestamp
        })

    return cleaned
