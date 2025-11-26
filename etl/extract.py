import os
from bs4 import BeautifulSoup

FORMAT_KEYWORDS = {"Paperback", "Hardcover", "Kindle Edition", "Perfect paperback"}

def extract_books(sample_html_path="etl/data/amazon_de_Data_Engineering_books.html"):
    if not os.path.exists(sample_html_path):
        raise FileNotFoundError(f"Sample file not found: {sample_html_path}")

    with open(sample_html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    books = []
    product_list = soup.find_all("div", {"data-component-type": "s-search-result"})

    for product in product_list:

        # ---- Title ----
        title_tag = product.select_one('h2 span')
        title = title_tag.get_text(strip=True) if title_tag else None
        if title:
            title = title.replace("SponsoredSponsored", "").strip()

        # ---- Author (excluding format types) ----
        author = None

        author_candidates = product.select('span.a-size-base-plus, a.a-size-base-plus')

        for tag in author_candidates:
            text = tag.get_text(strip=True)
            if text not in FORMAT_KEYWORDS and len(text) > 2:
                author = text
                break

        # ---- Price ----
        price_tag = product.select_one("span.a-price span.a-offscreen")
        price = (
            price_tag.get_text(strip=True).replace("€", "").replace(",", ".")
            if price_tag else None
        )

        # ---- Rating ----
        rating_tag = product.select_one("span.a-icon-alt")
        rating = (
            rating_tag.get_text(strip=True).split()[0] if rating_tag else None
        )

        # ---- Rating Count ----
        rating_count_tag = product.select_one("span.a-size-base.s-underline-text.s-link-style")
        rating_count = (
            rating_count_tag.get_text(strip=True).replace(".", "") if rating_count_tag else None
        )

        books.append({
            "title": title,
            "author": author,
            "price": price,
            "rating": rating,
            "rating_count": rating_count
        })

    return books
