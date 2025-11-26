from etl.extract import extract_books
from etl.transform import clean_books

raw = extract_books()
cleaned = clean_books(raw)

for book in cleaned[:5]:  # print first five
    print(book)
