from etl.extract import extract_books

books = extract_books()
for b in books:
    print(b)
