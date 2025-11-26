from etl.extract import extract_books
from etl.transform import clean_books
from etl.load import load_books

raw = extract_books()
cleaned = clean_books(raw)

run_id = load_books(cleaned)
print("Loaded into DB with scrape run id:", run_id)
