from etl.db import get_db_connection

def load_books(cleaned_books):
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1) Create scrape run entry
    cursor.execute("""
        INSERT INTO scrape_run (run_timestamp, source, status)
        VALUES (NOW(), 'amazon_engineering_books', 'in-progress')
    """)
    conn.commit()
    scrape_run_id = cursor.lastrowid

    for book in cleaned_books:
        # 2) Insert/Ignore into book table (idempotent)
        cursor.execute("""
            INSERT INTO book (title, author, category)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE id=id
        """, (book["title"], book["author"], book["category"]))
        conn.commit()

        # 3) Get book id
        cursor.execute("SELECT id FROM book WHERE title = %s", (book["title"],))
        book_id = cursor.fetchone()[0]

        # 4) Insert into snapshot table
        cursor.execute("""
            INSERT INTO book_snapshot (book_id, scrape_run_id, price, rating, rating_count, snapshot_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            book_id,
            scrape_run_id,
            book["price"],
            book["rating"],
            book["rating_count"],
            book["snapshot_timestamp"]
        ))
        conn.commit()

    # 5) Mark run as success
    cursor.execute("""
        UPDATE scrape_run SET status='success', total_books=%s
        WHERE id=%s
    """, (len(cleaned_books), scrape_run_id))
    conn.commit()

    cursor.close()
    conn.close()

    return scrape_run_id
