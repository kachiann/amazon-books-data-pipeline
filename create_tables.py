from etl.db import get_db_connection

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("🔧 Creating tables...")

    # ---- SCRAPE RUN TABLE ----
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_run (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_timestamp DATETIME NOT NULL,
            source VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL,
            total_books INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # ---- BOOK TABLE (Static Metadata) ----
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS book (
            id INT AUTO_INCREMENT PRIMARY KEY,
            asin VARCHAR(20) UNIQUE,
            title VARCHAR(500) UNIQUE NOT NULL,
            author VARCHAR(255),
            category VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # ---- SNAPSHOT TABLE (Daily Changing Attributes) ----
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS book_snapshot (
            id INT AUTO_INCREMENT PRIMARY KEY,
            book_id INT NOT NULL,
            scrape_run_id INT NOT NULL,
            price DECIMAL(10,2),
            rating DECIMAL(3,2),
            rating_count INT,
            snapshot_timestamp DATETIME NOT NULL,
            FOREIGN KEY (book_id) REFERENCES book(id),
            FOREIGN KEY (scrape_run_id) REFERENCES scrape_run(id)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tables created successfully!")

if __name__ == "__main__":
    create_tables()
