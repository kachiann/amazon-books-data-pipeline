CREATE TABLE scrape_run (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_timestamp DATETIME NOT NULL,
    source VARCHAR(100) NOT NULL,      -- e.g. 'amazon_engineering_best_sellers'
    status VARCHAR(20) NOT NULL,       -- 'success' or 'failed'
    total_books INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book (
    id INT AUTO_INCREMENT PRIMARY KEY,
    asin VARCHAR(20) UNIQUE,           -- Amazon ID if you capture it
    title VARCHAR(500) NOT NULL,
    author VARCHAR(255),
    category VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_snapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    book_id INT NOT NULL,
    scrape_run_id INT NOT NULL,
    price DECIMAL(10,2),
    currency VARCHAR(10),
    rating DECIMAL(3,2),               -- e.g. 4.7
    rating_count INT,
    snapshot_timestamp DATETIME NOT NULL,
    FOREIGN KEY (book_id) REFERENCES book(id),
    FOREIGN KEY (scrape_run_id) REFERENCES scrape_run(id)
);
