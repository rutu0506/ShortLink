import sqlite3
import datetime
import uuid

from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()


def init_db():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            long_url TEXT NOT NULL UNIQUE,
            short_code TEXT NOT NULL UNIQUE,
            clicks INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            last_accessed_at DATETIME
        )
    ''')
    conn.commit()
    conn.close()


def generate_short_code(long_url):
    return str(uuid.uuid3(uuid.NAMESPACE_URL, long_url)).replace('-', '')


def insert_url(long_url):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    try:
        c.execute('SELECT short_code FROM urls WHERE long_url = ?', (long_url,))
        result = c.fetchone()
        if result:
            short_code = result[0]
        else:
            short_code = generate_short_code(long_url)
            c.execute('INSERT INTO urls (long_url, short_code, created_at) VALUES (?, ?, ?)',
                      (long_url, short_code, datetime.datetime.now()))
            conn.commit()
    except sqlite3.IntegrityError:
        return insert_url(long_url)
    finally:
        conn.close()
    return short_code


def get_long_url(short_code):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('SELECT long_url FROM urls WHERE short_code = ?', (short_code,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None


def cleanup():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    cutoff = datetime.datetime.now() - datetime.timedelta(days=30)
    c.execute('DELETE FROM urls WHERE last_accessed_at < ? OR (created_at < ? AND '
              'last_accessed_at IS NULL)',
              (cutoff, cutoff))
    conn.commit()
    conn.close()
    print(f"Deleted old URLs (<{cutoff})")


def init_scheduled_background_cleanup():
    scheduler.add_job(func=cleanup, trigger='interval', days=30)
    scheduler.start()


def stop_scheduled_background_cleanup():
    scheduler.shutdown()
