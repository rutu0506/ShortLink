import datetime
from flask import Flask, request, redirect, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import sqlite3
from utils import insert_url, get_long_url, init_db, init_scheduled_background_cleanup, \
    stop_scheduled_background_cleanup

app = Flask(__name__)

limiter = Limiter(app=app, key_func=get_remote_address, default_limits=['200 per day', '50 per hour'])


@app.route('/', methods=['GET', 'POST'])
@limiter.limit('5 per minute')
def home():
    if request.method == 'POST':
        long_url = request.form['long_url']
        short_code = insert_url(long_url)
        if short_code:
            short_url = request.host_url + short_code
            return render_template('index.html', short_url=short_url)
        else:
            return render_template('index.html', error='Something went Wrong!')
    return render_template('index.html')


@app.route('/<short_code>')
def redirect_to_long_url(short_code):
    long_url = get_long_url(short_code)
    if long_url:
        conn = sqlite3.connect('database.db')
        c = conn.cursor()
        c.execute('UPDATE urls SET last_accessed_at = ?, clicks = clicks + 1 WHERE short_code = ?',
                  (datetime.datetime.now(), short_code))
        conn.commit()
        conn.close()
        return redirect(long_url)
    return "URL not found", 404


@app.errorhandler(429)
def ratelimit_handler():
    return render_template("rate_limit_exceeded.html"), 429


if __name__ == '__main__':
    init_db()
    init_scheduled_background_cleanup()
    app.run()
    stop_scheduled_background_cleanup()
