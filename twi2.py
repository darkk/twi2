#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -- Leonid Evdokimov <leon@darkk.net.ru>

from urllib import urlencode, splitquery
import random
from urllib2 import urlopen, HTTPError
from urlparse import parse_qs
from Queue import Queue
from threading import Thread
import socket
import logging
import json
import sqlite3
from optparse import OptionParser
import re
import time
import BaseHTTPServer


def scheme_update(db):
    db.execute('PRAGMA foreign_keys = ON')

    version = None
    try:
        for row in db.execute('SELECT scheme_version FROM scheme_version'):
            version = row[0]
    except sqlite3.OperationalError:
        # probably, there is no such table
        db.execute('CREATE TABLE scheme_version(scheme_version INTEGER)')
    if version is None:
        version = 0
        db.execute('INSERT INTO scheme_version VALUES(?)', (version,) )

    schemes = (
        """
        CREATE TABLE timeline (
            status_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            user_screenname TEXT,
            created_at TEXT,
            lat REAL, lon REAL,
            status_text TEXT)
        """, """
        CREATE TABLE twitter_account (
            user_screenname TEXT)
        """, """
        CREATE TABLE destinations (
            dest_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dest_type TEXT,
            dest_login TEXT,
            dest_cred TEXT,
            UNIQUE (dest_type, dest_login))
        """, """
        CREATE TABLE queue (
            status_id INTEGER REFERENCES timeline (status_id),
            dest_id INTEGER REFERENCES destinations (dest_id),
            start_ts INTEGER,
            warn_ts INTEGER,
            done_ts INTEGER,
            PRIMARY KEY (status_id, dest_id))
        """, """
        CREATE TABLE rewritten (
            status_id INTEGER REFERENCES timeline (status_id),
            rewritten_text TEXT)
        """, """
        CREATE TABLE urls (
            url_id INTEGER PRIMARY KEY AUTOINCREMENT,
            status_id INTEGER REFERENCES timeline (status_id),
            short_url TEXT,
            expanded_url TEXT)
        """
    )
    for i in xrange(version, len(schemes)):
        query = schemes[i]
        logging.info('scheme_update: %s', query)
        db.execute(query)
        db.execute('UPDATE scheme_version SET scheme_version = scheme_version + 1')
        db.commit()

def now():
    return int(time.time())

def cmd_fetch_new(db):
    qs_ext = {}
    for row in db.execute("SELECT MAX(status_id) FROM timeline"):
        if row[0] is not None:
            qs_ext['since_id'] = row[0]
    return twi_fetchapi(db, qs_ext)

def cmd_fetch_old(db):
    qs_ext = {}
    for row in db.execute("SELECT MIN(status_id) FROM timeline"):
        if row[0] is not None:
            qs_ext['max_id'] = row[0]-1
    return twi_fetchapi(db, qs_ext)

def cmd_twi_login(db, user_screenname):
    db.execute("DELETE FROM twitter_account")
    db.execute("INSERT INTO twitter_account VALUES(?)", (user_screenname,))
    db.commit()

def twi_fetchapi(db, qs_ext):
    screen_name = db.execute("SELECT user_screenname FROM twitter_account").fetchone()[0]
    url = 'http://api.twitter.com/1/statuses/user_timeline.json'
    query_string = {
        'screen_name': screen_name,
        'include_rts': 'true',
        'count': 100,
        'exclude_replies': 1,
    }
    query_string.update(qs_ext)
    req = url + '?' + urlencode(query_string)
    logging.info('Calling twitter API: %s' % req)
    resp = urlopen(req)
    status_list = json.load(resp)
    store_status_list(db, status_list)

def store_status_list(db, status_list):
    for s in status_list:
        origin = s.get('retweeted_status', s)
        if s['geo'] is not None:
            lat, lon = s['geo']['coordinates']
        else:
            lat, lon = None, None
        logging.debug('Going to store status_id: %d created_at: %s', s['id'], s['created_at'])
        db.execute(
            "INSERT INTO timeline VALUES(?, ?, ?, ?, ?, ?, ?)",
            (s['id'], origin['user']['id'], origin['user']['screen_name'],
             s['created_at'], lat, lon, origin['text']))
        db.commit()

def expand_url(url):
    logging.info('Expanding url <%s>', url)
    try:
        fd = urlopen(url, timeout=15)
        fd.close()
        return fd.geturl() # redirections are processed by urllib2
    except HTTPError, err:
        # wikipedia returns 403 Forbidden, when it sees urllib
        logging.warning("Can't properly decode URL \"%s\" - returning last one" % url, exc_info=1)
        newurl = err.geturl()
        if newurl:
            return newurl
    except Exception:
        logging.warning("Can't decode URL \"%s\"" % url, exc_info=1)
    return None

def rewrite(status):
    assert isinstance(status, unicode)
    rewrite = []
    logging.debug('Exploding status: %s', repr(status))
    for match in re.finditer(r'https?://[^\s]+', status):
        begin, end = match.span()
        while True:
            url = status[begin:end]
            newurl = expand_url(url)
            logging.debug('Exploded URL %s -> %s', repr(url), repr(newurl))
            if newurl is not None and newurl != url:
                rewrite.append( (begin, end, url, newurl) )
                break
            elif status[end-1] in u'…,.!?()[]{}/"\':“”«»':
                end -= 1 # try to decode in case of bad punctuation
            else:
                break
    rewrite.sort(reverse=True)
    for begin, end, url, newurl in rewrite:
        if status[begin:end] != url:
            raise RuntimeError, 'status[%d:%d] == %s differes from url=%s. status=%s, newurl=%s' % (
                    begin, end, repr(status[begin:end]), repr(url), repr(status), repr(newurl))
        status = status[:begin] + newurl + status[end:]
    return status, [(x[2], x[3]) for x in rewrite]

def cmd_rewrite(db):
    q = """
        SELECT status_id, status_text FROM timeline
        WHERE status_id IN (
            SELECT status_id FROM timeline
            EXCEPT
            SELECT status_id FROM rewritten
        )
        """ # FIXME: what about `EXISTS` ?
    for status_id, status_text in db.execute(q):
        rewritten_text, urls = rewrite(status_text) # FIXME: parallel map?
        for short_url, expanded_url in urls:
            db.execute("INSERT INTO urls (status_id, short_url, expanded_url) VALUES(?, ?, ?)",
                    (status_id, short_url, expanded_url))
        db.execute("INSERT INTO rewritten VALUES(?, ?)", (status_id, rewritten_text))
        db.commit()

def cmd_enqueue(db):
    q = """
        INSERT INTO queue (status_id, dest_id, start_ts, warn_ts, done_ts)
        SELECT status_id, dest_id, ?, ?, NULL FROM (
            SELECT status_id, dest_id FROM rewritten, destinations
            EXCEPT
            SELECT status_id, dest_id FROM queue
        )
        """
    ts = now()
    rows = db.execute(q, (ts, ts)).rowcount
    db.commit()
    logging.info('Injected %d tasks into queue', rows)

# Doc:
# http://vk.com/developers.php?oid=-17680044&p=OAuth_Authorization_Dialog
# http://vk.com/developers.php?o=-17680044&p=Application%20Access%20Rights
# http://vk.com/developers.php?o=-17680044&p=Authorizing%20Sites
def cmd_vk_login(db, client_id, client_secret):
    token_q = Queue()
    myhost = socket.getfqdn() # from DNS
    myport = 1925
    path = '/twi2/auth-gate'
    class VkCodeHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self):
            logging.info('GET callback from VK: %s', self.path)
            qpath, query_string = splitquery(self.path)
            if query_string is None:
                query_string = ''
            q = parse_qs(query_string)
            if qpath != path or 'code' not in q:
                self.send_response(500)
                self.end_headers()
            token_q.put(q['code'][0])
            self.send_response(200)
            reply = 'Ok, go to [s]hell.'
            self.send_header('content-length', len(reply))
            self.send_header('content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(reply)
        def log_message(self, format, *args):
            logging.info("%s %s" % (self.address_string(), format%args))

    logging.info('Staring callback HTTP server at %s:%d', myhost, myport)
    srv = BaseHTTPServer.HTTPServer((myhost, myport), VkCodeHandler)
    thr = Thread(target=srv.serve_forever, name='webserver')
    thr.start()

    query_string = urlencode({
        'client_id': client_id,
        'scope': 'wall,offline',
        'redirect_uri': 'http://%s:%u%s' % (myhost, myport, path),
        'response_type': 'code'})
    logging.info('Go to: http://oauth.vk.com/authorize?' + query_string)

    code = token_q.get()
    srv.shutdown()
    thr.join()
    logging.info('Got code for token: %s', code)

    query_string = urlencode({
        'client_id': client_id,
        'code': code,
        'client_secret': client_secret})
    resp = urlopen('https://api.vk.com/oauth/access_token?' + query_string)
    token = json.load(resp)
    if 'error' in token or token['expires_in'] != 0:
        raise RuntimeError, 'Got bad token: %s' % repr(token)
    logging.info('Got token: %s', token)
    db.execute(
        "INSERT INTO destinations (dest_type, dest_login, dest_cred) VALUES('vk.com', ?, ?)",
        (token['user_id'], token['access_token']))
    db.commit()

def vk_call(db, method, **kvargs):
    access_token = db.execute("SELECT dest_cred FROM destinations WHERE dest_type = 'vk.com'").fetchone()[0]
    query = {'access_token': access_token}
    query.update(kvargs)
    query = urlencode(query)
    url = ''.join(['https://api.vk.com/method/', method, '?', query])
    logging.info('Fetching API url: %s', url)
    resp = urlopen(url)
    d = json.load(resp)
    if 'error' in d:
        raise RuntimeError, str(d)
    return d

def cmd_vk_dequeue(db):
    wall_count = 20
    for e in vk_call(db, method='wall.get', count=wall_count)['response']:
        if isinstance(e, dict) and 'text' in e:
            rows = db.execute("""
                UPDATE queue SET done_ts = ?
                WHERE done_ts IS NULL
                AND status_id <= (
                    SELECT status_id FROM rewritten WHERE rewritten_text = ?)
                AND dest_id = (
                    SELECT dest_id FROM destinations WHERE dest_type = 'vk.com')
                """, (now(), e['text'])).rowcount
            db.commit()
            if rows != 0:
                logging.info('Dequeued %d tasks', rows)

def cmd_push(db):
    tasks = db.execute("""
            SELECT dest_type, dest_id, t.status_id, rewritten_text
            FROM (SELECT dest_id, MIN(status_id) AS status_id FROM queue WHERE done_ts IS NULL GROUP BY dest_id) AS t
            JOIN destinations USING (dest_id)
            JOIN rewritten ON (t.status_id = rewritten.status_id)
            """).fetchall()
    random.shuffle(tasks)
    pusher = {
        'vk.com': vk_pusher
    }
    for dest_type, dest_id, status_id, rewritten_text in tasks:
        # TODO: retweets! lat-lon! smarter URLs!
        pusher[dest_type](db, rewritten_text.encode('utf-8'))
        db.execute('UPDATE queue SET done_ts = ? WHERE status_id = ? AND dest_id = ?',
                (now(), status_id, dest_id))
        db.commit()

def vk_pusher(db, message):
    response = vk_call(db, 'wall.post', message=message)
    logging.info('wall.post ok: post_id=%d', response['response']['post_id'])

def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = OptionParser()
    parser.usage = 'Usage: %prog [options] <action> <action> ...'
    parser.add_option('-d', '--database', help='Use DATABASE as sqlite db to store queue.')
    parser.add_option('-t', '--twi-name', help='Use TWI_NAME to pull data from twitter.')
    parser.add_option('-k', '--vk-client', help='Use VK_CLIENT as application `client_id` for vk.com API.')
    parser.add_option('-K', '--vk-secret', help='Use VK_SECRET as application `client_secret` for vk.com API.')
    parser.add_option('-j', '--juick-login', help='Use JUICK_LOGIN for juick.com API.')
    parser.add_option('-J', '--juick-password', help='Use JUICK_PASSWORD for juick.com API.')
    known_args = {
        'twi_login': (cmd_twi_login, ('twi_name',)),
        'fetch_new': cmd_fetch_new,
        'fetch_old': cmd_fetch_old,
        'vk_login': (cmd_vk_login, ('vk_client', 'vk_secret')),
        'vk_dequeue': cmd_vk_dequeue,
        'rewrite': cmd_rewrite,
        'enqueue': cmd_enqueue,
        'push': cmd_push,
        #'juick_login': (juick_login, ('juick_login', 'juick_password')),
    }
    parser.epilog = 'Actions: ' + ', '.join(sorted(known_args.keys()))
    opt, args = parser.parse_args()
    for action in args:
        if action not in known_args:
            parser.error('Unknown action: <%s>' % action)

    db = sqlite3.connect(opt.database)
    scheme_update(db)
    for action in args:
        if callable(known_args[action]):
            func, args = known_args[action], []
        else:
            func, args = known_args[action]
        args = [getattr(opt, key) for key in args]
        if not all(args):
            raise RuntimeError, 'Some args are missing for action <%s>' % action
        func(db, *args)
    return 0

if __name__ == '__main__':
    main()