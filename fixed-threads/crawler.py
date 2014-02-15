# coding=utf-8
__author__ = 'leobernard'

"""
Die Crawler-Funktionen des Projektes.

Im Rahmen der Facharbeit "Datenbanken - Big Data" von Leo Bernard

"""

import urlparse  # Bibliothek zum Parsen von Web-Adressen laden
import urllib2  # Bibliothek zum Abrufen von Websites laden
import threading  # Bibliothek für Threading-Funktionen laden
import re  # Bibliothek für reguläre Ausdrücke (Regular Expressions) laden
import time  # Bibltiothek zur Zeitmessung laden

from bs4 import BeautifulSoup, Comment  # Bibliothek zum Auslesen von Elementen aus dem HTML Code laden


class Crawler(threading.Thread):
    def __init__(self, worker_id, client, queue, options):
        threading.Thread.__init__(self)
        self.worker_id = worker_id
        self.client = client
        self.queue = queue
        self.options = options

    def filter_readabletext(self, element):
        """Prüft, ob das gegebene HTML-Element ein gültiger Text ist."""
        if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
            return False
        elif isinstance(element, Comment):
            return False
        elif unicode(element) in [u"\n", u' ']:
            return False
        elif len(element) < 3:
            return False
        elif not re.match(ur'([A-Za-z])+', unicode(element)):
            return False
        elif not u' ' in unicode(element).strip():
            return False
        else:
            return True

    def run(self):
        """
        Lädt eine gegebene Seite, extrahiert die benötigten Metadaten und fügt diese in die Datenbank ein.
        Dazu werden alle auf der geladenen Seite vorhandenen Links in die Warteschlange eingefügt.
        """
        queue = self.client['crawler-database']['queue']
        crawled = self.client['crawler-database']['crawled']

        if int(self.options.verbosity) >= 1:
            print "\t[%03d] Worker wurde gestartet, wartet auf Aufträge." % self.worker_id

        while True:
            if not self.queue.empty():
                current_job = self.queue.get()
                try:
                    if int(self.options.verbosity) >= 1:
                        print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [GET] " + \
                              current_job[u'url'] + " wird aufgerufen..."

                    time_start = int(round(time.time() * 1000))

                    req = urllib2.Request(
                        current_job[u'url'],
                        headers={'User-Agent': "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 "
                                               "(KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36"}
                    )

                    con = urllib2.urlopen(req)
                    source = con.read()
                    time_response = int(round(time.time() * 1000))
                    soup = BeautifulSoup(source)
                except Exception, e:
                    print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [!!!] " + \
                          current_job[u'url'] + " ist fehlerhaft:", e
                    pass
                else:
                    if int(self.options.verbosity) >= 2:
                        print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [" + \
                              str(con.getcode()) + "] " + current_job[u'url'] + " hat geantwortet..."

                    # Scraping
                    soup_links = soup.findAll('a')
                    soup_title = soup.find('title')
                    soup_author = soup.find('meta', {'name': 'author'})
                    soup_description = soup.find('meta', {'name': 'description'})
                    soup_keywords = soup.find('meta', {'name': 'keywords'})
                    soup_texts = soup.findAll(text=True)

                    # Einzufügendes Objekt erstellen
                    urlobject = {
                        "url": current_job[u'url'],
                        "title": soup_title.string if soup_title is not None else "",
                        "author": soup_author['content'] if soup_author is not None else "",
                        "description": soup_description['content'] if soup_description is not None else "",
                        "keywords": soup_keywords['content'] if soup_keywords is not None else "",
                        "linkcount": len(soup_links),
                        "textblocks": filter(self.filter_readabletext, soup_texts),
                        "depth": current_job[u'depth'],
                        "parent": current_job[u'parent'],
                        "latency": time_response - time_start,
                        "children": []
                    }

                    single_doc = crawled.find_one({'url': urlobject["url"]})  # Prüfen, ob die URL schon gecrawlt wurde

                    if single_doc is not None:  # Wenn die URL schon gecrawlt wurde...
                        doc_id = single_doc[u'_id']  # ...Parent auf den bereits existierenden Eintrag setzen
                    else:
                        doc_id = crawled.insert(urlobject)  # URL in die Datenbank einfügen, Parent auf URL setzen

                    # Übergeordnete URL suchen und gecrawlte URL als Kind einfügen
                    if current_job[u'parent'] is not 0:
                        crawled.find_and_modify({'_id': current_job[u'parent']}, {'$push': {'children': doc_id}})

                    if int(current_job[u'depth']) < int(self.options.depth):  # Maximale Tiefe nicht überschreiten
                        linklist = []
                        for item in soup.findAll('a'):
                            try:
                                link = urlparse.urljoin(current_job[u'url'], item['href'])
                                if not link in linklist and not link.startswith("#") \
                                        and not link.startswith("mailto") and not link.startswith("javascript")\
                                        and not re.match(".*\.(jpg|jpeg|png|gif|pdf)", link):
                                    linklist.append(link)
                                    queue.insert({
                                        'url': link.split("#", 1)[0],
                                        'depth': current_job[u'depth'] + 1,
                                        'parent': doc_id
                                    })
                            except:
                                pass