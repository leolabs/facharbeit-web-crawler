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

from bs4 import BeautifulSoup, Comment  # Bibliothek zum Auslesen von Elementen aus dem HTML Code laden


class Crawler(threading.Thread):
    def __init__(self, client, url, parent, depth, options):
        threading.Thread.__init__(self)
        self.client = client
        self.url = url
        self.parent = parent
        self.depth = depth
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

        try:
            req = urllib2.Request(self.url, headers={'User-Agent': "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 "
                                            "(KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36"})

            con = urllib2.urlopen(req)
            source = con.read()
            soup = BeautifulSoup(source)
        except:
            print "\t[" + str(self.depth) + "] [!!!] " + self.url + " ist fehlerhaft..."
            pass
        else:
            print "\t[" + str(self.depth) + "] [" + str(con.getcode()) + "] " + self.url + " hat geantwortet..."

            # Scraping
            soup_links = soup.findAll('a')
            soup_title = soup.find('title')
            soup_author = soup.find('meta', {'name': 'author'})
            soup_description = soup.find('meta', {'name': 'description'})
            soup_keywords = soup.find('meta', {'name': 'keywords'})
            soup_texts = soup.findAll(text=True)

            # Einzufügendes Objekt erstellen
            urlobject = {
                "url": self.url,
                "title": soup_title.string if soup_title is not None else "",
                "author": soup_author['content'] if soup_author is not None else "",
                "description": soup_description['content'] if soup_description is not None else "",
                "keywords": soup_keywords['content'] if soup_keywords is not None else "",
                "linkcount": len(soup_links),
                "textblocks": filter(self.filter_readabletext, soup_texts),
                "depth": self.depth,
                "parent": self.parent,
                "children": []
            }

            single_doc = crawled.find_one({'url': urlobject["url"]})  # Prüfen, ob die URL bereits gecrawlt wurde

            if single_doc is not None:  # Wenn die URL schon gecrawlt wurde...
                doc_id = str(single_doc[u'_id'])  # ...parent auf den bereits existierenden Eintrag setzen
            else:
                doc_id = crawled.insert(urlobject)  # URL in die Datenbank einfügen

            # Übergeordnete URL suchen und gecrawlte URL als Kind einfügen
            if self.parent is not 0:
                crawled.find_and_modify({'_id': self.parent}, {'$push': {'children': doc_id}})

            if int(self.depth) < int(self.options.depth):  # Nur wenn die maximale Tiefe nicht überschritten ist
                linklist = []
                for item in soup.findAll('a'):
                    try:
                        link = urlparse.urljoin(self.url, item['href'])
                        if not link in linklist and not link.startswith("#") \
                                and not link.startswith("mailto") and not link.startswith("javascript")\
                                and not re.match(".*\.(jpg|jpeg|png|gif|pdf)", link):
                            linklist.append(link)
                            queue.insert({
                                'url': link.split("#", 1)[0],
                                'depth': self.depth + 1,
                                'parent': doc_id
                            })
                    except:
                        pass