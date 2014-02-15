# coding=utf-8
__author__ = 'Leo Bernard'

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


class Worker(threading.Thread):
    def __init__(self, worker_id, client, queue, options):
        threading.Thread.__init__(self)
        self.worker_id = worker_id
        self.client = client
        self.queue = queue
        self.options = options
        self.is_killed = False

    def kill_worker(self):
        """Sagt dem Worker, dass er sich nach dem aktuellen Auftrag beenden soll."""
        print "\t[%03d] Worker wird so bald wie möglich gestoppt." % self.worker_id
        self.is_killed = True

    def filter_readabletext(self, element):
        """Prüft, ob das gegebene HTML-Element ein gültiger Text ist."""
        if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
            return False  # Tags, die nicht mit dem Dokument zu tun haben, ausschließen
        elif isinstance(element, Comment):
            return False  # HTML-Kommentare ausschließen
        elif unicode(element) in [u"\n", u' ']:
            return False  # Leere Tags ausschließen
        elif len(element) < 3:
            return False  # Tags mit weniger als drei Zeichen ausschließen
        elif not re.match(ur'([A-Za-z])+', unicode(element)):
            return False  # Tags, die keine Buchstaben enthalten, ausschließen
        elif not u' ' in unicode(element).strip():
            return False  # Tags, die keine Leerzeichen enthalten, ausschließen
        else:
            return True  # Ansonsten kann der Block mit in die Liste der Links aufgenommen werden

    def run(self):
        """
        Lädt eine gegebene Seite, extrahiert die benötigten Metadaten und fügt diese in die Datenbank ein.
        Dazu werden alle auf der geladenen Seite vorhandenen Links in die Warteschlange eingefügt.
        """
        queue = self.client['crawler-database']['queue']  # Queue aus der Datenbank auswählen
        crawled = self.client['crawler-database']['crawled']  # Liste der gecrawlten Websites aus Datenbank auswählen

        if int(self.options.verbosity) >= 1:  # Benutzer bescheidgeben, dass der Worker nun läuft
            print "\t[%03d] Worker wurde gestartet, wartet auf Aufträge." % self.worker_id

        while True:  # Unendlichschleife starten, die die Warteschlange auf Einträge überprüft und diese ggf. abarbeitet
            if self.is_killed:  # Wenn der Worker als zu beenden markiert ist, Schleife verlassen
                print "\t[%03d] Worker wurde gestoppt." % self.worker_id
                break

            if not self.queue.empty():  # Wenn es Aufträge in der Warteschlange gibt, diese verarbeiten
                current_job = self.queue.get()  # Auftrag aus der Warteschlange entnehmen
                try:
                    if int(self.options.verbosity) >= 1:
                        print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [GET] " + \
                              current_job[u'url'] + " wird aufgerufen..."

                    time_start = int(round(time.time() * 1000))  # Zeit vor dem Aufruf speichern

                    # Website-Aufruf initialisieren
                    req = urllib2.Request(
                        current_job[u'url'],
                        headers={'User-Agent': "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 "
                                               "(KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36"}
                    )

                    con = urllib2.urlopen(req)  # Website aufrufen
                    source = con.read()  # Inhalt der Website auslesen
                    time_response = int(round(time.time() * 1000))  # Zeit nach dem Aufruf speichern
                    soup = BeautifulSoup(source)  # Empfangenen HTML-Code parsen
                except Exception, e:  # Wenn ein Fahler aufgetreten ist, diesen ausgeben
                    print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [!!!] " + \
                          current_job[u'url'] + " ist fehlerhaft:", e
                    pass
                else:  # Wenn bis jetzt alle Abfragen fehlerfrei ausgeführt wurden
                    if int(self.options.verbosity) >= 2:
                        print "\t[%03d] [" % self.worker_id + str(current_job[u'depth']) + "] [" + \
                              str(con.getcode()) + "] " + current_job[u'url'] + " hat geantwortet..."

                    # Scraping
                    soup_links = soup.findAll('a')  # Alle Links finden
                    soup_title = soup.find('title')  # Titel der Seite finden
                    soup_author = soup.find('meta', {'name': 'author'})  # Author der Seite finden
                    soup_description = soup.find('meta', {'name': 'description'})  # Beschreibung der Seite finden
                    soup_keywords = soup.find('meta', {'name': 'keywords'})  # Keywords der Seite finden
                    soup_texts = soup.findAll(text=True)  # Alle Text-Blöcke finden

                    # Einzufügendes Objekt erstellen
                    urlobject = {
                        "url": current_job[u'url'],
                        "title": soup_title.string if soup_title is not None else "",
                        "author": soup_author['content'] if soup_author is not None else "",
                        "description": soup_description['content'] if soup_description is not None else "",
                        "keywords": soup_keywords['content'] if soup_keywords is not None else "",
                        "linkcount": len(soup_links),
                        "textblocks": "\n;\n".join(filter(self.filter_readabletext, soup_texts)),
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
                        linklist = []  # Liste der bereits gefundenen Links anlegen
                        for item in soup.findAll('a'):  # Alle Links finden
                            try:
                                # Relative URLs zu absoluten URLs parsen
                                link = urlparse.urljoin(current_job[u'url'], item['href'])

                                # Wenn der Link nicht bereits in der Liste ist und auf eine andere Website zeigt
                                if not link in linklist and not link.startswith("#") \
                                        and not link.startswith("mailto") and not link.startswith("javascript")\
                                        and not re.match(".*\.(jpg|jpeg|png|gif|pdf)", link):
                                    linklist.append(link)  # Link zu der Liste der bereits gefundenen Links hinzufügen

                                    # Link in die Queue der Datenbank einfügen
                                    queue.insert({
                                        'url': link.split("#", 1)[0],
                                        'depth': current_job[u'depth'] + 1,
                                        'parent': doc_id
                                    })
                            except:
                                pass