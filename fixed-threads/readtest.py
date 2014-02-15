# coding=utf-8
__author__ = "Leo Bernard"

"""
Der Queue-Manager des Projektes, zudem das Main-Script

Im Rahmen der Facharbeit "Datenbanken - Big Data" von Leo Bernard

"""

import sys  # Systembezogene Funktionen laden
import Queue  # Datenstruktur Warteschlange laden
import crawler  # Crawler-Funktionen laden (siehe crawler.py)

from optparse import OptionParser  # Bibliothek zum Verarbeiten von Kommandozeilenargumenten laden
from pymongo import MongoClient  # Bibliothek zur Verbindung von Python zur MongoDB laden

# =================== Verfügbare Kommandozeilenparameter festlegen ==================== #
parser = OptionParser(description="Dieses Programm crawlt Websites indem es von einer gegebenen "
                                  "Adresse aus alle Links bis zu einer gegebenen Tiefe aufruft und sie durchsucht. Die "
                                  "Ergebnisse werden in MongoDB gespeichert. Werden keine Parameter angegeben, wird "
                                  "das Programm die in MongoDB gespeicherte Warteschlange weiter abarbeiten.", usage="")
parser.add_option("-w", "--website", dest="website", default=False,
                  help="Leert die Datenbank und startet das Crawling von der gegebenen Adresse aus.", metavar="WEBSITE")
parser.add_option("-d", "--depth", dest="depth", default=3,
                  help="Die maximale Tiefe der Links, die gecrawlt werden sollen. (Def", metavar="DEPTH")
parser.add_option("-m", "--mode", dest="mode", default="b",
                  help="Gibt an, ob der Crawler die anstehenden Seiten per "
                       "Tiefensuche (d) oder per Breitensuche (b) arbeiten soll.", metavar="MODE")
parser.add_option("-t", "--threads", dest="threads", default=20,
                  help="Gibt die anzahl der Threads an, die gleichzeitig die Warteschlange abarbeiten.", metavar="MODE")
parser.add_option("-v", "--verbosity", dest="verbosity", default="2",
                  help="Gibt die genauigkeit der Ausgabe an: 0 - Nur Fehler; 1 - 0 & Aufrufe; "
                       "2 - 1 & Antworten; 3 - 2 & Antwort-Details; 4 - Alles", metavar="VERBOSITY")

(options, args) = parser.parse_args()  # Gegebene Parameter laden.

# ============================== Initialisierung starten ============================== #
client = MongoClient(max_pool_size=500)  # Verbindung zu unserer Datenbank (MongoDB) herstellen
queue = client['crawler-database']['queue']  # Warteschlange aus der Datenbank auswählen
crawled = client['crawler-database']['crawled']  # Liste der gecrawlten Websites aus der Datenbank auswählen

if options.website is not False:  # Wenn eine Start-Website als Parameter angegeben wurde.
    queue.drop()
    crawled.drop()
    queue.insert({'url': options.website, 'parent': 0, 'depth': 0})
    print "Der Crawler wird die bestehenden Daten löschen und mithilfe von", options.threads, \
          "Workern alle Websites bis zur Tiefe", options.depth, "ausgehend von der Adresse " + \
          options.website + " per " + ("Breitensuche" if options.mode == "b" else "Tiefensuche") + " durchsuchen."
    print "====================================================================="
else:
    print "Der Crawler wird mithilfe von", str(options.threads), "Workern alle Websites bis zur Tiefe", \
          options.depth, "per" + ("Breitensuche" if options.mode == "b" else "Tiefensuche") + \
          " durchsuchen, die sich in der Warteschlange der Datenbank befinden."
    print "====================================================================="

counter = 1  # Zählvariable einführen, welche die Anzahl der bereits abgefragten Websites speichert
job_queue = Queue.Queue(0)  # Lokale Warteschlange initialisieren, welche später die Jobs für alle Worker enthalten wird
workers = []  # Liste der Worker initialisieren

for i in range(1, int(options.threads) + 1):  # Die gegebene Anzahl von Workern initialisieren
    thread = crawler.Worker(i, client, job_queue, options)  # Worker erstellen
    thread.start()  # Worker starten
    workers.append(thread)  # Worker der Liste der Worker hinzufügen

try:  # Fehler abfangen
    while True:  # Endlosschleife starten, welche die Queue-Items der Datenbank abfragt und in die lokale Queue einfügt
        document = queue.find_and_modify(remove=True)  # Erstes Item der Queue aus der Datenbank abrufen und entfernen

        if document is not None:
            if int(document[u'depth']) <= int(options.depth):
                job_queue.put(document)
                counter += 1


except KeyboardInterrupt:  # Wenn das Programm beendet wird, alle Threads beenden
    print "========================================================================"
    print "Worker werden beendet..."

    for worker in workers:  # Alle laufenden Worker als zu beenden markieren
        worker.kill_worker()

    for worker in workers:  # Warten, bis alle Worker beendet wurden
        worker.join()

    print "========================================================================"
    print "Crawling wurde beendet. Insgesamt wurden", counter, "Websites abgefragt."
    sys.exit(0)