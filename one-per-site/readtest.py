# coding=utf-8
import crawler  # Crawler-Funktionen laden (siehe crawler.py)
import sys  # Systembezogene Funktionen laden
from pymongo import MongoClient  # Bibliothek zur Verbindung von Python zur MongoDB laden
from optparse import OptionParser  # Bibliothek zum Verarbeiten von Kommandozeilenargumenten laden

# =================== Verfügbare Kommandozeilenparameter festlegen ==================== #
parser = OptionParser(description="Dieses Programm crawlt Websites indem es von einer gegebenen "
                                  "Adresse aus alle Links bis zu einer gegebenen Tiefe aufruft und sie durchsucht. Die "
                                  "Ergebnisse werden in MongoDB gespeichert. Werden keine Parameter angegeben, wird "
                                  "Programm die in MongoDB gespeicherte Warteschlange weiter abarbeiten.", usage="")
parser.add_option("-w", "--website", dest="website", default=False,
                  help="Leert die Datenbank und startet das Crawling von der gegebenen Adresse aus.", metavar="WEBSITE")
parser.add_option("-d", "--depth", dest="depth", default=3,
                  help="Die maximale Tiefe der Links, die gecrawlt werden sollen. (Def", metavar="DEPTH")
parser.add_option("-m", "--mode", dest="mode", default="b",
                  help="Gibt an, ob der Crawler die anstehenden Seiten per "
                       "Tiefensuche (d) oder per Breitensuche (b) arbeiten soll.", metavar="MODE")
parser.add_option("-v", "--verbosity", dest="verbosity", default="2",
                  help="Gibt die genauigkeit der Ausgabe an: 0 - Nur Fehler; 1 - 0 & Aufrufe; "
                       "2 - 1 & Antworten; 3 - 2 & Antwort-Details; 4 - Alles", metavar="VERBOSITY")

(options, args) = parser.parse_args()  # Gegebene Parameter laden.

# ============================== Initialisierung starten ============================== #
client = MongoClient(max_pool_size=500)
queue = client['crawler-database']['queue']
crawled = client['crawler-database']['crawled']

if options.website is not False:
    queue.drop()
    crawled.drop()
    queue.insert({'url': options.website, 'parent': 0, 'depth': 0})
    print "Der Crawler wird nun alle Websites bis zur Tiefe", options.depth, "ausgehend von der Adresse " + \
          options.website + " per " + ("Breitensuche" if options.mode == "b" else "Tiefensuche") + " durchsuchen."
    print "====================================================================="

counter = 1

try:
    while True:
        document = queue.find_one()

        if document is not None:
            if int(document[u'depth']) <= int(options.depth):
                if options.verbosity >= 1:
                    print "\t[" + str(document[u'depth']) + "] [GET] " + document[u'url'] + " wird aufgerufen..."

                queue.remove({'_id': document[u'_id']})

                thread = crawler.Crawler(client, document[u'url'], document[u'parent'], document[u'depth'], options)
                thread.setDaemon(True)
                thread.start()
            else:
                queue.remove({'_id': document[u'_id']})
except KeyboardInterrupt:
    print "========================================================"
    print "Crawling wurde beendet."
    sys.exit(0)