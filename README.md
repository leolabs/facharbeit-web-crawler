facharbeit-web-crawler
======================

Ein Crawler für Websites, geschrieben in Python, für MongoDB. Dieses Projekt wurde im Rahmen der Facharbeit "Datenbanken - Big Data" für das Gymnasium Gerresheim von Leo Bernard geschrieben


Benötigte Libraries
===================

- pymongo
- BeautifulSoup 4


Verwendung
==========

```
Usage: start.py [-w WEBSITE] [-d DEPTH] [-m MODE] [-t THREADS] [-v VERBOSITY]

Dieses Programm crawlt Websites indem es von einer gegebenen Adresse aus alle
Links bis zu einer gegebenen Tiefe aufruft und sie durchsucht. Die Ergebnisse
werden in MongoDB gespeichert. Werden keine Parameter angegeben, wird das
Programm die in MongoDB gespeicherte Warteschlange weiter abarbeiten.

Options:
  -h, --help    show this help message and exit
  -w WEBSITE    Leert die Datenbank und startet das Crawling von der gegebenen
                Adresse aus.
  -d DEPTH      Die maximale Tiefe der Links, die gecrawlt werden sollen.
                (Default: 3)
  -m MODE       Gibt an, ob der Crawler die anstehenden Seiten per Tiefensuche
                (d) oder per Breitensuche (b) arbeiten soll. (Default: b)
  -t THREADS    Gibt die anzahl der Threads an, die gleichzeitig die
                Warteschlange abarbeiten. (Default: 20)
  -v VERBOSITY  Gibt die genauigkeit der Ausgabe an: 0 - Nur Fehler; 1 - 0 &
                Aufrufe; 2 - 1 & Antworten; 3 - 2 & Antwort-Details; 4 - Alles
                (Default: 2)
```
