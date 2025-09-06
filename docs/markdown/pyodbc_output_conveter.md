**Output-Konverter** in `pyodbc` sind eine Funktionalität, mit der du beim Lesen von Daten aus der Datenbank bestimmte Datentypen **automatisch umwandeln** kannst – z. B. `datetimeoffset` oder `geometry`, die `pyodbc` sonst nicht oder fehlerhaft verarbeitet.

---

### 🧠 Hintergrund

Wenn du mit `pyodbc` auf eine Spalte zugreifst, deren SQL-Typ von `pyodbc` nicht nativ unterstützt wird (z. B. `-151` = `datetimeoffset`), bekommst du:

> `pyodbc.ProgrammingError: ODBC SQL type -151 is not yet supported.`

Das passiert, **bevor** du mit dem Ergebnis weiterarbeiten kannst.

---

### ✅ Lösung: Output-Konverter

Mit einem Output-Konverter sagst du `pyodbc`:

> "Wenn du Typ `X` siehst, dann verarbeite ihn mit meiner eigenen Funktion."

---

### 🛠 Beispiel: `datetimeoffset` behandeln

```python
import pyodbc
from datetime import datetime

def handle_datetimeoffset(value):
    if value is None:
        return None
    # SQL Server gibt datetimeoffset als string zurück, Beispiel:
    # '2025-06-22 10:20:00 +02:00'
    return str(value)

# pyodbc-Konverter registrieren
pyodbc.add_output_converter(-151, handle_datetimeoffset)

# Verbindung aufbauen wie gewohnt
cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};Server=...;Trusted_Connection=yes;")
cursor = cnxn.cursor()

cursor.execute("SELECT MyDatetimeOffsetColumn FROM MyTable")
rows = cursor.fetchall()
print(rows)
```

---

### 🔎 Typen-ID-Liste (Auszug)

| SQL Server Datentyp    | ODBC Typ-ID | Hinweis                           |
| ---------------------- | ----------- | --------------------------------- |
| `datetimeoffset`       | `-151`      | Muss explizit behandelt werden    |
| `geometry`/`geography` | `-151`      | (auch -151; muss geprüft werden)  |
| `hierarchyid`          | n/a         | (oft als `nvarchar` zurückcasten) |

---

### 💡 Wann sinnvoll?

* Wenn du **nicht alle Spalten CASTen** willst im SQL.
* Wenn du **mehrere Tabellen und viele Datentypen** hast.
* Wenn du bereits `SELECT *` nutzt und **nachträglich behandeln** willst.

---

### 📌 Hinweis

Du kannst mehrere Output-Konverter gleichzeitig registrieren – sie greifen nur, wenn der entsprechende ODBC-Typ auftaucht.

---

Wenn du willst, kann ich dir auch eine Utility-Klasse schreiben, die automatisch alle registrierten problematischen Typen abfängt und in Strings umwandelt.


Hier sind gute **Lernressourcen**, um sich gezielt mit **Output-Konvertern in `pyodbc`** vertraut zu machen:

---

### 📘 1. **Offizielle `pyodbc` Dokumentation**

Die wichtigste Quelle zur `add_output_converter()` Funktion:

🔗 [https://github.com/mkleehammer/pyodbc/wiki/UsingOutputConverters](https://github.com/mkleehammer/pyodbc/wiki/UsingOutputConverters)

> Erklärt das Prinzip der Konverter, wie man sie mit `add_output_converter()` registriert und worauf man achten muss – z. B. dass der Wert als `bytes` übergeben wird (nicht `str`!).

---

### 🧪 2. **Konkretes Beispiel für `datetimeoffset`**

GitHub-Issue mit praktischer Lösung für genau deinen Fall:

🔗 [https://github.com/mkleehammer/pyodbc/issues/398](https://github.com/mkleehammer/pyodbc/issues/398)

> User erklären dort, wie man `datetimeoffset` behandelt, da SQL Server ihn nicht korrekt mapped. Enthält funktionierenden Beispielcode.

---

### 📚 3. **Stack Overflow Beispiele**

* Typ `-151` (datetimeoffset):
  🔗 [https://stackoverflow.com/q/58894627](https://stackoverflow.com/q/58894627)

* Allgemeine Output-Konverter mit `pyodbc`:
  🔗 [https://stackoverflow.com/q/59017411](https://stackoverflow.com/q/59017411)

> Oft bekommst du dort realistische Szenarien – etwa für Geometry-Daten oder benutzerdefinierte Typen.

---

### 🔧 4. **Tiefer Einstieg: ODBC-Typen verstehen**

Wenn du verstehen willst, **welche Typ-IDs** in `pyodbc` auftauchen und wie sie dem SQL-Server entsprechen:

🔗 [ODBC Data Types (Microsoft Docs)](https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types)

---

### 🛠 Kurzes praktisches Beispiel

```python
import pyodbc

def convert_datetimeoffset(raw_bytes):
    if raw_bytes is None:
        return None
    return raw_bytes.decode('utf-8')  # alternativ: parse mit datetime + tz

pyodbc.add_output_converter(-151, convert_datetimeoffset)
```

---

### 💡 Tipp für den Alltag

Wenn du viele Queries hast und nicht alle problematischen Spalten im SQL `CAST()`en willst, ist ein Output-Konverter oft **die stabilste, wartbare Lösung**.

---

Wenn du möchtest, kann ich dir auch ein **Template mit Logging, Typ-IDs und dynamischer Registrierung** von Konvertern erstellen. Sag einfach Bescheid.

Alles klar! Du willst also einen **kurzen, handhabbaren Output-Konverter** für den ODBC-Datentyp `-151`, der bei dir den SQL Server Datentyp `geography` (Raumdaten) repräsentiert, richtig?

---

### Kontext zu `-151` und `geography`:

* In SQL Server ist der `geography`-Datentyp ein spezieller Typ für Geodaten (Punkte, Linien, Polygone).
* Über ODBC wird der Typ als `-151` (SQL Server spezifischer SQLType) identifiziert.
* `pyodbc` liest die `geography`-Spalte in der Regel als `bytes` oder als eine Binärstruktur.
* Um die Geodaten sinnvoll zu nutzen, musst du die Binärdaten in ein verständliches Format umwandeln, z.B. **WKT (Well Known Text)** oder ähnliches.

---

### So kannst du das in pyodbc mit einem Output-Konverter machen

`pyodbc` unterstützt keine globale Registrierung von Output-Konvertern per `add_output_converter` (gibt es nicht).

Aber du kannst eine **eigene Funktion schreiben**, die nach dem Abruf die Spalte mit Typ `-151` (geography) in ein WKT-String konvertiert.

---

### Schritt 1: Binärdaten des geography-Typs in WKT umwandeln

Dafür kannst du in Python z.B. die Bibliothek [`shapely`](https://shapely.readthedocs.io/en/stable/) verwenden (für Geometrien) zusammen mit \[`geopandas`] oder `wkb`-Modul, das Well-Known-Binary in WKT umwandeln kann.

```bash
pip install shapely
```

---

### Beispielcode

```python
import pyodbc
from shapely import wkb

# Beispiel-Funktion: geography (bytes) zu WKT String
def geography_to_wkt(value):
    if value is None:
        return None
    try:
        geom = wkb.loads(value)
        return geom.wkt  # Gibt Well-Known-Text zurück
    except Exception as e:
        print("Fehler bei der Umwandlung:", e)
        return None

# Verbindung
conn = pyodbc.connect("deine_connection_string")
cursor = conn.cursor()

# Beispielabfrage
cursor.execute("SELECT geography_column FROM deine_tabelle")

for row in cursor:
    geography_value = row[0]  # bytes von geography-Spalte
    wkt_string = geography_to_wkt(geography_value)
    print(wkt_string)
```

---

### Wie machst du das "automatisch" beim Abrufen?

Du kannst eine kleine Wrapper-Funktion schreiben, die dir bei jedem Abruf diese Umwandlung macht, z.B.:

```python
def fetch_with_geo_wkt(cursor):
    for row in cursor:
        row = list(row)  # tuple in Liste wandeln (mutable)
        # z.B. geography in Spalte 0
        if row[0] is not None:
            row[0] = geography_to_wkt(row[0])
        yield tuple(row)
```

---

### Kurz gesagt:

* `pyodbc` selbst konvertiert `-151 geography` nicht automatisch.
* Hole die rohen Bytes und wandle sie mit `shapely.wkb.loads` in WKT um.
* Verwende deine eigene Wrapper-Funktion, um das bei jeder Abfrage anzuwenden.

---

Falls du magst, kann ich dir auch ein kleines Paket-Skript schreiben, das das für dich kapselt. Oder möchtest du es lieber so manuell machen?
Guter Punkt! Tatsächlich gibt es in `pyodbc` **die Methode `add_output_converter`**, aber **nicht im Modul `pyodbc` direkt**, sondern als Methode des **Connection-Objekts** (`cnxn`).

---

### Erklärung

* `pyodbc` hat keine `add_output_converter` auf Modulebene, also **kein** `pyodbc.add_output_converter(...)`.
* Aber du kannst **für deine Verbindung** (`cnxn`) einen Output-Konverter registrieren mit:

```python
cnxn.add_output_converter(sql_type_code, converter_function)
```

* Das heißt, du machst das **für jede Verbindung separat**, z.B.:

```python
import pyodbc

def _handle_geometry(value):
    if value is None:
        return None
    # Beispiel: Wandle bytes (geography) in WKT um, hier nur Dummy:
    return value.hex()  # oder eigene Logik mit shapely.wkb.loads

cnxn = pyodbc.connect('deine_connection_string')

# Output-Konverter für SQL Server Geography (-151) registrieren
cnxn.add_output_converter(-151, _handle_geometry)

cursor = cnxn.cursor()
cursor.execute("SELECT geography_column FROM deine_tabelle")

for row in cursor:
    print(row[0])  # Hier wird _handle_geometry automatisch angewendet
```

---

### Wichtig

* `-151` ist der SQL-Typ-Code für `geography` (SQL Server).
* `add_output_converter` ist eine **Methode der `Connection`-Instanz**, nicht vom `pyodbc`-Modul.

---

### Zusammenfassung

* Falscher Fehler kommt, wenn du schreibst:

```python
pyodbc.add_output_converter(...)  # geht nicht
```

* Richtig ist:

```python
cnxn = pyodbc.connect(...)
cnxn.add_output_converter(-151, deine_converter_funktion)
```

---

Wenn du möchtest, kann ich dir helfen, den Converter passend für deine `geography`-Spalte zu schreiben! Magst du?
