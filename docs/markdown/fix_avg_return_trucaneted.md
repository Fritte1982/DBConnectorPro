Alles klar 👍 — jetzt ist’s klar: du meinst **Microsoft SQL Server (MSSQL / T-SQL)**.
Dort passiert genau das gleiche: `AVG()` über `INT` → Ergebnis wird als **Integer** berechnet, Nachkommastellen gehen verloren.

---

# 📘 Doku: `AVG()` in Microsoft SQL Server (MSSQL)

## Problem

Bei Aggregationen über Ganzzahlen rechnet SQL Server standardmäßig mit `INT`.
Dadurch wird eine **Ganzzahldivision** durchgeführt, Nachkommastellen werden abgeschnitten.

### Beispiel:

```sql
SELECT AVG(num) 
FROM (VALUES (1), (2)) AS t(num);
```

Erwartet: `1.5`
SQL Server gibt zurück: `1`

---

## Ursache

* `COUNT()` → liefert `INT`
* `AVG(INT)` → Ergebnis ebenfalls `INT` → Bruchteil geht verloren

---

## Lösung: CAST oder CONVERT

Damit `AVG()` mit Gleitkommazahlen arbeitet, den Wert vorher umwandeln.

### CAST auf FLOAT

```sql
SELECT AVG(CAST(num AS FLOAT)) AS [AVG(num)]
FROM (
    SELECT O.id, O.name, COUNT(U.id) as num
    FROM owner O
    LEFT JOIN upfall U ON O.id = U.owner_id
    GROUP BY O.id, O.name
) my_subquery;
```

### CAST auf DECIMAL (präziser, wenn es auf Nachkommastellen ankommt)

```sql
SELECT AVG(CAST(num AS DECIMAL(10,2))) AS [AVG(num)]
FROM (
    SELECT O.id, O.name, COUNT(U.id) as num
    FROM owner O
    LEFT JOIN upfall U ON O.id = U.owner_id
    GROUP BY O.id, O.name
) my_subquery;
```

---

## Best Practice für MSSQL

* **FLOAT** → wenn ungefähre Genauigkeit reicht (z. B. Messwerte).
* **DECIMAL(p,s)** → wenn exakte Werte gebraucht werden (z. B. Geldbeträge).
* Immer bewusst casten, wenn du nicht nur Ganzzahlen erwartest.

---

👉 Soll ich dir dein Beispiel gleich mit `DECIMAL` umschreiben, sodass es **fix 2 Nachkommastellen** anzeigt (z. B. `1.60` statt `1.6`)?
