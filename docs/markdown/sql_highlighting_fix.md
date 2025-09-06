Ah, jetzt wird es klar 😅

Das liegt **nicht an deinem SQL-Code** – PyCharm kann **innerhalb von Python-Strings kein SQL-Highlighting automatisch** machen.

Auch ein **roher String** (`r""" ... """`) ändert daran nichts. Selbst einfache SQL-Befehle in dreifachen Anführungszeichen werden als **normaler Python-String** behandelt.

---

### Möglichkeiten, trotzdem Highlighting zu bekommen:

1. **Manuelle Language Injection in PyCharm**

   * Cursor in den String setzen
   * `Alt + Enter` → „**Inject language or reference**“ → **SQL** auswählen
   * Danach wird der String SQL-hervorgehoben

2. **SQL in externe `.sql`-Datei auslagern**

   * Speichere den Code in `uspCustomerIn_Select_firstTracking.sql`
   * PyCharm zeigt **vollständiges SQL-Highlighting**
   * Python-Code liest die Datei ein:

     ```python
     with open("uspCustomerIn_Select_firstTracking.sql", "r") as f:
         sp_in_customer_out_tracking = f.read()
     ```

3. **Keine native Unterstützung für `GO`**

   * PyCharm erkennt `GO` nicht als Batch-Trenner in Strings
   * In externen `.sql`-Dateien wird es korrekt behandelt

---

💡 Kurz: **Highlighting in Python-Strings funktioniert nur über Language Injection oder externe SQL-Dateien**.
Der Typ `r""" ... """` alleine reicht **nicht**, egal wie komplex der SQL-Code ist.

Wenn du willst, kann ich dir ein kleines **Step-by-Step Beispiel zeigen**, wie du `GO` und Language Injection gleichzeitig sauber in PyCharm nutzen kannst. Willst du, dass ich das mache?
