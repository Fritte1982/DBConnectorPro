Ja, absolut 👍
In SQL Server kannst du dir alle Stored Procedures mit Schema-Namen auflisten lassen. Hier sind ein paar Varianten:

---

### 1. Über die **Katalog-Views**

```sql
SELECT 
    SCHEMA_NAME(p.schema_id) AS SchemaName,
    p.name AS ProcedureName
FROM sys.procedures p
ORDER BY SchemaName, ProcedureName;
```

---

### 2. Mit zusätzlichen Infos (z. B. Erstelldatum, Änderungsdatum)

```sql
SELECT 
    SCHEMA_NAME(p.schema_id) AS SchemaName,
    p.name AS ProcedureName,
    p.create_date,
    p.modify_date
FROM sys.procedures p
ORDER BY SchemaName, ProcedureName;
```

---

### 3. Alternative mit `INFORMATION_SCHEMA.ROUTINES`

```sql
SELECT 
    ROUTINE_SCHEMA AS SchemaName,
    ROUTINE_NAME AS ProcedureName
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
ORDER BY SchemaName, ProcedureName;
```

---

👉 **Tipp:**

* `sys.procedures` ist detaillierter (z. B. `object_id`, `is_ms_shipped`, etc.).
* `INFORMATION_SCHEMA.ROUTINES` ist SQL-Standard, aber manchmal weniger Infos.

---

Willst du, dass ich dir auch ein Query baue, das gleich den **vollständig qualifizierten Namen** (`Schema.Procedure`) als eine Spalte ausgibt, sodass du die SPs direkt mit Copy/Paste ausführen kannst?
