Alles klar! Ich erstelle dir eine **komplette Dokumentation für einen GitHub Actions Workflow** für ein Python-Projekt, inklusive **Installation von Dev-Abhängigkeiten, Linting mit Ruff, Testlauf mit pytest** und Bereinigung alter Template-Ordner / `__pycache__`.

---

# GitHub Actions Workflow für Python-Projekte

## 1️⃣ Ziel

* Automatische Installation von Python-Abhängigkeiten (Runtime + Dev)
* Linting mit **Ruff**
* Testausführung mit **pytest**
* Bereinigung von alten Template-Dateien und `__pycache__`, um Importkonflikte zu vermeiden
* Läuft auf allen Commits (`push`) und Pull Requests (`pull_request`) auf den Hauptbranches (`main` / `master`)
* Unterstützt mehrere Python-Versionen (z. B. 3.12, 3.13)

---

## 2️⃣ Projektstruktur

```
my_project/
├─ .github/
│  └─ workflows/
│     └─ python-ci.yml       # GitHub Actions Workflow
├─ requirements_dev.txt      # Dev-Abhängigkeiten (pytest, ruff, etc.)
├─ pyproject.toml
├─ setup.py
├─ my_package/
│  └─ __init__.py
└─ tests/
   └─ test_module.py
```

---

## 3️⃣ `requirements_dev.txt` Beispiel

```text
pytest
ruff
black
flake8
mypy
pre-commit
```

* Enthält **alle Dev-Tools**, die im Workflow genutzt werden.
* Keine Platzhalter-Versionen (`0.0.xx`) – entweder ohne Version oder konkrete Version (`ruff==0.12.12`).

---

## 4️⃣ GitHub Actions Workflow-Datei

**Pfad:** `.github/workflows/python-ci.yml`

```yaml
name: Python CI

on:
  push:
    branches: [ "main", "master" ]
  pull_request:
    branches: [ "main", "master" ]

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.12, 3.13]

    steps:
      # Repository auschecken
      - name: Checkout repository
        uses: actions/checkout@v4

      # Python-Setup
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      # Bereinigen alter Template-Dateien und __pycache__
      - name: Cleanup old template and cache
        run: |
          rm -rf DB_Connector_Pro_template || true
          find . -name "__pycache__" -type d -exec rm -rf {} +
          find . -name "*.pyc" -delete

      # Dev-Abhängigkeiten installieren
      - name: Install dev dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements_dev.txt

      # Linting mit Ruff
      - name: Lint with Ruff
        run: |
          ruff check --select=E9,F63,F7,F82
          ruff check --exit-zero --statistics

      # Projekt installieren
      - name: Install project
        run: pip install .

      # Tests ausführen
      - name: Run tests
        run: pytest
```

---

## 5️⃣ Erklärung der Steps

| Step                             | Zweck                                                                                                      |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `Checkout repository`            | Holt den Code vom GitHub-Repository                                                                        |
| `Set up Python`                  | Installiert die gewünschte Python-Version                                                                  |
| `Cleanup old template and cache` | Entfernt alte Cookiecutter-Template-Ordner und `__pycache__`, `.pyc`-Dateien, um Importfehler zu vermeiden |
| `Install dev dependencies`       | Installiert alle Entwicklungs-Abhängigkeiten aus `requirements_dev.txt`                                    |
| `Lint with Ruff`                 | Prüft den Code auf Syntaxfehler, undefinierte Namen und Code-Style-Probleme                                |
| `Install project`                | Installiert das aktuelle Projekt, damit Tests laufen                                                       |
| `Run tests`                      | Führt alle pytest-Tests aus                                                                                |

---

## 6️⃣ Hinweise & Best Practices

1. **Dev-Tools in `requirements_dev.txt`** immer aktuell halten.
2. **Template-Ordner nach Cookiecutter-Generierung entfernen**, sonst Import-Konflikte bei pytest.
3. **Python-Versionen testen** über die Matrix (`3.12`, `3.13`, …)
4. **Exit Codes**:

   * Ruff: `--exit-zero` für Statistiken ohne Build-Abbruch
   * pytest: Exit-Code 1+ signalisiert Testfehler

---

✅ Mit diesem Workflow hast du **einen vollständigen CI/CD-Check** für Python-Projekte auf GitHub, der automatisch lintet, testet und Fehler durch alte Template-Dateien vermeidet.

---

Wenn du willst, kann ich dir noch eine **PowerShell-Version für lokale Entwickler** schreiben, die die gleichen Cleanup- und Test-Schritte ausführt, damit du lokal die gleiche Umgebung wie GitHub Actions hast.

Willst du, dass ich das mache?
