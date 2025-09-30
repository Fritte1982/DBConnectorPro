# __init__.py
from .streamlit_klasse import streamlit_server
from .db_connection_manager import DB_Connection
from pathlib import Path
import sys

# Absoluter Pfad dynamisch bestimmen
package_path = Path(__file__).resolve().parent

# Füge den Pfad zum sys.path hinzu
if str(package_path) not in sys.path:
    sys.path.append(str(package_path))



# Optional: explizit festlegen, welche Symbole importiert werden können
__all__ = ['DB_Connection',"streamlit_server"]
