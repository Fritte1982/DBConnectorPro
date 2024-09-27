import pyodbc
import json
import os
import pandas as pd
from tabulate import tabulate
from pathlib import Path

pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class DefaultValues:
    def __init__(self) -> None:
        self.default_values = {'driver': "{SQL SERVER}",
                               "host": r"FRITTE2\SQLEXPRESS",
                               "db_name": "employees",
                               "win_auth": "yes"}

    def get_defaults(self):
        return self.default_values


class FileManager:
    def __init__(self, file_path: str = "excer_sql_attributes.json") -> None:
        self.file_path = Path(file_path)
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_attributes(self):
        if os.path.exists(self.file_path):
            with self.file_path.open('r') as file:
                return json.load(file)
        return {}

    def _save_attributes(self, data):
        self._ensure_directory_exists()
        with self.file_path.open("w") as file:
            json.dump(data, file, indent=4)


class UserKey:
    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.property_manager = None
        self.all_attributes = self.file_manager._load_attributes()
        self.current_user_key = None

    def set_property_manager(self, property_manager):
        self.property_manager = property_manager

    def load_user_attributes(self, userkey):
        if self.property_manager is None:
            raise ValueError("PropertyManager ist noch nicht gesetzt")

        # Lade die gespeicherten Benutzerattribute, wenn sie existieren
        return self.all_attributes.get(userkey, {})

    def save_user_attributes(self):
        if self.current_user_key:
            self.all_attributes = self.file_manager._load_attributes()
            updated_attributes = {k: v for k, v in self.property_manager._attributes.items()
                                  if v != self.property_manager.default_values.get(k)}

            if updated_attributes:
                self.all_attributes[self.current_user_key] = updated_attributes
                self.file_manager._save_attributes(self.all_attributes)

    def set_user_key(self, new_user_key):
        if self.current_user_key is not None:
            self.save_user_attributes()

        self.current_user_key = new_user_key

        if self.property_manager:
            new_attributes = self.load_user_attributes(new_user_key)
            self.property_manager._attributes = self.property_manager._merge_attributes(new_attributes)
        else:
            raise ValueError("PropertyManager ist noch nicht gesetzt")


class PropertyManager:
    def __init__(self, file_manager: FileManager, default=None, initial_values=None) -> None:
        self.file_manager = file_manager
        self.user_key = None  # Wird später gesetzt
        self.default = default
        self.default_values = DefaultValues().get_defaults()
        self._attributes = {}

        if initial_values is None:
            initial_values = {}

        self._initial_values = initial_values

    def set_user_key(self, user_key: UserKey):
        self.user_key = user_key

        # Verzögerte Initialisierung der Attribute, nachdem UserKey gesetzt wurde
        if self.user_key is not None:
            self._initialize_attributes(self._initial_values)

    def _initialize_attributes(self, initial_values):
        if self.user_key is None:
            raise ValueError("UserKey ist noch nicht gesetzt")

        if self.default is True:
            # Verwende Standardwerte
            self._attributes = {**self.default_values, **initial_values}

        elif self.default is False:
            # Benutzerdefinierte Werte laden und dann mergen
            user_attributes = self.user_key.load_user_attributes(self.user_key.current_user_key)
            self._attributes = {**user_attributes, **initial_values}

        else:
            self._attributes = self._merge_attributes(initial_values)

    def _merge_attributes(self, initial_values):
        # Merging der Attribute
        merged_attributes = {**self.default_values, **initial_values}
        return merged_attributes

    def get(self, key):
        return self._attributes.get(key, self.default_values.get(key))

    def set(self, key, value):
        if key in self.default_values:
            self._attributes[key] = value
        else:
            raise KeyError(f"Der Schlüssel '{key}' ist nicht in den Standardwerten definiert")
        self.user_key.save_user_attributes()

    def reset_to_defaults(self):
        self._attributes = self.default_values.copy()
        self.user_key.save_user_attributes()


class ConnectingAttributesMixin:
    """Klasse für die Setter und Getter der Attribute, die im PropertyManager
    verwaltet werden, realisiert über property-Dekoratoren"""

    @property
    def driver(self):
        return self.property_manager.get("driver")

    @driver.setter
    def driver(self, value):
        self.property_manager.set("driver", value)

    @property
    def host(self):
        return self.property_manager.get("host")

    @host.setter
    def host(self, value):
        self.property_manager.set("host", value)

    @property
    def db_name(self):
        return self.property_manager.get("db_name")

    @db_name.setter
    def db_name(self, value):
        self.property_manager.set("db_name", value)

    @property
    def win_auth(self):
        return self.property_manager.get("win_auth")

    @win_auth.setter
    def win_auth(self, value):
        self.property_manager.set("win_auth", value)


class DB_Connection(ConnectingAttributesMixin):
    def __init__(self, file_path: str = "excer_sql_attributes.json", initial_values=None, default=None) -> None:
        self.file_manager = FileManager(file_path)
        self.user_key = UserKey(self.file_manager)
        self.property_manager = PropertyManager(self.file_manager, initial_values=initial_values, default=default)

        # Setze PropertyManager für UserKey
        self.user_key.set_property_manager(self.property_manager)
        # Setze UserKey für PropertyManager
        self.property_manager.set_user_key(self.user_key)

    def set_user(self, user_key: str):
        self.user_key.set_user_key(user_key)

    def _setup_connection(self):
        cnxn_string = (
            f"Driver={self.driver}; "
            f"Server={self.host}; "
            f"database={self.db_name}; "
            f"Trusted_Connection={self.win_auth}"
        )
        self.cnxn_string = cnxn_string

    def connect_n_cursor(self):
        cnxn = pyodbc.connect(self.cnxn_string)
        cursor = cnxn.cursor()
        self.cursor = cursor
        return self.cursor

    def daten_spalten(self, query: str):
        self.query = query
        self.cursor = self.connect_n_cursor()
        self.cursor.execute(query)
        self.data = self.cursor.fetchall()
        describe = self.cursor.description
        self.header_list = [i[0] for i in describe]
        return self.data, self.header_list

    def tabellen_ausgabe(self, data=None, header=None):
        if not data:
            data = self.data
            data = [list(i) for i in data]
        if not header:
            header = self.header_list
        df = pd.DataFrame(data=data, columns=header)
        tabelle = tabulate(df, headers=header, tablefmt="presto")
        print(tabelle)


def main():
    cnxnstring = DB_Connection(file_path="Test.json")
    cnxnstring.set_user("test1")
    cnxnstring.driver = "{ODBC Driver 17 for SQL Server}"
    print(cnxnstring.driver)
    # cnxnstring.set_user("test2")
    # cnxnstring.db_name ="Northwind"
    # print(cnxnstring.db_name)
    cnxnstring._setup_connection()
    select_anweisung = "SELECT TOP (10) * "
    from_quali = "FROM employees e "
    query = select_anweisung + from_quali
    cnxnstring.daten_spalten(query)
    cnxnstring.tabellen_ausgabe()


if __name__ == "__main__":
    main()

# das Json wird erst erstellt wenn ein cnx-Attribute geändert wird, sonst default.
# TODO Deskriptoren,  Dataclass statt property-decorator
# DONE Serialisierung der Attribute mach Benutzer trennen und trotzdem bequem auf die attribute zugreifen können.
# TODO json und Pfad anlegen wenn nicht vorhanden.
# TODO die Hauptklasse teilen, eine Klasse DB_query_output,
# TODO eine Methode drop_dublicated die auch mit durch Joins qualifizierte Spalten-Namen, die sich am Präfix
#  unterscheiden klar kommt.
# TODO den getter im default ausprobieren, wenn keine json angelegt ist
# TODO Tests und Exceptions
