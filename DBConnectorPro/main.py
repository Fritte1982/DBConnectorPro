from DBConnectorPro.db_connection_manager import DB_Connection
import os

user: str = "emp_exercise_1"
view_schema_keys = """SELECT *
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE """
order_by_schema = "ORDER BY TABLE_NAME "
schema_tabelle_name = view_schema_keys + order_by_schema
user_file = r"user_file.json"


def main() -> None:
    print(os.getcwd())
    db_excercis = DB_Connection(file_path=user_file)
    db_excercis.set_user(user_key=user)
    db_excercis.driver = "{ODBC Driver 17 for SQL Server}"
    db_excercis._setup_connection()
    db_excercis.daten_spalten(view_schema_keys)
    db_excercis.tabellen_ausgabe()


if __name__ == "__main__":
    main()

# TODO den getter im default ausprobieren, wenn keine json angelegt ist
# TODO work with Git and Branches
# TODO rebuilt the db_connection_manager-modul in new Branch, oriented on commonsense Design-Pattern
