from  pathlib import Path


userfile_name =  "user_file.json"

skrip_path = __file__
skrip_path = Path(skrip_path)
skrip_path = skrip_path.parent

user_daten_folder = skrip_path.parent / "user_daten"
user_file = user_daten_folder /  userfile_name


package_path =(r"F:\Backup-23-01-25-a\D-backup-2023-01-25\desk-ab-21\office-training" +
r"\Python-ordner-a\python-scripts_vs-code\sonstige_projekte\SQL_Python"+
r"\employees__exercises\OOP_Herangehnsweise")



def main():
    if user_file.exists():
        print(user_file, "OK")


if __name__ == '__main__':
    main()
