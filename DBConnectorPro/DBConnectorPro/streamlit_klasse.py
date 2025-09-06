import streamlit as st, os, subprocess, inspect 


class streamlit_server:
    
    def __init__(self, target_script: str = None):
        """
        Initialisiert den Streamlit-Server.
        :param target_script: Der Pfad zur Hauptdatei, die von Streamlit ausgeführt werden soll.
                        Wenn None, wird der Pfad zur aufrufenden Datei automatisch ermittelt.
        """
        # Automatisch den Pfad der aufrufenden Datei ermitteln, falls kein Pfad angegeben ist
        if target_script is None:
            if __name__ == "__main__":
                self.target_script = os.path.abspath(inspect.getfile(inspect.currentframe()))
            else:
                frame = inspect.stack()[-1]
            self.target_script = os.path.abspath(frame.filename)
        else:
            self.target_script = os.path.abspath(target_script)
    
    def streamlit_run(self):
    # Prüfen, ob der Server bereits läuft
        if os.getenv("STREAMLIT_SERVER_STARTED", "") == "1":
            print("Streamlit-Server läuft bereits.")
            return False
        # Markieren, dass der Server gestartet wurde
        os.environ["STREAMLIT_SERVER_STARTED"] = "1"
        # Absoluten Skriptpfad ermitteln
        # script_path = os.path.abspath(__file__)
        # Streamlit-Server im Hintergrund starten
        process = subprocess.Popen(["streamlit", "run", self.target_script])
        try:
            # Warten, bis der Prozess beendet wird (optional)
            process.wait()
        except KeyboardInterrupt:
            # Bei `Strg + C` den Streamlit-Server stoppen
            print("Streamlit-Server wird gestoppt...")
            process.terminate()
            process.wait()
            return  False# Beenden der Funktion nach Stoppen des Servers
            
    def start_streamlit(self):
        if not self.streamlit_run():
            return         
    
    