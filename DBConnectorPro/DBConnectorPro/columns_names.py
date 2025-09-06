
import pandas as pd

def rename_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Benennt doppelte Spaltennamen in einem DataFrame um, indem ein Suffix (_1, _2, ...) hinzugefügt wird.
    Die erste Instanz eines Spaltennamens bleibt unverändert.
    
    Args:
        df (pd.DataFrame): Das ursprüngliche DataFrame mit möglichen doppelten Spaltennamen.
    
    Returns:
        pd.DataFrame: Eine Kopie des DataFrames mit eindeutigen Spaltennamen.
    """
    new_columns = []
    seen = {}

    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)

    df_new = df.copy()
    df_new.columns = new_columns
    return df_new