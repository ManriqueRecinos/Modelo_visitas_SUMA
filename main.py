from data_clean.cleaner import DataCleaner

def run():
    cleaner = DataCleaner().load()
    try:
        result = cleaner.split_and_save(usuarios_col="usuario", out_dir="data")
        print(
            "Guardado:\n"
            f" - No nulos: {result['not_null_count']} -> {result['not_null_path']}\n"
            f" - Nulos:    {result['null_count']} -> {result['null_path']}\n"
            f"Total: {result['total']}"
        )
    except KeyError as e:
        # Si el nombre exacto de la columna difiere (espacios, mayúsculas, BOM, etc.)
        print(f"{e}\nColumnas disponibles: {list(cleaner.df.columns)}")

if __name__ == "__main__":
    run()