from pathlib import Path
from data_clean.cleaner import DataCleaner
from data_clean.order import CSVOrganizer

CLEAN_DIR = Path(__file__).parent / "data_clean" / "data"
NO_NULOS = CLEAN_DIR / "usuarios_no_nulos.csv"
NULOS = CLEAN_DIR / "usuarios_nulos.csv"


def run_clean() -> None:
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
        # Si el nombre exacto de la columna difiere
        print(f"{e}\nColumnas disponibles: {list(cleaner.df.columns)}")


def run_order() -> None:
    # Verificar que existan los archivos limpios; si no, limpiar primero
    if not NO_NULOS.exists() or not NULOS.exists():
        print("Aún no se ha limpiado. Ejecutando limpieza antes de organizar...")
        run_clean()

    organizer = CSVOrganizer(out_root="order")
    inputs = [NO_NULOS, NULOS]
    total = 0
    for inp in inputs:
        written = organizer.organize_file(inp, date_col="view_date")
        print(f"Archivo {inp} -> {len(written)} archivos mensuales")
        total += len(written)
    print(f"Total de archivos generados: {total}")


def main() -> None:
    print("¿Qué deseas hacer?\n[L]impiar\n[O]rganizar\n[A]mbos")
    choice = input("> ").strip().lower()

    if choice in ("l", "limpiar"):
        run_clean()
    elif choice in ("o", "organizar"):
        run_order()
    elif choice in ("a", "ambos"):
        run_clean()
        run_order()
    else:
        print("Opción no reconocida. Ejecutando 'Organizar' con autoclean si es necesario...")
        run_order()


if __name__ == "__main__":
    main()