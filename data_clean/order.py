from __future__ import annotations
from pathlib import Path
import pandas as pd

MONTHS_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


class CSVOrganizer:
    def __init__(self, out_root: str | Path = "order") -> None:
        # Carpeta de salida por defecto dentro de data_clean/
        base_dir = Path(__file__).parent
        self.out_root = (base_dir / out_root) if not Path(out_root).is_absolute() else Path(out_root)
        self.out_root.mkdir(parents=True, exist_ok=True)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df

    def _ensure_dir(self, year: int, month_num: int) -> Path:
        month_dir = self.out_root / str(year) / MONTHS_ES[month_num]
        month_dir.mkdir(parents=True, exist_ok=True)
        return month_dir

    def organize_file(self, input_csv: str | Path, date_col: str = "view_date") -> list[Path]:
        input_path = Path(input_csv)
        if not input_path.is_absolute():
            # Resolver relativo a data_clean/
            input_path = Path(__file__).parent / input_path
        if not input_path.exists():
            raise FileNotFoundError(f"No existe el archivo: {input_path}")

        df = pd.read_csv(input_path, header=0, skipinitialspace=True)
        df = self._normalize_columns(df)
        date_col = date_col.lower()
        if date_col not in df.columns:
            raise KeyError(f"Columna de fecha '{date_col}' no encontrada. Columnas: {list(df.columns)}")

        # Parseo de fecha
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

        written_paths: list[Path] = []
        base_name = input_path.stem  # p.ej., usuarios_no_nulos

        # Agrupar por año y mes
        for (year, month), g in df.groupby([df[date_col].dt.year, df[date_col].dt.month], sort=True):
            out_dir = self._ensure_dir(int(year), int(month))
            out_file = out_dir / f"{base_name}.csv"
            g.to_csv(out_file, index=False)
            written_paths.append(out_file.resolve())

        return written_paths


def main() -> None:
    organizer = CSVOrganizer(out_root="order")
    inputs = [
        Path("data") / "usuarios_no_nulos.csv",
        Path("data") / "usuarios_nulos.csv",
    ]
    total = 0
    for inp in inputs:
        written = organizer.organize_file(inp, date_col="view_date")
        print(f"Archivo {inp} -> {len(written)} archivos mensuales")
        total += len(written)
    print(f"Total de archivos generados: {total}")


if __name__ == "__main__":
    main()