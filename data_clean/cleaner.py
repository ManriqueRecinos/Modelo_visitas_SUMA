from pathlib import Path
import pandas as pd
import numpy as np


class DataCleaner:
    """Simple data loading/inspection service.

    Usage:
        cleaner = DataCleaner().load()
        cleaner.split_and_save()
    """

    def __init__(self, csv_path: str = "../main_views.csv") -> None:
        # Resuelve la ruta relativa al archivo csv
        self.csv_path = (Path(__file__).parent / csv_path).resolve()
        self.df: pd.DataFrame | None = None

    def load(self) -> "DataCleaner":
        # Asegura que la primera fila sea el header y remueve espacios alrededor de campos
        df = pd.read_csv(self.csv_path, header=0, skipinitialspace=True)
        # Normaliza nombres de columnas: quita espacios y a minúsculas
        df.columns = [str(c).strip().lower() for c in df.columns]
        # Normaliza columna 'usuario' si existe
        if 'usuario' in df.columns:
            # Convierte a string donde aplica, quita espacios, estandariza vacíos y textos como null/none/nan a NA
            s = df['usuario'].astype('string')
            s = s.str.strip()
            s = s.replace({"": pd.NA})
            s = s.replace(r"(?i)^(null|none|nan)$", pd.NA, regex=True)
            df['usuario'] = s
        self.df = df
        return self

    def head(self, n: int = 5) -> pd.DataFrame:
        if self.df is None:
            raise ValueError("Datos no cargados. llama a load() primero.")
        return self.df.head(n)

    def _ensure_output_dir(self, out_dir: Path | str = "data") -> Path:
        out_path = Path(out_dir)
        if not out_path.is_absolute():
            # crear dentro de data_clean/
            out_path = Path(__file__).parent / out_path
        out_path.mkdir(parents=True, exist_ok=True)
        return out_path

    def split_and_save(self, usuarios_col: str = "usuario", out_dir: Path | str = "data") -> dict:
        """Divide el DF en dos: usuarios no nulos y nulos, y guarda ambos CSV.
        Retorna rutas y conteos.
        """
        if self.df is None:
            raise ValueError("Datos no cargados. llama a load() primero.")

        out_path = self._ensure_output_dir(out_dir)

        if usuarios_col not in self.df.columns:
            raise KeyError(
                f"Columna '{usuarios_col}' no encontrada en el CSV. Columnas disponibles: {list(self.df.columns)}"
            )

        # Crea máscara de nulos True para valores NA (incluye vacíos y 'null'/'none'/'nan' ya normalizados)
        null_mask = self.df[usuarios_col].isna()
        df_null = self.df[null_mask]
        df_not_null = self.df[~null_mask]

        path_not_null = out_path / "usuarios_no_nulos.csv"
        path_null = out_path / "usuarios_nulos.csv"

        df_not_null.to_csv(path_not_null, index=False)
        # En el archivo de nulos, mostrar la cadena 'null' en la columna usuario
        df_null_out = df_null.copy()
        df_null_out[usuarios_col] = df_null_out[usuarios_col].fillna("null")
        df_null_out.to_csv(path_null, index=False)

        return {
            "not_null_path": str(path_not_null.resolve()),
            "null_path": str(path_null.resolve()),
            "not_null_count": int(len(df_not_null)),
            "null_count": int(len(df_null)),
            "total": int(len(self.df)),
        }


def main() -> None:
    cleaner = DataCleaner().load()
    result = cleaner.split_and_save(usuarios_col="usuario", out_dir="data")
    print(
        "Guardado:\n"
        f" - No nulos: {result['not_null_count']} -> {result['not_null_path']}\n"
        f" - Nulos:    {result['null_count']} -> {result['null_path']}\n"
        f"Total: {result['total']}"
    )


if __name__ == "__main__":
    main()