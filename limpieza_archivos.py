import pandas as pd

# Función para limpiar un DataFrame dado
def limpiar_df(df, nombre_archivo):
    print(f"\n--- LIMPIEZA DE: {nombre_archivo} ---\n")

    # 1. Verificar valores nulos
    print("Valores nulos por columna:")
    print(df.isnull().sum())

    # También revisar strings inválidos
    invalid_values = ["NULL", "null", "n/a", "na", "none", "", " "]
    print("\nValores inválidos (como 'NULL', 'n/a'):")
    for col in df.columns:
        count = df[col].astype(str).str.lower().isin(invalid_values).sum()
        if count > 0:
            print(f" - {col}: {count} valores inválidos")

    # 2. Convertir QTY a numérico
    if "QTY" in df.columns:
        df["QTY"] = pd.to_numeric(df["QTY"], errors="coerce")
        if df["QTY"].isnull().sum() > 0:
            mediana_qty = df["QTY"].median()
            df["QTY"].fillna(mediana_qty, inplace=True)
            print(f"\nValores faltantes en QTY reemplazados por la mediana: {mediana_qty}")

    # 3. Normalizar la columna CATALOG
    if "CATALOG" in df.columns:
        df["CATALOG"] = df["CATALOG"].str.strip().str.capitalize()

        # Correcciones comunes
        correcciones = {
            "Sporst": "Sports",
            "Sport": "Sports",
            "Spots": "Sports",
            "Tosy": "Toys",
            "Toy": "Toys",
            "Tots": "Toys",
            "Pet": "Pets",
            "Pest": "Pets",
            "Prts": "Pets",
            "Pats": "Pets",
            "Gardning": "Gardening",
            "Gardenings": "Gardening",
            "GARDENING": "Gardening",
            "Garden": "Gardening",
            "Softwars": "Software",
            "Softwar": "Software",
            "Softwares": "Software",
            "Collectable": "Collectibles",
            "Collectibles": "Collectibles",
            "Colectibles": "Collectibles",
            "Collectables": "Collectibles",
            "Collectible": "Collectibles"
        }
        df["CATALOG"] = df["CATALOG"].replace(correcciones)

    # 4. Eliminar duplicados
    original_len = len(df)
    df.drop_duplicates(inplace=True)
    print(f"\nDuplicados eliminados: {original_len - len(df)}")

    # 5. Mostrar valores únicos en CATALOG
    if "CATALOG" in df.columns:
        print("\nValores únicos en CATALOG (limpios):")
        print(df["CATALOG"].value_counts())

    # 6. Guardar archivo limpio
    output_file = nombre_archivo.replace(".csv", "_CLEAN.csv")
    df.to_csv(output_file, index=False)
    print(f"\nArchivo limpio guardado como: {output_file}\n")


# Limpiar Catalog_Orders.csv
df_catalog = pd.read_csv("Catalog_Orders.csv")
limpiar_df(df_catalog, "Catalog_Orders.csv")

# Limpiar Web_orders.csv
df_web = pd.read_csv("Web_orders.csv")
limpiar_df(df_web, "Web_orders.csv")
