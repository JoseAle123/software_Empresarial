import pandas as pd

def explorar_archivo(nombre_archivo):
    print(f"\n--- Análisis del archivo: {nombre_archivo} ---\n")
    
    # Cargar archivo
    df = pd.read_csv(nombre_archivo)
    
    # Mostrar primeros datos
    print("Primeras filas:")
    print(df.head())
    
    # 1. Verificar valores nulos
    print("\nValores nulos por columna:")
    print(df.isnull().sum())
    
    # 2. Buscar valores inválidos comunes
    invalid_values = ['null', 'n/a', 'na', 'none', '', ' ']
    print("\nValores considerados inválidos (como 'NULL', 'n/a', ''):")
    for col in df.columns:
        count_invalid = df[col].astype(str).str.lower().isin(invalid_values).sum()
        if count_invalid > 0:
            print(f" - {col}: {count_invalid} valores inválidos")
    
    # 3. Analizar columna CATALOG si existe
    if "CATALOG" in df.columns:
        print("\nValores únicos en CATALOG:")
        print(df["CATALOG"].unique())

        print("\nFrecuencia de valores en CATALOG:")
        print(df["CATALOG"].value_counts())

# Ejecutar exploración para ambos archivos
explorar_archivo("Catalog_Orders.csv")
explorar_archivo("Web_orders.csv")
