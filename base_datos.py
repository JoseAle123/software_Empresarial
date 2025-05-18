import pandas as pd
import psycopg2

# Configuración de conexión
DB_CONFIG = {
    "dbname": "-------",
    "user": "--------",
    "password": "------",
    "host": "localhost",
    "port": 5432
}

# Mapeo correcto de claves primarias
primary_keys = {
    "DimProveedor": "id_proveedor",
    "DimCliente": "id_cliente",
    "DimProducto": "id_producto",
    "DimTiempo": "id_tiempo"
}

# Limpieza de strings
def clean_string(s):
    if pd.isna(s):
        return None
    return str(s).strip().strip('"').strip("'")

# Función para insertar o recuperar ID
def insert_and_get_id(cursor, table, column, value):
    pk = primary_keys[table]
    cursor.execute(f"SELECT {pk} FROM {table} WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING {pk}", (value,))
    return cursor.fetchone()[0]

# Función principal del ETL
def run_etl():
    # Leer archivo CSV
    df = pd.read_csv('Orders_Products_Merged.csv', header=None)
    df.columns = [
        'ID_x', 'INV', 'PCODE', 'DATE', 'CATALOG', 'QTY',
        'CUSTNUM', 'ID_y', 'TYPE', 'DESCRIP', 'PRICE', 'COST', 'SUPPLIER'
    ]

    # Limpiar y transformar datos
    df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m-%d', errors='coerce')
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce').fillna(0).astype(int)
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
    df['COST'] = pd.to_numeric(df['COST'], errors='coerce')
    df['SUPPLIER'] = df['SUPPLIER'].apply(clean_string)
    df['CUSTNUM'] = df['CUSTNUM'].apply(clean_string)
    df['DESCRIP'] = df['DESCRIP'].apply(clean_string)
    df['TYPE'] = df['TYPE'].apply(clean_string)
    df['CATALOG'] = df['CATALOG'].apply(clean_string)
    df['PCODE'] = df['PCODE'].apply(clean_string)

    df['INGRESOS'] = df['QTY'] * df['PRICE']
    df['GANANCIA'] = df['QTY'] * (df['PRICE'] - df['COST'])

    # Conectar a PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        if pd.isna(row['SUPPLIER']) or pd.isna(row['CUSTNUM']) or pd.isna(row['DATE']):
            continue  # Saltar filas incompletas críticas

        # DimProveedor
        id_proveedor = insert_and_get_id(cursor, "DimProveedor", "nombre", row['SUPPLIER'])

        # DimCliente
        id_cliente = insert_and_get_id(cursor, "DimCliente", "nombre", row['CUSTNUM'])

        # DimTiempo
        cursor.execute("SELECT id_tiempo FROM DimTiempo WHERE fecha = %s", (row['DATE'],))
        result = cursor.fetchone()
        if result:
            id_tiempo = result[0]
        else:
            cursor.execute("""
                INSERT INTO DimTiempo (fecha, año, mes, dia)
                VALUES (%s, %s, %s, %s)
                RETURNING id_tiempo
            """, (row['DATE'], row['DATE'].year, row['DATE'].month, row['DATE'].day))
            id_tiempo = cursor.fetchone()[0]

        # DimProducto
        cursor.execute("SELECT id_producto FROM DimProducto WHERE pcode = %s", (row['PCODE'],))
        result = cursor.fetchone()
        if result:
            id_producto = result[0]
        else:
            cursor.execute("""
                INSERT INTO DimProducto (nombre, tipo, descripcion, precio, costo, pcode, id_proveedor)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id_producto
            """, (
                row['DESCRIP'], row['TYPE'], row['CATALOG'],
                row['PRICE'], row['COST'], row['PCODE'], id_proveedor
            ))
            id_producto = cursor.fetchone()[0]

        # Insertar en HechosVentas
        cursor.execute("""
            INSERT INTO HechosVentas (
                id_producto, id_cliente, id_proveedor, id_tiempo,
                cantidad, precio_unitario, costo_unitario, ingresos, ganancia
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_producto, id_cliente, id_proveedor, id_tiempo,
            row['QTY'], row['PRICE'], row['COST'], row['INGRESOS'], row['GANANCIA']
        ))

    # Confirmar y cerrar
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ ETL completado y datos cargados correctamente.")

# Ejecutar
if __name__ == "__main__":
    run_etl()
