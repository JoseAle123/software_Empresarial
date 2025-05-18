import pandas as pd

# Cargar datasets
web = pd.read_csv("Web_orders_CLEAN.csv")
catalog = pd.read_csv("Catalog_Orders_CLEAN.csv")
products = pd.read_csv("products.csv")

# Limpieza y conversión de fechas en catalog
catalog['DATE'] = pd.to_datetime(catalog['DATE'], dayfirst=True, errors='coerce')
catalog = catalog.dropna(subset=['DATE'])
catalog['DATE'] = catalog['DATE'].dt.strftime('%Y-%m-%d')

# Limpieza y conversión de fechas en web
web['DATE'] = pd.to_datetime(web['DATE'], dayfirst=True, errors='coerce')
web = web.dropna(subset=['DATE'])
web['DATE'] = web['DATE'].dt.strftime('%Y-%m-%d')

# Concatenar ambos datasets
merged_orders = pd.concat([web, catalog], ignore_index=True)

# Hacer merge con productos usando PCODE
merged_full = merged_orders.merge(products, on='PCODE', how='left')

# Guardar resultado final
merged_full.to_csv("Orders_Products_Merged.csv", index=False)

print("Proceso completado. Archivo guardado como Orders_Products_Merged.csv")
