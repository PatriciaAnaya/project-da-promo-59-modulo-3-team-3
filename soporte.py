import sqlite3
import pandas as pd
from sqlalchemy import create_engine

def extraer_datos(ruta_archivo):
    try:
        df = pd.read_csv(ruta_archivo, index_col=0)
        print("✅ Extracción: Datos cargados.")
        return df
    except Exception as e:
        print(f"❌ Error en la extracción: {e}")
        return None

def transformar_datos(df):
    import re
    df_limpio = df.drop_duplicates().copy()
    df_limpio = df_limpio.dropna(subset=['MonthlyIncome', 'Gender'])
   
    bins = [0, 5, 10, 15, 20, 100]
    labels = ['0-5 años', '6-10 años', '11-15 años', '16-20 años', 'Más de 20 años']
    df_limpio['TenureGroup'] = pd.cut(df_limpio['YearsAtCompany'], bins=bins, labels=labels, right=False)
    
    def limpiar_nombre(nombre):
        # Insertar espacio antes de mayúsculas (para separar PascalCase)
        nombre = re.sub(r'(?<!^)(?=[A-Z])', ' ', nombre)
        # Limpiar guiones bajos previos, pasar a minúsculas y quitar espacios extra
        return nombre.replace('_', ' ').lower().strip().replace(' ', '_')

    df_limpio.columns = [limpiar_nombre(col) for col in df_limpio.columns]

    # 2. Limpiar el CONTENIDO de las filas (solo columnas de texto)
    cols_texto = df_limpio.select_dtypes(include=['object']).columns
    
    for col in cols_texto:
        df_limpio[col] = (df_limpio[col]
                   .str.replace("_", " ", regex=False)
                   .str.title()
                   .str.strip())
    
   
    print("✅ Transformación: Limpieza completada.")
    return df_limpio

def crear_base_de_datos(nombre_db):
    conn = sqlite3.connect(nombre_db)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Empleados_Analisis (
            Gender TEXT, JobLevel INTEGER, MonthlyIncome REAL,
            PercentSalaryHike REAL, YearsAtCompany INTEGER,
            TenureGroup TEXT, WorkLifeBalance INTEGER,
            TrainingTimesLastYear INTEGER, DistanceFromHome INTEGER,
            BusinessTravel TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print(f"✅ BBDD: Estructura lista.")

def cargar_datos(df, nombre_db):
    try:
        engine = create_engine(f'sqlite:///{nombre_db}')
        df.to_sql('Empleados_Analisis', con=engine, if_exists='replace', index=False)
        print("✅ Carga: Éxito en la base de datos.")
    except Exception as e:
        print(f"❌ Error en la carga: {e}")