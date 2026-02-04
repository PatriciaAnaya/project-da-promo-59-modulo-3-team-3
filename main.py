# Importamos las funciones del archivo soporte.py
import soporte as sp

def ejecutar_proyecto():
    print("--- INICIANDO PROCESO ETL PARA ABC CORPORATION ---\n")
   
    # 1. Configuración de rutas
    csv_fuente = "hr.csv"
    base_datos_final = "Empresa_Talento.db"
   
    # 2. Ejecución paso a paso
    # EXTRAER
    datos_extraidos = sp.extraer_datos(csv_fuente)
   
    if datos_extraidos is not None:
        # TRANSFORMAR
        datos_listos = sp.transformar_datos(datos_extraidos)
       
        # CREAR BBDD
        sp.crear_base_de_datos(base_datos_final)
       
        # CARGAR
        sp.cargar_datos(datos_listos, base_datos_final)
       
        print("\n🌟 ¡Proyecto finalizado con éxito!")
    else:
        print("⚠️ El proceso se detuvo porque no se pudo leer el archivo original.")

if __name__ == "__main__":
    ejecutar_proyecto()