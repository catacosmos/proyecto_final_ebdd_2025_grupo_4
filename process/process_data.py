import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc
import pandas as pd

# CONFIGURACIÓN
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = "proyecto_final_137mb.db" 
JAR_NAME = "sqlite-jdbc-3.43.0.0.jar"
OUTPUT_FILE = "resultado_spark_final.csv"

db_path = os.path.join(BASE_DIR, DB_NAME)
jar_path = os.path.join(BASE_DIR, JAR_NAME)


# 1. Buscar la base de datos 
if not os.path.exists(db_path):
    files = [f for f in os.listdir(BASE_DIR) if f.endswith(".db")]
    if len(files) > 0:
        print(f"No se encontró '{DB_NAME}', usaré'{files[0]}'")
        db_path = os.path.join(BASE_DIR, files[0])
    else:
        print(f"ERROR: No encuentro ninguna base de datos .db en esta carpeta.")
        sys.exit(1)

# 2. Verificar el Driver JDBC
if not os.path.exists(jar_path):
    print(f"ERROR: Falta el driver '{JAR_NAME}'.")
    sys.exit(1)

print(f"\nIniciando Spark en Linux...")
print(f"   Analizando: {os.path.basename(db_path)}")

# INICIAR SPARK
spark = SparkSession.builder \
    .appName("ProyectoGaleriasLinux") \
    .master("local[*]") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

# Reducir ruido en la consola
spark.sparkContext.setLogLevel("ERROR")

try:
    # 1. LECTURA
    jdbc_url = f"jdbc:sqlite:{db_path}"
    print("Leyendo tablas desde SQLite...")
    
    df_ventas = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "VENTA").option("driver", "org.sqlite.JDBC").load()
    df_productos = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "PRODUCTO").option("driver", "org.sqlite.JDBC").load()

    total_filas = df_ventas.count()
    print(f"Total registros cargados: {total_filas:,}")

    # 2. PROCESAMIENTO DISTRIBUIDO
    df_ventas = df_ventas.repartition(4)
    
    print("Ejecutando agregaciones y cruce de datos...")
    df_resultado = df_ventas.join(df_productos, "id_prod") \
        .groupBy("nombre") \
        .agg(
            sum("monto").alias("total_vendido"), 
            count("id_venta").alias("cantidad_ventas")
        ) \
        .orderBy(desc("total_vendido"))

    # MOSTRAR RESULTADOS
    print("\nTop 5 Obras más vendidas:")
    df_resultado.show(5, truncate=False)

    # GUARDAR CSV
    print("Guardando reporte final...")
    
    pdf_resultado = df_resultado.toPandas()
    output_path = os.path.join(BASE_DIR, OUTPUT_FILE)
    pdf_resultado.to_csv(output_path, index=False)
    
    print(f"LISTO - Archivo generado: {OUTPUT_FILE}")

except Exception as e:
    print(f"\nError: {e}")

finally:
    spark.stop()