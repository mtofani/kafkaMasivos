import mysql.connector
from datetime import datetime, timedelta, timezone
import configparser
import pytz
import os
import logging


def cargar_configuracion():
    # Construir la ruta completa del archivo de configuración
    ruta_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')

    # Cargar la configuración desde el archivo
    config = configparser.ConfigParser()
    config.read(ruta_config)

    return config

def obtener_conexion():
    config = cargar_configuracion()["DB"]
    return mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

def convertir_a_gmt3(time_stamp):
    # Convertir a zona horaria UTC
    time_stamp_utc = time_stamp.replace(tzinfo=pytz.UTC)

    # Convertir a zona horaria GMT-3
    time_stamp_gmt3 = time_stamp_utc.astimezone(pytz.timezone("America/Argentina/Buenos_Aires"))

    return time_stamp_gmt3



def verificar_tiempo_fila(cursor, umbral_minutos):
    # Query para obtener la información
    query = "SELECT * FROM `vno-events` order by time_stamp DESC LIMIT 1;"
    cursor.execute(query)
    row = cursor.fetchone()

    if row:
        logging.info("Respuesta de la base de datos: %s", row)

        # Negar la condición: si serial_alarm_id NO contiene al menos dos puntos
        if not row["serial_alarm_id"].count('.') >= 2:
            logging.info("Se aplica conversión a GMT-3 para serial_alarm_id: %s", row["serial_alarm_id"])

            # Asegurarse de que time_stamp sea un objeto datetime
            if isinstance(row["time_stamp"], datetime):
                time_stamp_gmt3 = convertir_a_gmt3(row["time_stamp"])
            else:
                time_stamp_gmt3 = convertir_a_gmt3(datetime.strptime(row["time_stamp"], "%Y-%m-%d %H:%M:%S"))

            # Obtener la hora actual en GMT-3 directamente
            ahora_gmt3 = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires")).replace(microsecond=0)

            # Aplicar conversión solo si la condición se cumple
            if isinstance(time_stamp_gmt3, datetime):
                # Convertir a la misma zona horaria que ahora_gmt3
                time_stamp_gmt3 = time_stamp_gmt3.astimezone(pytz.timezone("America/Argentina/Buenos_Aires")).replace(microsecond=0)

        else:
            logging.info("No se aplica conversión para serial_alarm_id: %s", row["serial_alarm_id"])
            time_stamp_gmt3 = row["time_stamp"]

            # Obtener la hora actual en GMT-3 directamente
            ahora_gmt3 = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires")).replace(microsecond=0)
        
        ##Calcular DELTA de muestras
        #print(ahora_gmt3,time_stamp_gmt3)
        time_stamp_gmt3 =  time_stamp_gmt3.astimezone(pytz.timezone("America/Argentina/Buenos_Aires")).replace(microsecond=0)
        diferencia_tiempo = ahora_gmt3 - time_stamp_gmt3

        logging.info("Diferencia de tiempo: %s", diferencia_tiempo)

        # Formatear la diferencia de tiempo
        logging.info("Tiempo formateado: %s", str(time_stamp_gmt3))

        # Comparar la diferencia en minutos utilizando timedelta
        if diferencia_tiempo > timedelta(minutes=umbral_minutos):
            print(0)
            logging.info("Resultado: 0")
        else:
            print(1)
            logging.info("Resultado: 1")
    else:
        logging.warning("No se encontraron filas en la tabla.")
        print(0)

def main():
    # Configurar el logging y dir de proyecto
    directorio_proyecto = os.path.dirname(os.path.abspath(__file__))
    os.chdir(directorio_proyecto)
    
    config = cargar_configuracion()
    logfile = config["LOG"]["path"]
  
 
    logging.basicConfig(filename=logfile,format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    umbral_minutos = int(config["UMBRAL"]["umbral_minutos"])


    # Obtener conexión a la base de datos
    try:
        conexion = obtener_conexion()
        cursor = conexion.cursor(dictionary=True)

        # Verificar el tiempo en la última fila
        verificar_tiempo_fila(cursor, umbral_minutos)

    except Exception as e:
        print(0)
        logging.error("Error en la aplicación: %s", str(e))

    finally:
        # Cerrar la conexión
        if 'conexion' in locals() and conexion.is_connected():
            cursor.close()
            conexion.close()

if __name__ == "__main__":
    main()

