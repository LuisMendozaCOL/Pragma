import csv
import pandas as pd
import os
from sqlalchemy import create_engine
from Database import crear_base_de_datos

# Función para imprimir estadísticas
def mostrar_stadisticas(estadisticas: dict):
    """Imprime en pantalla las estadisticas actuales de los registros que han sido insertados en la base de datos.
    En particular tiene en cuenta los valores mínimo, máximo, promedio y número de registros de la columna precio de la tabla Compras.

    Args:
        estadisticas (dict): diccionario que contiene las estadisticas
    """
    print(
        "Estadísticas: N° de registros: {}, Promedio: {}, Mínimo: {}, Máximo: {}".format(
            estadisticas["registros"],
            estadisticas["promedio"],
            estadisticas["minimo"],
            estadisticas["maximo"],
        )
    )


def extraer_datos_de_csv(archivo_csv):
    """Permite extraer la información contenida dentro del archivo csv pasado como argumento.
    Luego, da formato de float a la columna price y int a la columna user_id del archivo csv.
    En caso de encontrar un valor vacio en la columna price, la función asigna None a dicho valor.

    Args:
        archivo_csv (.csv): archivo csv que contiene la información

    Returns:
        fechas (list): lista que contiene las fechas encontradas en el archivo csv
        precios (list): lista que contiene los precios encontradas en el archivo csv
        ids(list): lista que contiene los encontrados en el archivo csv
    """
    # Listas para almacenar los datos
    fechas = []
    precios = []
    ids = []
    # Función para convertir el precio a entero si no está vacío
    f_casting_precios = lambda x: int(x) if len(x) > 0 else None
    # Ruta del archivo CSV
    ruta = "Archivos/" + archivo_csv
    # Abrimos el archivo CSV
    with open(ruta, "r") as archivo:
        # Leemos el archivo con un lector de CSV
        lector = csv.reader(archivo, delimiter=",")
        # Ignoramos la cabecera
        next(lector)
        # Recorremos las líneas del archivo
        for linea in lector:
            # Añadimos fecha
            fechas.append(linea[0])
            # Convertimos el precio a entero si no está vacío
            precios.append(f_casting_precios(linea[1]))
            # Convertimos el id a entero
            ids.append(int(linea[2]))
    # Devolvemos los datos extraídos
    return fechas, precios, ids


def enviar_a_sql(engine, df, batch_size, estadisticas):
    """Toma un Dataframe de pandas (df) y lo envía a la base de datos pasada como argumento en la variable engine,
    en baches del tamaño batch_size y guarda el promedio, mínimo, máximo de la columna precios en la variable estadisticas.

    Args:
        engine (sqlalchemy.engine.base.Engine): controlador de base de datos
        df (pd.Dataframe): Dataframe con el conjunto de información organizada
        batch_size (int): Tamaño del bache de datos a ingestar
        estadisticas (dict): diccionario que contiene las estadisticas
    """

    # Función para calcular el promedio acumulado
    calc_cum_promedio = lambda x: x["suma_acumulada"] / x["registros_no_nulos"]
    # Función para calcular el mínimo acumulado
    calc_cum_min = (
        lambda anterior_min, min_: min_ if min_ < anterior_min else anterior_min
    )
    # Función para calcular el máximo acumulado
    calc_cum_max = (
        lambda anterior_max, max_: max_ if max_ > anterior_max else anterior_max
    )

    # Iterar sobre el dataframe por lotes (batches)
    batch_count = 1
    for i in range(0, len(df), batch_size):
        # Extraer el lote de datos
        batch = df[i : i + batch_size]
        # Escribir el lote en la base de datos SQL
        try:
            batch.to_sql("compras", con=engine, if_exists="append", index=False)
            print("Cargue exitoso del lote Nº", batch_count)
            batch_count += 1

            # Actualizar estadisticas
            estadisticas["registros"] += batch.fecha.count()
            estadisticas["suma_acumulada"] += batch.precio.sum()
            estadisticas["registros_no_nulos"] += batch.precio.count()
            estadisticas["promedio"] = calc_cum_promedio(estadisticas)
            estadisticas["minimo"] = calc_cum_min(
                estadisticas["minimo"], batch.precio.min()
            )
            estadisticas["maximo"] = calc_cum_max(
                estadisticas["maximo"], batch.precio.max()
            )
            # Imprimir estadisticas
            mostrar_stadisticas(estadisticas)

        except Exception as e:
            print("Error al guardar la información en la base de datos:")
            print(e.args)


if __name__ == "__main__":
    # Creamos una instancia de motor de base de datos llamada "engine" para conectarnos a la base de datos "Pragma.db".
    engine = create_engine("sqlite:///Pragma.db")
    crear_base_de_datos(engine)

    lista_de_archivos = os.listdir("Archivos/")
    archivo_de_validacion = lista_de_archivos.pop()

    estadisticas = {
        "registros_no_nulos": 0,
        "suma_acumulada": 0,
        "registros": 0,
        "promedio": None,
        "minimo": 100,
        "maximo": 0,
    }

    for archivo_csv in lista_de_archivos:
        print("Cargando información de archivo", archivo_csv)
        fechas, precios, ids = extraer_datos_de_csv(archivo_csv)

        # Cargar CSV formateado a un dataframe de pandas
        diccionario = dict(fecha=fechas, precio=precios, id_usuario=ids)
        df = pd.DataFrame(diccionario)

        # Ajustar el tamaño del lote (batch size)
        batch_size = 5
        enviar_a_sql(engine, df, batch_size, estadisticas)
        # Imprime un salto de línea
        print("\n")
