import pandas as pd
from datetime import datetime
import numpy as np


# PA i DO
def valores_codigos(data: pd.DataFrame, lista: list, nombre_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        for ingreso in fila['ingressos']:  # Iterar sobre cada ingreso en 'ingressos'
            for valor in ingreso['codiDiagnostics']:  # Iterar sobre cada valor en 'codiDiagnostics'
                if valor in lista:  # Verificar si el valor está en la lista de valores a buscar
                    data.at[indice, nombre_columna] = 1  # Asignar 1 si el valor está presente
                    break  # Romper el bucle si se encuentra el valor para esta fila
                else:
                    data.at[indice, nombre_columna] = 0  # Asignar 0 si el valor no está presente
            if data.at[indice, nombre_columna] == 1:  # Verificar si el valor está en la lista de valores a buscar
                break  # Romper el bucle si se encuentra el valor para esta fila
    return data


# Cantidad de ingresos
def contar_diccionarios(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():
        num_diccionarios = len(fila[nombre_columna]) if isinstance(fila[nombre_columna], list) else 0
        data.at[indice, 'Num_ingresos'] = num_diccionarios
    return data


# Sumatorio dias ingresados
def dias_ingreso_total(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        suma_dias = 0  # Inicializar la suma de días de ingreso para esta fila
        for ingreso in fila[nombre_columna]:  # Iterar sobre cada ingreso en la columna de interés
            fecha_ingreso = datetime.strptime(ingreso['dataIngres'], '%Y-%m-%d')
            fecha_alta = datetime.strptime(ingreso['dataAlta'], '%Y-%m-%d')
            diferencia = fecha_alta - fecha_ingreso
            suma_dias += diferencia.days  # Sumar los días de ingreso de este ingreso a la suma total
        data.at[indice, 'Dias_totales_ingresado'] = suma_dias  # Asignar la suma de días de ingreso a la nueva columna
    return data


# Rango de edad
def asignar_intervalo_edad(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        edad = fila[nombre_columna]  # Obtener la edad del paciente de la columna de interés
        if edad < 21:
            intervalo = 'Menor de 21'
        elif edad < 31:
            intervalo = '21-30'
        elif edad < 41:
            intervalo = '31-40'
        elif edad < 51:
            intervalo = '41-50'
        elif edad < 61:
            intervalo = '51-60'
        elif edad < 71:
            intervalo = '61-70'
        elif edad < 81:
            intervalo = '71-80'
        elif edad < 91:
            intervalo = '81-90'
        else:
            intervalo = '91 o más'
        data.at[indice, 'Intervalo_edad'] = intervalo  # Asignar el intervalo de edad a la nueva columna
    return data


# Valores barthel
def sumar_barthel(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:  # Esto define la función sumar_barthel que
    # toma un DataFrame de Pandas (data), el nombre de la columna de interés (nombre_columna) y devuelve un DataFrame modificado.
    def suma_sin_fecha(
            diccionario):  # Esto define una función interna llamada suma_sin_fecha que toma un diccionario como entrada.
        suma_parcial = 0  # Inicializa una variable llamada suma_parcial que almacenará la suma de los valores, comenzando en 0.
        for clave, valor in diccionario.items():  # Itera sobre cada par clave-valor en el diccionario.
            if clave != 'data':  # Verifica si la clave actual es diferente de 'fecha'
                suma_parcial += int(
                    valor)  # Si la clave no es 'fecha', suma el valor correspondiente al total, convirtiéndolo primero a entero.
        return suma_parcial  # Devuelve la suma parcial de los valores, excluyendo la fecha.

    # Aplicar la función a la columna 'barthel' para obtener la suma de los valores, excluyendo la clave 'data'
    data['Barthel_resultados'] = data[nombre_columna].apply(
        suma_sin_fecha)  # Aplica la función suma_sin_fecha a cada elemento
    # de la columna 'barthel' en el DataFrame, almacenando el resultado en una nueva columna llamada 'Suma total Barthel'.
    return data  # Devuelve el DataFrame modificado con la nueva columna de suma total de los valores de 'barthel', excluyendo la fecha


# Emina (antiguo EMINA sumatorio resultados)
def sumar_emina(data: pd.DataFrame,
                nombre_columna: str) -> pd.DataFrame:  # Esto define una función llamada sumar_barthel
    # que toma un DataFrame data, el nombre de una columna nombre_columna y devuelve un DataFrame modificado.

    # Aplicar la función a la columna 'emina' para obtener la suma de los valores, excluyendo las últimas claves
    data['EMINA_sumatorios_comparados'] = data[nombre_columna].apply(
        suma_sin_ultimas_claves)  # Aplica la función suma_sin_ultimas_claves
    # a la columna especificada del DataFrame data y asigna los resultados a una nueva columna llamada 'Suma total Barthel'.

    return data  # Devuelve el DataFrame modificado con la nueva columna agregada


def suma_sin_ultimas_claves(diccionarios):
    if not diccionarios:  # Verifica si la lista de diccionarios está vacía
        return None  # Devuelve None si la lista de diccionarios está vacía. None detiene la ejecución de la función
        # y no continuará con el bucle ni con el resto del código

    suma_parcial = 0  # Inicializa el sumatorio parcial

    for diccionario in diccionarios:  # Itera sobre cada diccionario en la lista de diccionarios
        if diccionario:  # Verifica si el diccionario está vacío
            for clave, valor in diccionario.items():  # Itera sobre cada par clave-valor en el diccionario
                if clave not in ['dataValoracio',
                                 'resultat']:  # Verifica si la clave no es 'dataValoracio' ni 'resultat'
                    if valor.replace('.', '', 1).isdigit():  # Verifica si el valor es un número
                        suma_parcial += float(valor)  # Suma el valor al sumatorio parcial

            if 'resultat' in diccionario:  # Verifica si 'resultat' está en el diccionario
                if suma_parcial == float(diccionario['resultat']):  # Verifica si el sumatorio coincide con 'resultat'
                    return suma_parcial  # Devuelve el sumatorio parcial
            continue

        return None  # Devuelve None si el diccionario está vacío

    return None  # Devuelve None si el sumatorio no coincide con 'resultat' o si 'resultat' no está presente en el último diccionario


# Emina funcion alternativa (coge solo la ultima clave 'resultats' teniendo en cuenta que none != 0,
# ademas usada tmb en mna, al haber agregado nueva_columna: str y convertir la funcion en estatica)
def obtener_ultimo_resultat(data: pd.DataFrame, nombre_columna: str, nueva_columna: str) -> pd.DataFrame:
    """
    Función para obtener el último valor de la clave 'resultat' en una columna de tipo lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el último valor de la clave 'resultat'.
    """

    # Aplicar la función obtener_ultimo a la columna especificada del DataFrame
    data[nueva_columna] = data[nombre_columna].apply(obtener_ultimo)

    return data  # Devolver el DataFrame modificado


@staticmethod  # Para crear funciones estaticas
def obtener_ultimo(diccionarios):
    """
    Función interna para obtener el último valor de la clave 'resultat' en la lista de diccionarios.

    Parámetros:
        - diccionarios: Lista de diccionarios.

    Devuelve:
        - Último valor de la clave 'resultat' o None si no está presente.
    """
    if diccionarios:  # Verificar si la lista de diccionarios no está vacía
        ultimo_diccionario = diccionarios[-1]  # Obtener el último diccionario de la lista
        if 'resultat' in ultimo_diccionario:  # Verificar si la clave 'resultat' está presente en el último diccionario
            return ultimo_diccionario['resultat']  # Devolver el valor de la clave 'resultat'
    return None  # Devolver None si la lista de diccionarios está vacía o si 'resultat' no está presente en el último diccionario


# pes
def obtener_valor_promedio(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    """
    Función para calcular el promedio de los valores de la clave 'valor' en los diccionarios de una lista.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene las listas de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el promedio de los valores.
    """
    # Aplica el método estático calcular_promedio a la columna especificada del DataFrame
    data['promedio_pes'] = data[nombre_columna].apply(calcular_promedio)

    return data


@staticmethod
def calcular_promedio(diccionarios):
    """
    Método estático para calcular el promedio de los valores de la clave 'valor' en los diccionarios de una lista.

    Parámetros:
        - diccionarios: Lista de diccionarios.

    Devuelve:
        - Promedio de los valores de la clave 'valor'.
    """
    if not diccionarios:
        return None  # Devuelve None si la lista de diccionarios está vacía

    valores = []  # Lista para almacenar los valores de la clave 'valor'
    for d in diccionarios:
        if isinstance(d, dict) and 'valor' in d and d['valor'].strip():
            valores.append(float(d['valor']))  # Añade el valor de 'valor' como un número flotante a la lista

    if valores:
        return np.nanmean(valores)  # Calcula el promedio de los valores y lo devuelve
    else:
        return None  # Devuelve None si no hay valores válidos
