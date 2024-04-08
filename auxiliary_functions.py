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

    # Aplicar la función a la columna 'barthel' para obtener la suma de los valores, excluyendo la clave 'data'
    data['Barthel_resultados'] = data[nombre_columna].apply(
        suma_sin_fecha)  # Aplica la función suma_sin_fecha a cada elemento
    # de la columna 'barthel' en el DataFrame, almacenando el resultado en una nueva columna llamada 'Suma total Barthel'.
    return data  # Devuelve el DataFrame modificado con la nueva columna de suma total de los valores de 'barthel', excluyendo la fecha

def suma_sin_fecha(diccionario):  # Esto define una función interna llamada suma_sin_fecha que toma un diccionario como entrada.
    suma_parcial = 0  # Inicializa una variable llamada suma_parcial que almacenará la suma de los valores, comenzando en 0.
    for clave, valor in diccionario.items():  # Itera sobre cada par clave-valor en el diccionario.
        if clave != 'data':  # Verifica si la clave actual es diferente de 'fecha'
            suma_parcial += int(
                valor)  # Si la clave no es 'fecha', suma el valor correspondiente al total, convirtiéndolo primero a entero.
    return suma_parcial  # Devuelve la suma parcial de los valores, excluyendo la fecha.


# EMINA sumatorios comparados
def sumar_emina(data: pd.DataFrame, nombre_columna: str,
                nueva_columna: str) -> pd.DataFrame:  # Esto define una función llamada sumar_barthel
    # que toma un DataFrame data, el nombre de una columna nombre_columna y devuelve un DataFrame modificado.

    # Aplicar la función a la columna 'emina' para obtener la suma de los valores, excluyendo las últimas claves
    data[nueva_columna] = data[nombre_columna].apply(
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

def calcular_promedio(lista_diccionarios):
    """
    Calcula el promedio de los valores numéricos de la clave 'valor' en una lista de diccionarios.

    Parámetros:
        - lista_diccionarios: Lista de diccionarios.

    Devuelve:
        - Promedio de los valores numéricos de la clave 'valor' o el valor único si solo hay un diccionario,
          None si la lista está vacía.
    """
    print("Iniciando cálculo del promedio...")

    if not lista_diccionarios:
        print("La lista de diccionarios no es válida o está vacía.")
        return None

    # Filtrar diccionarios vacíos
    diccionarios_no_vacios = [d for d in lista_diccionarios if d]

    if not diccionarios_no_vacios:
        print("La lista de diccionarios no contiene diccionarios válidos.")
        return None

    valores = []
    for diccionario in diccionarios_no_vacios:
        if 'valor' in diccionario:
            valor = diccionario['valor']
            try:
                valor_numerico = float(valor)
                valores.append(valor_numerico)
            except ValueError:
                # Ignorar si el valor no es numérico
                print(f"Valor no válido en el diccionario: {valor}")

    if valores:
        if len(valores) == 1:
            # Si solo hay un valor, devolver ese valor directamente
            print(f"Solo hay un valor en la lista: {valores[0]}")
            return valores[0]
        else:
            # Calcular el promedio de los valores numéricos
            valores_numericos = [v for v in valores if isinstance(v, (int, float))]
            if valores_numericos:
                promedio = sum(valores_numericos) / len(valores_numericos)
                print(f"Promedio calculado: {promedio}")
                return promedio
            else:
                print("No se encontraron valores numéricos válidos para calcular el promedio.")
                return None
    else:
        print("No se encontraron valores numéricos en los diccionarios.")
        return None

def calcular_promedio(lista_diccionarios):
    """
    Calcula el promedio de los valores numéricos de la clave 'valor' en una lista de diccionarios.

    Parámetros:
        - lista_diccionarios: Lista de diccionarios.

    Devuelve:
        - Promedio de los valores numéricos de la clave 'valor' o el valor único si solo hay un diccionario,
          None si la lista está vacía o no contiene valores válidos.
    """
    print("Iniciando cálculo del promedio...")

    if not lista_diccionarios or not any(lista_diccionarios):
        print("La lista de diccionarios no es válida o está vacía.")
        return None

    valores = []
    for diccionario in lista_diccionarios:
        if diccionario and 'valor' in diccionario:
            valor = diccionario['valor']
            try:
                valor_numerico = float(valor)
                valores.append(valor_numerico)
            except ValueError:
                # Ignorar si el valor no es numérico
                print(f"Valor no válido en el diccionario: {valor}")

    if not valores:
        print("No se encontraron valores numéricos en los diccionarios.")
        return None

    if len(valores) == 1:
        # Si solo hay un valor, devolver ese valor directamente
        print(f"Solo hay un valor en la lista: {valores[0]}")
        return valores[0]

    # Calcular el promedio de los valores numéricos
    valores_numericos = [v for v in valores if isinstance(v, (int, float))]
    if valores_numericos:
        promedio = sum(valores_numericos) / len(valores_numericos)
        print(f"Promedio calculado: {promedio}")
        return promedio
    else:
        print("No se encontraron valores numéricos válidos para calcular el promedio.")
        return None



# Canadenca
def canadenca_comparada(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    """
    Función para comparar el sumatorio de ciertas claves con el valor de 'total' en una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el resultado deseado.
    """
    # Aplicar la función calcular_sumatorio_y_comparar a la columna especificada del DataFrame
    data['Canadenca_sumatorios_comparados'] = data[nombre_columna].apply(
        lambda x: calcular_sumatorio_y_comparar(x[0]) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Devolver el DataFrame modificado


def calcular_sumatorio_y_comparar(diccionario):
    """
    Función interna para calcular el sumatorio y compararlo con el valor de 'total' en un diccionario.

    Parámetros:
        - diccionario: Diccionario que contiene los datos.

    Devuelve:
        - Sumatorio calculado si coincide con 'total', None en caso contrario.
    """
    if not isinstance(diccionario, dict):  # Verificar si el diccionario no es válido
        return None

    sumatorio = 0  # Inicializar el sumatorio
    keys_to_exclude = ['total', 'dataValoracio', 'horaValoracio']

    # Calcular el sumatorio de las claves numéricas válidas en el diccionario
    for clave, valor in diccionario.items():
        if clave not in keys_to_exclude and valor.strip():  # Excluir claves no deseadas y valores vacíos
            try:
                # Convertir coma a punto para parsear a float
                valor_float = float(valor.replace(',', '.'))
                sumatorio += valor_float
            except ValueError:
                pass  # Ignorar valores no numéricos

    # Obtener el valor de 'total' del diccionario
    total = diccionario.get('total')

    # Comparar el sumatorio con 'total' y devolver el resultado adecuado
    if total is not None and sumatorio == float(total):
        return sumatorio
    else:
        return None

# TODO: quedan por "hacer codigos" de: labs, atcs.
# TODO: descriptiva, distribuciones i test categoricos. edad, sexo...
# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es lista de diccionarios []
# hacer sumatorio de la lista i dividir entre su longitud
