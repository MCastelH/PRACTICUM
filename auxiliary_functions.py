import pandas as pd
from datetime import datetime


# PA, P i DO
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


def suma_sin_fecha(
        diccionario):  # Esto define una función interna llamada suma_sin_fecha que toma un diccionario como entrada.
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
    Calcula el promedio de los valores numéricos de la clave 'valor' en una lista de diccionarios.

    Parámetros:
        - diccionarios: Lista de diccionarios con claves 'valor' y 'data'.

    Devuelve:
        - Promedio de los valores numéricos de la clave 'valor'.
          Devuelve None si la lista de diccionarios está vacía, no es válida o contiene una lista vacía.
    """
    # Verificar si la entrada no es válida o si es una lista vacía
    if not diccionarios or not isinstance(diccionarios, list) or (len(diccionarios) == 1 and not diccionarios[0]):
        return None

    valores = []  # Lista para almacenar los valores numéricos de 'valor'

    # Iterar sobre cada diccionario en la lista
    for dic in diccionarios:
        if isinstance(dic, dict) and 'valor' in dic:
            valor = dic['valor']
            if isinstance(valor, (int, float)):
                valores.append(valor)
            elif isinstance(valor, str):
                try:
                    valor_float = float(valor)
                    valores.append(valor_float)
                except ValueError:
                    pass  # Ignorar valores no numéricos o conversiones fallidas

    # Calcular el promedio si se encontraron valores numéricos
    if valores:
        promedio = sum(valores) / len(valores)
        return promedio
    else:
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


# Disfagia y disfagia conocida identificada con mecvvs
def disfagia_mecvvs(data: pd.DataFrame, nombre_columna: str) -> pd.DataFrame:
    """
    Función para comparar el valor de 'disfagia' en el último diccionario con 'SI' o 'S' en una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el resultado deseado.
    """
    # Aplicar la función obtener_ultima_disfagia a la columna especificada del DataFrame
    data['Disfagia_mecvvs'] = data[nombre_columna].apply(
        lambda x: obtener_ultima_disfagia(x) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Devolver el DataFrame modificado

def obtener_ultima_disfagia(diccionarios):
    """
    Obtiene el valor de la clave 'disfagia' o 'disfagiaConeguda' del último diccionario válido que contiene esta clave.

    Parámetros:
        - diccionarios: Lista de diccionarios.

    Devuelve:
        - 1 si el valor de 'disfagia' es 'SI' o 'S'.
        - 0 si el valor de 'disfagia' es 'NO' o 'N'.
        - None si la clave 'disfagia' no se encuentra en ningún diccionario válido.
    """
    if not isinstance(diccionarios, list) or not diccionarios:
        return None  # Devolver None si la entrada no es una lista válida o está vacía

    valor_disfagia = None  # Valor por defecto

    # Iterar hacia atrás en la lista de diccionarios
    for dic in reversed(diccionarios):
        if isinstance(dic, dict):
            # Buscar la clave 'disfagia' o 'disfagiaConeguda'
            if 'disfagia' in dic:
                valor_disfagia = dic['disfagia'].strip().upper()
            elif 'disfagiaConeguda' in dic:
                valor_disfagia = dic['disfagiaConeguda'].strip().upper()

            # Evaluar el valor de 'disfagia' normalizado
            if valor_disfagia is not None:
                if valor_disfagia == 'SI' or valor_disfagia == 'S':
                    return 1
                elif valor_disfagia == 'NO' or valor_disfagia == 'N':
                    return 0

    return None  # Devolver None si no se encuentra la clave 'disfagia' en ningún diccionario válido

# MECVVS para eficacia y seguridad
def extraer_valor_clave(data: pd.DataFrame, nombre_columna: str, clave: str, nueva_columna: str) -> pd.DataFrame:
    """
    Función para extraer el valor de una clave específica en el último diccionario de una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.
        - nueva_columna: Nombre de la nueva columna donde se almacenarán los valores extraídos.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene los valores extraídos.
    """
    # Aplicar la función para extraer el valor de la clave a la columna especificada del DataFrame
    data[nueva_columna] = data[nombre_columna].apply(
        lambda x: obtener_valor_clave(x, clave) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Devolver el DataFrame modificado


def obtener_valor_clave(diccionarios, clave):
    """
    Obtiene el valor de una clave específica del último diccionario válido que contiene esta clave.

    Parámetros:
        - diccionarios: Lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.

    Devuelve:
        - Valor de la clave especificada si se encuentra en el último diccionario válido.
        - None si la clave no se encuentra en ningún diccionario válido.
    """
    if not isinstance(diccionarios, list) or not diccionarios:
        return None  # Devolver None si la entrada no es una lista válida o está vacía

    valor = None  # Valor por defecto

    # Iterar hacia atrás en la lista de diccionarios
    for dic in reversed(diccionarios):
        if isinstance(dic, dict):
            if clave in dic:
                valor = dic[clave]
                break  # Salir del bucle si se encuentra la clave en el diccionario

    # Transformar valores 'SI' o 'S' en 1 y 'NO' o 'N' en 0
    if valor is not None:
        if valor.strip().upper() == 'SI' or valor.strip().upper() == 'S':
            return 1
        elif valor.strip().upper() == 'NO' or valor.strip().upper() == 'N':
            return 0


    return valor  # Devolver el valor encontrado o None si la clave no se encontró


# MECVVS para viscolidad i volumen
# Esta funcion se podria usar tambien para extraer los resultados de mna y emina
def extraer_valor_clave_simple(data: pd.DataFrame, nombre_columna: str, clave: str, nueva_columna: str) -> pd.DataFrame:
    """
    Función para extraer el valor de una clave específica en el último diccionario de una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.
        - nueva_columna: Nombre de la nueva columna donde se almacenarán los valores extraídos.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene los valores extraídos.
    """
    # Aplicar la función para extraer el valor de la clave a la columna especificada del DataFrame
    data[nueva_columna] = data[nombre_columna].apply(
        lambda x: obtener_valor(x, clave) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Devolver el DataFrame modificado

def obtener_valor(diccionarios, clave):
    """
    Obtiene el valor de una clave específica del último diccionario válido que contiene esta clave.

    Parámetros:
        - diccionarios: Lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.

    Devuelve:
        - Valor de la clave especificada si se encuentra en el último diccionario válido.
        - None si la clave no se encuentra en ningún diccionario válido.
    """
    if not isinstance(diccionarios, list) or not diccionarios:
        return None  # Devolver None si la entrada no es una lista válida o está vacía

    valor = None  # Valor por defecto

    # Iterar hacia atrás en la lista de diccionarios
    for dic in reversed(diccionarios):
        if isinstance(dic, dict):
            if clave in dic:
                valor = dic[clave]
                break  # Salir del bucle si se encuentra la clave en el diccionario

    return valor  # Devolver el valor encontrado o None si la clave no se encontró

# Valores del lab
def extraer_name_value_to_column(data: pd.DataFrame, nombre_columna: str, nombre_interes: str, nueva_columna: str) -> pd.DataFrame:
    """
    Función para extraer los valores de la clave 'value' de una lista de diccionarios en una columna, filtrando por un nombre de interés.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nombre_columna: Nombre de la columna que contiene la lista de diccionarios.
        - nombre_interes: Nombre de interés para filtrar la extracción de valores.
        - nueva_columna: Nombre para la nueva columna que contendrá los valores extraídos.

    Devuelve:
        - DataFrame con una nueva columna que contiene los valores de 'value' filtrados por el nombre de interés.
    """
    # Crear una lista para almacenar los valores extraídos
    valores_extraidos = []

    # Iterar sobre cada fila de la columna de diccionarios
    for lista_diccionarios in data[nombre_columna]:
        if isinstance(lista_diccionarios, list) and lista_diccionarios:
            valor_extraido = None
            for diccionario in lista_diccionarios:
                # Verificar si el diccionario contiene el nombre de interés
                if 'name' in diccionario and diccionario['name'] == nombre_interes:
                    # Extraer el valor de 'value'
                    valor_extraido = diccionario.get('value')
                    break  # Salir del bucle una vez encontrado el nombre de interés
            # Agregar el valor extraído a la lista de valores
            valores_extraidos.append(valor_extraido)
        else:
            # Si la lista de diccionarios es vacía o no válida, agregar None
            valores_extraidos.append(None)

    # Verificar si la longitud de valores extraídos coincide con la longitud del DataFrame original
    if len(valores_extraidos) != len(data):
        raise ValueError("La longitud de valores extraídos no coincide con la longitud del DataFrame original.")

    # Agregar los valores extraídos como una nueva columna al DataFrame original
    data[nueva_columna] = valores_extraidos

    return data


# TODO: quedan por "hacer codigos" de: labs, atcs.
# TODO: descriptiva, distribuciones i test categoricos. edad, sexo...
# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es lista de diccionarios []
# float num enteros, int para decimales con punto
