import pandas as pd
from datetime import datetime, timedelta

# TODO: comprueba que los nombres de las funciones sean descriptivos y concisos.
# TODO: Agrega una descripción de la funcionalidad de las funciones.
# TODO: Comprueba si se pueden simplificar las funciones y si se pueden reutilizar partes de código.
# PA, P i DO
def codis_ICD(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        for ingreso in fila['ingressos']:  # Iterar sobre cada ingreso en 'ingressos'
            for valor in ingreso['codiDiagnostics']:  # Iterar sobre cada valor en 'codiDiagnostics'
                if valor in llista:  # Verificar si el valor está en la lista de valores a buscar
                    data.at[indice, nova_columna] = 1  # Asignar 1 si el valor está presente
                    break  # Romper el bucle si se encuentra el valor para esta fila
                else:
                    data.at[indice, nova_columna] = 0  # Asignar 0 si el valor no está presente
            if data.at[indice, nova_columna] == 1:  # Verificar si el valor está en la lista de valores a buscar
                break  # Romper el bucle si se encuentra el valor para esta fila
    return data


# Cantidad de ingresos
def nombre_ingressos(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():
        num_diccionarios = len(fila[nom_columna]) if isinstance(fila[nom_columna], list) else 0
        data.at[indice, 'Num_ingresos'] = num_diccionarios
    return data


# Sumatorio dias ingresados
def dies_ingressat_total(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        suma_dias = 0  # Inicializar la suma de días de ingreso para esta fila
        for ingreso in fila[nom_columna]:  # Iterar sobre cada ingreso en la columna de interés
            fecha_ingreso = datetime.strptime(ingreso['dataIngres'], '%Y-%m-%d')
            fecha_alta = datetime.strptime(ingreso['dataAlta'], '%Y-%m-%d')
            diferencia = fecha_alta - fecha_ingreso
            suma_dias += diferencia.days  # Sumar los días de ingreso de este ingreso a la suma total
        data.at[indice, 'Dias_totales_ingresado'] = suma_dias  # Asignar la suma de días de ingreso a la nueva columna
    return data


# Rango de edad
def interval_10_edat(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for indice, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        edad = fila[nom_columna]  # Obtener la edad del paciente de la columna de interés
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
        data.at[indice, 'Interval_edat'] = intervalo  # Asignar el intervalo de edad a la nueva columna
    return data


# Funcion que devuelve la fecha cuando está presente algun codigo de la lista que necesite introducir. Si el codigo se
# repite, devuelve la fecha más antigua
def obtenir_data_presencia_codi(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra la fecha más antigua ('dataIngres') asociada con un código de diagnóstico de la lista 'lista'
    en la lista de diccionarios 'ingressos' de cada fila del DataFrame 'data' y crea una nueva columna con los resultados.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - lista: Lista de códigos de diagnóstico a buscar.
        - nova_columna: Nombre de la nueva columna donde se almacenarán las fechas más antiguas por código.

    Devuelve:
        - DataFrame modificado con la nueva columna que contiene las fechas más antiguas por código.
    """
    # Aplicar la función 'encontrar_fecha_mas_antigua' a cada fila del DataFrame 'data'
    data[nova_columna] = data['ingressos'].apply(lambda x: encontrar_fecha_mas_antigua(x, llista))

    return data

def encontrar_fecha_mas_antigua(ingressos, llista):
    codigo_fecha_mas_antigua = {}

    for ingreso in ingressos:
        codigos_diagnosticos = ingreso.get('codiDiagnostics', [])
        fecha_ingreso = ingreso.get('dataIngres', '')

        for codigo in codigos_diagnosticos:
            if codigo in llista:
                if codigo not in codigo_fecha_mas_antigua or fecha_ingreso < codigo_fecha_mas_antigua[codigo]:
                    codigo_fecha_mas_antigua[codigo] = fecha_ingreso

    # Obtener solo las fechas más antiguas por código
    fechas_mas_antiguas = {codigo: fecha for codigo, fecha in codigo_fecha_mas_antigua.items()}

    # Encontrar la fecha más antigua de todas las fechas encontradas
    fecha_mas_antigua = min(fechas_mas_antiguas.values()) if fechas_mas_antiguas else None

    return fecha_mas_antigua


# Función que resta dos columnas que contienen fechas
def restar_dates(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Resta dos columnas que contienen fechas en formato %Y-%m-%d y guarda el resultado en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - columna1: Nombre de la primera columna con fechas.
        - columna2: Nombre de la segunda columna con fechas.
        - nova_columna: Nombre de la nueva columna donde se almacenarán los resultados de la resta.

    Devuelve:
        - DataFrame modificado con la nueva columna que contiene la diferencia entre las fechas.
    """
    # Convertir las columnas de fechas al tipo datetime
    data[columna1] = pd.to_datetime(data[columna1])
    data[columna2] = pd.to_datetime(data[columna2])

    # Restar las fechas y calcular la diferencia en valores absolutos
    data[nova_columna] = (data[columna1] - data[columna2]).dt.days.abs()

    return data


# Valores barthel
def sumar_barthel(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:  # Esto define la función sumar_barthel que
    # toma un DataFrame de Pandas (data), el nombre de la columna de interés (nom_columna) y devuelve un DataFrame
    # modificado.

    # Aplicar la función a la columna 'barthel' para obtener la suma de los valores, excluyendo la clave 'data'
    data['Barthel_resultados'] = data[nom_columna].apply(
        suma_sin_fecha)  # Aplica la función suma_sin_fecha a cada elemento
    # de la columna 'barthel' en el DataFrame, almacenando el resultado en una nueva columna llamada 'Suma total
    # Barthel'.
    return data  # Devuelve el DataFrame modificado con la nueva columna de suma total de los valores de 'barthel',
    # excluyendo la fecha


def suma_sin_fecha(
        diccionario):  # Esto define una función interna llamada suma_sin_fecha que toma un diccionario como entrada.
    suma_parcial = 0  # Inicializa una variable llamada suma_parcial que almacenará la suma de los valores,
    # comenzando en 0.
    for clave, valor in diccionario.items():  # Itera sobre cada par clave-valor en el diccionario.
        if clave != 'data':  # Verifica si la clave actual es diferente de 'fecha'
            suma_parcial += int(
                valor)  # Si la clave no es 'fecha', suma el valor correspondiente al total, convirtiéndolo primero a
            # entero.
    return suma_parcial  # Devuelve la suma parcial de los valores, excluyendo la fecha.


# EMINA sumatorios comparados
def sumar_emina(data: pd.DataFrame, nom_columna: str,
                nova_columna: str) -> pd.DataFrame:  # Esto define una función llamada sumar_barthel
    # que toma un DataFrame data, el nombre de una columna nom_columna y devuelve un DataFrame modificado.

    # Aplicar la función a la columna 'emina' para obtener la suma de los valores, excluyendo las últimas claves
    data[nova_columna] = data[nom_columna].apply(
        suma_sin_ultimas_claves)  # Aplica la función suma_sin_ultimas_claves
    # a la columna especificada del DataFrame data y asigna los resultados a una nueva columna llamada 'Suma total
    # Barthel'.

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

    return None  # Devuelve None si el sumatorio no coincide con 'resultat' o si 'resultat' no está presente en el
    # último diccionario


# Emina funcion alternativa (coge solo la última clave 'resultats' teniendo en cuenta que none != 0,
# además usada tmb en mna, al haber agregado nova_columna: str y convertir la funcion en estatica)
def obtenir_ultim_resultat(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Función para obtener el último valor de la clave 'resultat' en una columna de tipo lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el último valor de la clave 'resultat'.
    """

    # Aplicar la función obtener_ultimo a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(obtener_ultimo)

    return data  # Devolver el DataFrame modificado


# TODO: el nombre de las funciones debe proporcionar el significado de lo que hacen, modificalo.
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
# TODO: elige un idioma y mantenlo consistente en todo el código.
def obtenir_pes_o_mitjana(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Función para calcular el promedio de los valores de la clave 'valor' en los diccionarios de una lista.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene las listas de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el promedio de los valores.
    """
    # Aplica el método estático calcular_promedio a la columna especificada del DataFrame
    data['promedio_pes'] = data[nom_columna].apply(calcular_promedio)

    return data


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
def canadenca_comparada(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Función para comparar el sumatorio de ciertas claves con el valor de 'total' en una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el resultado deseado.
    """
    # Aplicar la función calcular_sumatorio_y_comparar a la columna especificada del DataFrame
    data['Canadenca_sumatorios_comparados'] = data[nom_columna].apply(
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
def disfagia_mecvvs(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Función para comparar el valor de 'disfagia' en el último diccionario con 'SI' o 'S' en una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene el resultado deseado.
    """
    # Aplicar la función obtener_ultima_disfagia a la columna especificada del DataFrame
    data['Disfagia_mecvvs'] = data[nom_columna].apply(
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
def extreure_valors_claus(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Función para extraer el valor de una clave específica en el último diccionario de una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.
        - nova_columna: Nombre de la nueva columna donde se almacenarán los valores extraídos.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene los valores extraídos.
    """
    # Aplicar la función para extraer el valor de la clave a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtener_valor_clave(x, clau) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Devolver el DataFrame modificado


def obtener_valor_clave(diccionarios, clau):
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
            if clau in dic:
                valor = dic[clau]
                break  # Salir del bucle si se encuentra la clave en el diccionario

    # Transformar valores 'SI' o 'S' en 1 y 'NO' o 'N' en 0
    if valor is not None:
        if valor.strip().upper() == 'SI' or valor.strip().upper() == 'S':
            return 1
        elif valor.strip().upper() == 'NO' or valor.strip().upper() == 'N':
            return 0

    return valor  # Devolver el valor encontrado o None si la clave no se encontró


# MECVVS para viscolidad y volumen
# Esta funcion se podria usar también para extraer los resultados de mna y emina
def extreure_valors_claus_simple(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Función para extraer el valor de una clave específica en el último diccionario de una lista de diccionarios.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.
        - clave: Clave cuyo valor se desea extraer de cada diccionario.
        - nova_columna: Nombre de la nueva columna donde se almacenarán los valores extraídos.

    Devuelve:
        - DataFrame modificado con una nueva columna que contiene los valores extraídos.
    """
    # Aplicar la función para extraer el valor de la clave a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtener_valor(x, clau) if isinstance(x, list) and len(x) > 0 else None)

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
def obtenir_valors_clau_interes(data: pd.DataFrame, nom_columna: str, clau_interes: str,
                                 nova_columna: str) -> pd.DataFrame:
    """
    Función para extraer los valores de la clave 'value' de una lista de diccionarios en una columna, filtrando por un nombre de interés.

    Parámetros:
        - data: DataFrame de pandas que contiene los datos.
        - nom_columna: Nombre de la columna que contiene la lista de diccionarios.
        - clau_interes: Nombre de interés para filtrar la extracción de valores.
        - nova_columna: Nombre para la nueva columna que contendrá los valores extraídos.

    Devuelve:
        - DataFrame con una nueva columna que contiene los valores de 'value' filtrados por el nombre de interés.
    """
    # Crear una lista para almacenar los valores extraídos
    valores_extraidos = []

    # Iterar sobre cada fila de la columna de diccionarios
    for llista_diccionaris in data[nom_columna]:
        if isinstance(llista_diccionaris, list) and llista_diccionaris:
            valor_extraido = None
            for diccionario in llista_diccionaris:
                # Verificar si el diccionario contiene el nombre de interés
                if 'name' in diccionario and diccionario['name'] == clau_interes:
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
    data[nova_columna] = valores_extraidos

    return data


# Función para calcular el valor Charlson para un paciente
def index_charlson(data: pd.DataFrame, columna_interes: str, nova_columna: str, charlson_dict: dict) -> pd.DataFrame:
    """
    Calcula el índice de Charlson para cada entrada en la lista de diagnósticos en la columna especificada
    y agrega el resultado a una nueva columna en el DataFrame.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - columna_entrada: Nombre de la columna que contiene la lista de diagnósticos.
        - nova_columna: Nombre de la nueva columna donde se almacenarán los resultados.
        - charlson_dict: Diccionario que mapea los valores de Charlson a listas de códigos de diagnóstico.

    Devuelve:
        - DataFrame modificado con la nueva columna de índice de Charlson.
    """
    data[nova_columna] = 0  # Inicializar la nueva columna con valores predeterminados

    # Recorrer cada fila del DataFrame
    for index, row in data.iterrows():
        charlson_value = 0  # Inicializar el valor de Charlson para esta fila

        # Obtener la lista de diagnósticos de la entrada actual
        diagnosticos_lista = row[columna_interes]

        if diagnosticos_lista is None or not isinstance(diagnosticos_lista, list):
            continue  # Saltar a la siguiente fila si no hay lista de diagnósticos válida

        # Iterar sobre cada diccionario en la lista de diagnósticos
        for diagnostico_dic in diagnosticos_lista:
            # Obtener la lista de códigos de diagnóstico de este diccionario
            codigos_diagnosticos = diagnostico_dic.get('codiDiagnostics', [])

            # Iterar sobre cada código de diagnóstico en la lista
            for codigo_diagnostico in codigos_diagnosticos:
                if isinstance(codigo_diagnostico, str) and codigo_diagnostico:
                    # Buscar el código en el diccionario de Charlson (charlson_dict)
                    for value, codes in charlson_dict.items():
                        if any(codigo_diagnostico.startswith(code) for code in codes):
                            charlson_value += value
                            break  # Salir del bucle una vez que se encuentra la coincidencia

        # Asignar el valor calculado a la nueva columna en el DataFrame
        data.loc[index, nova_columna] = charlson_value

    return data


# Funcion que devuelve el peso más antiguo
def obtenir_pes_mes_antic(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra el peso más antiguo en la lista de diccionarios 'pes' de cada fila y lo guarda en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - nova_columna: Nombre de la nueva columna donde se almacenará el peso más antiguo.

    Devuelve:
        - DataFrame modificado con la nueva columna del peso más antiguo.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        pes_data = row['pes']

        if not pes_data or len(pes_data) == 0:
            continue  # Saltar a la siguiente fila si 'pes_data' está vacío

        # Encontrar el diccionario con la fecha más antigua en la clave 'data'
        try:
            oldest_pes_data = min(pes_data, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            oldest_pes_weight = oldest_pes_data['valor']
            data.loc[index, nova_columna] = oldest_pes_weight
        except ValueError:
            # En caso de error al parsear la fecha, continuar con la siguiente fila
            continue

    return data


# Funcion que devuelve el peso más actual
def obtenir_pes_mes_nou(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra el peso más nuevo en la lista de diccionarios 'pes' de cada fila y lo guarda en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - nova_columna: Nombre de la nueva columna donde se almacenará el peso más nuevo.

    Devuelve:
        - DataFrame modificado con la nueva columna del peso más nuevo.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        pes_data = row['pes']

        if not pes_data or len(pes_data) == 0:
            continue  # Saltar a la siguiente fila si 'pes_data' está vacío

        # Encontrar el diccionario con la fecha más nueva en la clave 'data'
        try:
            newest_pes_data = max(pes_data, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            newest_pes_weight = newest_pes_data['valor']
            data.loc[index, nova_columna] = newest_pes_weight
        except ValueError:
            # En caso de error al parsear la fecha, continuar con la siguiente fila
            continue

    return data


# Funcion que devuelve la fecha del peso más antiguo
def obtenir_data_mes_antiga(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra la fecha más antigua en la lista de diccionarios 'pes' de cada fila y la guarda en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - nova_columna: Nombre de la nueva columna donde se almacenará la fecha más antigua.

    Devuelve:
        - DataFrame modificado con la nueva columna de la fecha más antigua.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        pes_data = row['pes']

        if not pes_data or len(pes_data) == 0:
            continue  # Saltar a la siguiente fila si 'pes_data' está vacío

        # Encontrar el diccionario con la fecha más antigua en la clave 'data'
        try:
            oldest_pes_data = min(pes_data, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            oldest_pes_date = oldest_pes_data['data']
            data.loc[index, nova_columna] = oldest_pes_date
        except ValueError:
            # En caso de error al parsear la fecha, continuar con la siguiente fila
            continue

    return data


# Funcion que devuelve la primera fecha en la que se cumple que hay un test mecvv positivo
def obtenir_primera_data_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra la fecha en la lista de diccionarios 'mecvvs' cuando se cumplen ciertas condiciones y la guarda en una
    nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - nova_columna: Nombre de la nueva columna donde se almacenará la fecha.

    Devuelve:
        - DataFrame modificado con la nueva columna de la fecha que cumple las condiciones.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        mecvvs_data = row['mecvvs']

        if not mecvvs_data or len(mecvvs_data) == 0:
            continue  # Saltar a la siguiente fila si 'mecvvs_data' está vacío

        # Buscar la primera fecha que cumpla las condiciones
        for mec_data in mecvvs_data:
            if ('disfagia' in mec_data and mec_data['disfagia'] in ['SI', 'S']) or \
                    ('disfagiaConeguda' in mec_data and mec_data['disfagiaConeguda'] in ['SI', 'S']):
                if ('alteracioSeguretat' in mec_data and mec_data['alteracioSeguretat'] in ['SI', 'S']) or \
                        ('alteracioEficacia' in mec_data and mec_data['alteracioEficacia'] in ['SI', 'S']):
                    fecha_primera_condicion = datetime.strptime(mec_data['data'][:8], '%Y%m%d').strftime('%Y-%m-%d')
                    data.loc[index, nova_columna] = fecha_primera_condicion
                    break  # Detener la búsqueda una vez que se encuentra la fecha

    return data


# Funcion que devuelve el peso, teniendo en cuenta la fecha del primer mecvv positivo. Para que devuelva el peso, su
# fecha debe coincidir con la fecha que hay en 'data primer mecvv', con un intervalo de una semana de margen
def obtenir_pes_coincident_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Encuentra el peso en la lista de diccionarios 'pes' que coincide con la fecha de 'data primer mecvv' dentro de un rango de ±7 días
    y guarda el peso correspondiente en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - nova_columna: Nombre de la nueva columna donde se almacenará el peso encontrado.

    Devuelve:
        - DataFrame modificado con la nueva columna del peso correspondiente al rango de fecha.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        mecvv_date_str = row['data primer MECVV']
        pes_data = row['pes']

        if not mecvv_date_str or not pes_data or len(pes_data) == 0:
            continue  # Saltar a la siguiente fila si no hay fecha en 'data_primer_mecvv' o 'pes_data' está vacío

        try:
            # Convertir la fecha de 'data_primer_mecvv' al formato datetime
            mecvv_date = datetime.strptime(mecvv_date_str, '%Y-%m-%d')

            # Definir los límites del rango de fechas (±3 días)
            date_start = mecvv_date - timedelta(days=3)
            date_end = mecvv_date + timedelta(days=3)

            # Buscar el peso en 'pes_data' que corresponde al rango de fechas
            for pes_entry in pes_data:
                pes_date_str = pes_entry.get('data')
                if pes_date_str:
                    pes_date = datetime.strptime(pes_date_str, '%Y-%m-%d')
                    if date_start <= pes_date <= date_end:
                        data.loc[index, nova_columna] = pes_entry.get('valor')
                        break  # Detener la búsqueda después de encontrar la primera coincidencia
        except ValueError:
            # En caso de error al parsear la fecha, continuar con la siguiente fila
            continue

    return data


# Función para obtener la resta de 2 columnas que contienen valores tipo object
def restar_columnes(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Resta los valores de dos columnas en un DataFrame y almacena el resultado en una nueva columna.

    Parámetros:
        - data: DataFrame que contiene los datos.
        - columna1: Nombre de la primera columna para restar.
        - columna2: Nombre de la segunda columna para restar.
        - nova_columna: Nombre de la nueva columna donde se almacenará el resultado de la resta.

    Devuelve:
        - DataFrame modificado con la nueva columna del resultado de la resta.
    """
    # Inicializar la nueva columna con None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        valor1 = row[columna1]
        valor2 = row[columna2]

        try:
            # Intentar convertir los valores a tipos numéricos (flotantes o enteros)
            valor1 = float(valor1)
            valor2 = float(valor2)

            # Realizar la resta y almacenar el resultado en la nueva columna
            data.loc[index, nova_columna] = valor1 - valor2
        except (ValueError, TypeError):
            # En caso de error al convertir o restar, almacenar None en la nueva columna
            data.loc[index, nova_columna] = None

    return data

# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es lista de diccionarios []
# float num enteros, int para decimales con punto
