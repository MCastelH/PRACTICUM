import pandas as pd
from datetime import datetime, timedelta


# TODO: comprueba que los nombres de las funciones sean descriptivos y concisos.
# TODO: Agrega una descripción de la funcionalidad de las funciones.
# TODO: Comprueba si se pueden simplificar las funciones y si se pueden reutilizar partes de código.

# Funció per obtenir les columnes de les malalties segons els seus ICD
def codis_ICD(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    for index, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        for ingres in fila['ingressos']:  # Iterar sobre cada ingrés en 'ingressos'
            for valor in ingres['codiDiagnostics']:  # Iterar sobre cada valor en 'codiDiagnostics'
                if valor in llista:  # Verificar si el valor està a la llista de codis a buscar
                    data.at[index, nova_columna] = 1  # Assignar 1 si el valor està present
                    break  # Aturar el bucle si es troba el valor per aquesta fila
                else:
                    data.at[index, nova_columna] = 0  # Assignar 0 si el valor no està present
            if data.at[index, nova_columna] == 1:  # Verificar si el valor està en la llista de codis a buscar
                break  # Aturar el bucle si es troba el valor per aquesta fila
    return data


# Funció per calcular el nombre d'ingressos que ha tingut cada pacient
def nombre_ingressos(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for index, fila in data.iterrows():
        num_diccionaris = len(fila[nom_columna]) if isinstance(fila[nom_columna], list) else 0
        data.at[index, 'Num_ingresos'] = num_diccionaris
    return data


# Funció que fa un sumatori de tots els dies en total que el pacient ha estat ingressat
def dies_ingressat_total(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for index, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        suma_dies = 0  # Inicialitzar la suma de dies d'ingrés per aquesta fila
        for ingres in fila[nom_columna]:  # Iterar sobre cada ingrés en la columna d'interès
            data_ingres = datetime.strptime(ingres['dataIngres'], '%Y-%m-%d')
            data_alta = datetime.strptime(ingres['dataAlta'], '%Y-%m-%d')
            diferencia = data_alta - data_ingres
            suma_dies += diferencia.days  # Sumar els dies d'ingrés d'aquest ingrés a la suma total de dies en total
            # que el pacient ha estat ingressat
        data.at[index, 'Dias_totales_ingresado'] = suma_dies  # Assignar la suma de dies d'ingrés a la nova columna
    return data


# Funció per classificar en un intèrval de 10 anys l'edat dels pacients
def interval_10_edat(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    for index, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        edat = fila[nom_columna]  # Obtenir l'edat del pacient de la columna d'interès
        if edat < 21:
            interval = 'Menor de 21'
        elif edat < 31:
            interval = '21-30'
        elif edat < 41:
            interval = '31-40'
        elif edat < 51:
            interval = '41-50'
        elif edat < 61:
            interval = '51-60'
        elif edat < 71:
            interval = '61-70'
        elif edat < 81:
            interval = '71-80'
        elif edat < 91:
            interval = '81-90'
        else:
            interval = '91 o más'
        data.at[index, 'Interval_edat'] = interval  # Assignar l'intèrval d'edat a la nova columna
    return data


# Funció que retorna la data quan està present algun codi de la llista introduïda. Si el codi es repeteix, retorna
# la data més antiga
def obtenir_data_presencia_codi(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Troba la data més antiga ('dataIngres') associada a un codi de diagnòstic de la llista 'lista' a la llista de
    diccionaris 'ingressos' de cada fila del DataFrame 'data' i crea una nova columna amb els resultats.

    Paràmetres:
     -data: DataFrame que conté les dades.
    -llista: Llista de codis de diagnòstic a cercar.
    -nova_columna: Nom de la nova columna on s'emmagatzemaran les dates més antigues per codi.

    Retorna:
    DataFrame modificat amb la nova columna que conté les dates més antigues per codi.
    """
    # Aplicar la funció 'trobar_data_mes_antiga' a cada fila del DataFrame 'data'
    data[nova_columna] = data['ingressos'].apply(lambda x: trobar_data_mes_antiga(x, llista))

    return data


def trobar_data_mes_antiga(ingressos, llista):
    codi_data_mes_antiga = {}

    for ingres in ingressos:
        codis_diagnostics = ingres.get('codiDiagnostics', [])
        data_ingres = ingres.get('dataIngres', '')

        for codi in codis_diagnostics:
            if codi in llista:
                if codi not in codi_data_mes_antiga or data_ingres < codi_data_mes_antiga[codi]:
                    codi_data_mes_antiga[codi] = data_ingres

    # Obtenir només les dates més antigues per codi
    dates_mes_antigues = {codi: data for codi, data in codi_data_mes_antiga.items()}

    # Encontrar la fecha más antigua de todas las fechas encontradas
    data_mes_antiga = min(dates_mes_antigues.values()) if dates_mes_antigues else None

    return data_mes_antiga


# Funció que resta dues columnes que contenen dates
def restar_dates(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Resta dues columnes que contenen dates en format %Y-%m-%d i guarda el resultat en una nova columna.

    Paràmetres:
    -data: DataFrame que conté les dades.
    -columna1: Nom de la primera columna amb dates.
    -columna2: Nom de la segona columna amb dates.
    -nova_columna: Nom de la nova columna on s'emmagatzemaran els resultats de la resta.

    Retorna:
    DataFrame modificat amb la nova columna que conté la diferència entre les dates.
    """
    # Convertir les columnes de dates al tipus datetime
    data[columna1] = pd.to_datetime(data[columna1])
    data[columna2] = pd.to_datetime(data[columna2])

    # Restar las fechas y calcular la diferencia en valores absolutos
    data[nova_columna] = (data[columna1] - data[columna2]).dt.days.abs()

    return data


# Funció per realitzar la suma de resultats de la columna 'barthel' sense agafar el valor de la clau 'data', comparar-ho
# amb la clau 'resultat' i si el sumatori es el mateix que el valor que hi ha en 'resultat', retorna el sumatori
# realitzat
def sumar_barthel(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:  # Aquesta funció defineix la funció
    # sumar_barthel que pren un DataFrame de Pandas (data), el nom de la columna d'interès (nom_columna) i retorna un
    # DataFrame modificat.

    # Aplicar la funció a la columna 'barthel' per obtenir la suma dels valors, però excloent la clau 'data'
    data['Barthel_resultats'] = data[nom_columna].apply(suma_sense_data)  # Aplica la funció suma_sense_data a cada
    # element de la columna 'barthel' en el DataFrame, emmagatzemant el resultat en una nova columna anomenada
    # 'Barthel_resultats'.
    return data  # Retorna el DataFrame modificat amb la nova columna de suma total dels valors de 'barthel', però
    # excloent els valors que hi hagi a la clau 'data'


def suma_sense_data(diccionari):  # Això defineix una funció interna anomenada suma_sense_data que pren un diccionari
    #com a entrada
    suma_parcial = 0  # Inicialitza una variable anomenada suma_parcial que emmagatzemarà la suma dels valors, començant
    # en 0.
    for clau, valor in diccionari.items():  # Itera sobre cada parell clau-valor en el diccionari.
        if clau != 'data':  # Verifica si la clau actual és diferent de 'data'
            suma_parcial += int(
                valor)  # Si la clau no és 'data', suma el valor corresponent al total, però convertint-lo primer a
            # enter.
    return suma_parcial  # Retorna la suma parcial dels valors, excloent la data.


# Funció semblant a l'anterior però pel test EMINA. En aquest cas no té en compte els valors de les claus
# 'dataValoracio' ni 'resultat'
def sumar_emina(data: pd.DataFrame, nom_columna: str,
                nova_columna: str) -> pd.DataFrame:  # Aquesta funció defineix la funció sumar_emina que pren un
    # DataFrame de Pandas (data), el nom de la columna d'interès (nom_columna) i retorna un DataFrame modificat amb una
    # nova columna (nova_columna).

    # Aplicar la funció a la columna 'emina' per obtenir la suma dels valors, excloent les últimes claus
    data[nova_columna] = data[nom_columna].apply(
        suma_sense_ultimes_claus)  # Aplica la funció suma_sense_ultimes_claus
    # a la columna especificada del DataFrame data i assigna els resultados a una nova columna anomenada 'EMINA
    # sumatoris comparats'.

    return data  # Retorna el DataFrame modificat amb la nova columna afegida


def suma_sense_ultimes_claus(diccionaris):
    if not diccionaris:  # Verifica si la llista de diccionaris està buida
        return None  # Retorna None si la llista de diccionaris està buida. None atura l'execució de la funció i no
        # continuarà amb el bucle ni amb la resta del codi

    suma_parcial = 0  # Inicialitza el sumatori parcial

    for diccionari in diccionaris:  # Itera sobre cada diccionari en la llista de diccionaris
        if diccionari:  # Verifica si el diccionari està buit
            for clau, valor in diccionari.items():  # Itera sobre cada parell clau-valor en el diccionari
                if clau not in ['dataValoracio',
                                'resultat']:  # Verifica si la clau no és 'dataValoracio' ni 'resultat'
                    if valor.replace('.', '', 1).isdigit():  # Verifica si el valor és un nombre
                        suma_parcial += float(valor)  # Suma el valor al sumatori parcial

            if 'resultat' in diccionari:  # Verifica si 'resultat' està en el diccionari
                if suma_parcial == float(diccionari['resultat']):  # Verifica si el sumatori coincideix amb 'resultat'
                    return suma_parcial  # Retorna el sumatori parcial
            continue

        return None  # Retorna None si el diccionari està buit

    return None  # Retorna None si el sumatori no coincideix amb 'resultat' o si 'resultat' no està present a l'últim
    # diccionari


# Funció alternativa per obtenir el resultat del test EMINA sense fer sumatori i comparar-ho (simplement tria la clau
# 'resultats').
def obtenir_ultima_clau(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per obtenir l'últim valor de la clau 'resultat' en una columna de tipus llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.

    Devuelve:
        - DataFrame modificat amb una nova columna que conté l'últim valor de la clau 'resultat'.
    """

    # Aplicar la funció obtenir_ultima_clau a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(obtenir_ultim_diccionari)

    return data  # Retorna el DataFrame modificat


# TODO: el nombre de las funciones debe proporcionar el significado de lo que hacen, modificalo.
def obtenir_ultim_diccionari(diccionaris):
    """
    Funció interna para obtenir l'últim valor de la clau 'resultat' en la llista de diccionaris.

    Paràmetres:
        - diccionaris: Llista de diccionaris.

    Retorna:
        - Últim valor de la clau 'resultat' o None si aquest no està present.
    """
    if diccionaris:  # Verificar si la llista de diccionaris no està buida
        ultim_diccionari = diccionaris[-1]  # Obtenir l'últim diccionari de la llista
        if 'resultat' in ultim_diccionari:  # Verificar si la clau 'resultat' està present en l'últim diccionari
            return ultim_diccionari['resultat']  # Retornar el valor de la clau 'resultat'
    return None  # Retornar None si la llista de diccionaris està buida o si 'resultat' no està present en l'últim diccionari


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
# fecha debe coincidir con la fecha que hay en 'data primer mecvv', con un interval de una semana de margen
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
        mecvv_date_str = row['Data primer MECVV']
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
