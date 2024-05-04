import pandas as pd
from datetime import datetime, timedelta


# TODO: comprueba que los Noms de las funciones sean descriptivos y concisos.
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
        data.at[index, 'Nombre ingressos'] = num_diccionaris
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
        data.at[index, 'Dies totals ingressat'] = suma_dies  # Assignar la suma de dies d'ingrés a la nova columna
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
        data.at[index, 'Interval edat'] = interval  # Assignar l'intèrval d'edat a la nova columna
    return data


# Funció que retorna la data quan està present algun codi de la llista introduïda. Si el codi es repeteix, retorna
# la data més antiga
def obtenir_data_presencia_codi(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Troba la data més antiga ('dataIngres') associada a un codi de diagnòstic de la llista 'llista' a la llista de
    diccionaris 'ingressos' de cada fila del DataFrame 'data' i crea una nova columna amb els resultats.

    Paràmetres:
     -data: DataFrame que conté les dades.
    -llista: llista de codis de diagnòstic a cercar.
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

    # Encontrar la data más antigua de todas las datas encontradas
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

    # Restar las datas y calcular la diferencia en valores absolutos
    data[nova_columna] = (data[columna1] - data[columna2]).dt.days.abs()

    return data


# Funció per realitzar la suma de resultats de la columna 'barthel' sense agafar el valor de la clau 'data', comparar-ho
# amb la clau 'resultat' i si el sumatori és el mateix que el valor que hi ha en 'resultat', retorna el sumatori
# realitzat
def sumar_barthel(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:  # Aquesta funció defineix la funció
    # sumar_barthel que pren un DataFrame de pandes (data), el nom de la columna d'interès (nom_columna) i retorna un
    # DataFrame modificat.

    # Aplicar la funció a la columna 'barthel' per obtenir la suma dels valors, però excloent la clau 'data'
    data['Barthel resultats'] = data[nom_columna].apply(suma_sense_data)  # Aplica la funció suma_sense_data a cada
    # element de la columna 'barthel' en el DataFrame, emmagatzemant el resultat en una nova columna anomenada
    # 'Barthel resultats'.
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
    # DataFrame de pandes (data), el nom de la columna d'interès (nom_columna) i retorna un DataFrame modificat amb una
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
                    if valor.replace('.', '', 1).isdigit():  # Verifica si el valor és un Nom
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
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.

    Retorna:
        - DataFrame modificat amb una nova columna que conté l'últim valor de la clau 'resultat'.
    """

    # Aplicar la funció obtenir_ultima_clau a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(obtenir_ultim_diccionari)

    return data  # Retorna el DataFrame modificat


# TODO: el Nom de las funciones debe proporcionar el significado de lo que hacen, modificalo.
def obtenir_ultim_diccionari(diccionaris):
    """
    Funció interna para obtenir l'últim valor de la clau 'resultat' en la llista de diccionaris.

    Paràmetres:
        - diccionaris: llista de diccionaris.

    Retorna:
        - Últim valor de la clau 'resultat' o None si aquest no està present.
    """
    if diccionaris:  # Verificar si la llista de diccionaris no està buida
        ultim_diccionari = diccionaris[-1]  # Obtenir l'últim diccionari de la llista
        if 'resultat' in ultim_diccionari:  # Verificar si la clau 'resultat' està present en l'últim diccionari
            return ultim_diccionari['resultat']  # Retornar el valor de la clau 'resultat'
    return None  # Retornar None si la llista de diccionaris està buida o si 'resultat' no està present en l'últim
    # diccionari


# Funció per obtenir el pes dels pacients o en el cas de que hi hagi més d'un valor, obtenir la seva mitjana
# TODO: elige un idioma y mantenlo consistente en todo el código.
def obtenir_pes_o_mitjana(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Funció per calcular la mitjana dels valors de la clau 'valor' als diccionaris d'una llista.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté les llistes de diccionaris.

    Retorna:
        - DataFrame modificat amb una nova columna que conté la mitjana dels valors.
    """
    # Aplica el mètode estàtic calcular_mitjana a la columna especificada del DataFrame
    data['Mitjana pes'] = data[nom_columna].apply(calcular_mitjana)

    return data


def calcular_mitjana(diccionaris):
    """
    Calcula la mitjana dels valors numèrics de la clau 'valor' en una llista de diccionaris.

    Paràmetres:
        - diccionaris: llista de diccionaris amb claus 'valor' i 'data'.

    Retorna:
        - Mitjana dels valors numèrics de la clau 'valor'.
          Retorna None si la llista de diccionaris està buida, no és vàlida o conté una llista buida.
    """
    # Verificar si la entrada no es vàlida o si és una llista buida
    if not diccionaris or not isinstance(diccionaris, list) or (len(diccionaris) == 1 and not diccionaris[0]):
        return None

    valors = []  # llista per emmagatzemar els valors numèrics de 'valor'

    # Iterar sobre cada diccionari en la llista
    for dic in diccionaris:
        if isinstance(dic, dict) and 'valor' in dic:
            valor = dic['valor']
            if isinstance(valor, (int, float)):
                valors.append(valor)
            elif isinstance(valor, str):
                try:
                    valor_float = float(valor)
                    valors.append(valor_float)
                except ValueError:
                    pass  # Ignorar valors no numèrics o conversions sense èxit

    # Calcular la mitjana si s'han trobat valors numèrics
    if valors:
        promedio = sum(valors) / len(valors)
        return promedio
    else:
        return None


# Funció que fa un sumatori dels valors de cada clau de la columna canadenca sense tenir en compte alguns d'ells, i la 
# compara amb la clau 'total', si coincideixen els valors retorna el sumatori realitzat
def canadenca_comparada(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Funció per comparar el sumatori de claus determinades amb el valor de 'total' en una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.

    Retorna:
        - DataFrame modificat amb una nova columna que conté el resultat desitjat.
    """
    # Aplicar la funció calcular_sumatori_i_comparar a la columna especificada del DataFrame
    data['Canadenca sumatoris comparats'] = data[nom_columna].apply(
        lambda x: calcular_sumatori_i_comparar(x[0]) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Retornar el DataFrame modificat


def calcular_sumatori_i_comparar(diccionari):
    """
    Funció interna per calcular el sumatori i comparar-ho amb el valor de 'total' en un diccionari.

    Paràmetres:
        - diccionari: Diccionari que conté les dades.

    Retorna:
        - Sumatori calculat si coincideix amb 'total', None en cas contrari.
    """
    if not isinstance(diccionari, dict):  # Verificar si el diccionari no és vàlid
        return None

    sumatori = 0  # Inicialitzar el sumatori
    claus_excloses = ['total', 'dataValoracio', 'horaValoracio']

    # Calcular el sumatori de les claus numèriques vàlides en el diccionari
    for clau, valor in diccionari.items():
        if clau not in claus_excloses and valor.strip():  # Excloure claus no desitjades i valores buits
            try:
                # Convertir coma a punt per transformar a float
                valor_float = float(valor.replace(',', '.'))
                sumatori += valor_float
            except ValueError:
                pass  # Ignorar valors no numèrics

    # Obtenir el valor de 'total' del diccionari
    total = diccionari.get('total')

    # Comparar el sumatori amb 'total' i retornar el resultat adequat
    if total is not None and sumatori == float(total):
        return sumatori
    else:
        return None


# Funció que retorna un 1 en cas de que en la clau 'disfagia'o 'disfagiaConeguda' (de la llista de diccionaris 'mecvvs'
# hi hagi un 'SI' o 'S'
def disfagia_mecvvs(data: pd.DataFrame, nom_columna: str) -> pd.DataFrame:
    """
    Funció per comparar el valor de 'disfagia' en l'últim diccionari amb 'SI' o 'S' en una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionarios.

    Retorna:
        - DataFrame modificat amb una nova columna que conté el resultat desitjat.
    """
    # Aplicar la funció obtenir_ultima_disfagia a la columna especificada del DataFrame
    data['Disfàgia MECVV'] = data[nom_columna].apply(
        lambda x: obtenir_ultima_disfagia(x) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Retornar el DataFrame modificat


def obtenir_ultima_disfagia(diccionaris):
    """
    Obté el valor de la clau 'disfagia' o 'disfagiaConeguda' de l'últim diccionari vàlid que conté aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.

    Retorna:
        - 1 si el valor de 'disfagia' és 'SI' o 'S'.
        - 0 si el valor de 'disfagia' és 'NO' o 'N'.
        - None si la clau 'disfagia' no es troba en cap diccionari vàlid.
    """
    if not isinstance(diccionaris, list) or not diccionaris:
        return None  # Retornar None si l'entrada no és una llista vàlida o està buida

    valor_disfagia = None  # Valor per defecte

    # Iterar cap enrere en la llista de diccionaris
    for dic in reversed(diccionaris):
        if isinstance(dic, dict):
            # Buscar la clau 'disfagia' o 'disfagiaConeguda'
            if 'disfagia' in dic:
                valor_disfagia = dic['disfagia'].strip().upper()
            elif 'disfagiaConeguda' in dic:
                valor_disfagia = dic['disfagiaConeguda'].strip().upper()

            # Evaluar el valor de 'disfagia' normalitzat
            if valor_disfagia is not None:
                if valor_disfagia == 'SI' or valor_disfagia == 'S':
                    return 1
                elif valor_disfagia == 'NO' or valor_disfagia == 'N':
                    return 0

    return None  # Retornar None si no es troba la clau 'disfagia' en cap diccionari vàlid


# Funció que busca en les claus indicades, els valors que aquestes contenen i transforma els valors trobats (si i no) 
# en 1 i 0
def extreure_valors_claus(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure el valor d'una clau específica en l'últim diccionari d'una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - clau: Clau el valor de la qual es desitja extreure de cada diccionari.
        - nova_columna: Nom de la nova columna on s'emmagatzemen els valors extrets.

    Retorna:
        - DataFrame modificat amb una nova columna que conté els valors extrets.
    """
    # Aplicar la funció per extreure el valor de la clau a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtenir_valor_clau(x, clau) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Retornar el DataFrame modificat


def obtenir_valor_clau(diccionaris, clau):
    """
    Obté el valor d'una clau específica de l'últim diccionario vàlid que conté aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.
        - clau: Clau el valor de la qual es desitja extreure de cada diccionari.

    Retorna:
        - Valor de la clau especificada si es troba en l'últim diccionari vàlid.
        - None si la clau no es troba en cap diccionari vàlid.
    """
    if not isinstance(diccionaris, list) or not diccionaris:
        return None  # Retornar None si l'entrada no és una llista vàlida o està buida

    valor = None  # Valor per defecte

    # Iterar cap endarrere en la llista de diccionaris
    for diccionari in reversed(diccionaris):
        if isinstance(diccionari, dict):
            if clau in diccionari:
                valor = diccionari[clau]
                break  # Sortir del bucle si la clau es troba en el diccionari

    # Transformar valors 'SI' o 'S' en 1 i 'NO' o 'N' en 0
    if valor is not None:
        if valor.strip().upper() == 'SI' or valor.strip().upper() == 'S':
            return 1
        elif valor.strip().upper() == 'NO' or valor.strip().upper() == 'N':
            return 0

    return valor  # Retornar el valor trobat o None si la clau no s'ha trobat


# Funció que retorna el valor (paraula) de la clau introduïda, de l'últim diccionari de la fila 
####### Esta funcion se podria usar también para extreure los resultados de mna y emina
def extreure_valors_claus_simple(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure el valor d'una clau específica en l'últim diccionari d'una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - clau: Clau el valor de la qual es desitja extreure de cada diccionari.
        - nova_columna: Nom de la nova columna on s'emmagatzemaran els valors extrets.

    Retorna:
        - DataFrame modificat amb una nova columna que conté els valors extrets.
    """
    # Aplicar la funció per extreure el valor de la clau a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtenir_valor(x, clau) if isinstance(x, list) and len(x) > 0 else None)

    return data  # Retornar el DataFrame modificat


def obtenir_valor(diccionaris, clau):
    """
    Obté el valor d'una clau específica de l'últim diccionari vàlid que conté aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.
        - clau: Clau el valor de la qual es desitja extreure de cada diccionari.

    Retorna:
        - Valor de la clau especificada si aquesta es troba en l'últim diccionari vàlid.
        - None si la clau no es troba en cap diccionari vàlid.
    """
    if not isinstance(diccionaris, list) or not diccionaris:
        return None  # Retornar None si l'entrada no és una llista vàlida o està buida

    valor = None  # Valor per defecte

    # Iterar cap enrere en la llista de diccionaris
    for dic in reversed(diccionaris):
        if isinstance(dic, dict):
            if clau in dic:
                valor = dic[clau]
                break  # Sortir del bucle si es troba la clau en el diccionari

    return valor  # Retornar el valor trobat o None si la clau no s'ha trobat


# Funció que retorna els valors de les claus introduïdes
def obtenir_valors_clau_interes(data: pd.DataFrame, nom_columna: str, clau_interes: str,
                                nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure els valors de la clau 'value' d'una llista de diccionaris en una columna, filtrant per un 
    Nom d'interès.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - clau_interes: Nom d'interès per filtrar l'extracció de valors.
        - nova_columna: Nom per la nova columna que contindrà els valors extrets.

    Retorna:
        - DataFrame amb una nova columna que conté els valors de 'value' filtrats pel Nom d'interès.
    """
    # Crear una llista per emmagatzemar els valors extrets
    valors_extrets = []

    # Iterar sobre cada fila de la columna de diccionaris
    for llista_diccionaris in data[nom_columna]:
        if isinstance(llista_diccionaris, list) and llista_diccionaris:
            valor_extret = None
            for diccionari in llista_diccionaris:
                # Verificar si el diccionari conté el Nom d'interès
                if 'name' in diccionari and diccionari['name'] == clau_interes:
                    # Extreure el valor de 'value'
                    valor_extret = diccionari.get('value')
                    break  # Sortir del bucle un cop s'ha trobat el Nom d'interès
            # Afegir el valor extret a la llista de valors
            valors_extrets.append(valor_extret)
        else:
            # Si la llista de diccionaris és buida o no vàlida, afegir None
            valors_extrets.append(None)

    # Verificar si la longitud de valors extrets coincideix amb la longitud del DataFrame original
    if len(valors_extrets) != len(data):
        raise ValueError("La longitud de valors extrets no coincideix amb la longitud del DataFrame original.")

    # Afegir els valors extrets com a una nova columna al DataFrame original
    data[nova_columna] = valors_extrets

    return data


# Funció per calcular el valor de l'index de Charlson per a un pacient, tenint en compte que cada codi representa un 
# valor i que aquests valors es poden anar sumant si el pacient en té més d'un
def index_charlson(data: pd.DataFrame, columna_interes: str, nova_columna: str, charlson_dict: dict) -> pd.DataFrame:
    """
    Calcula l'índex de Charlson per a cada entrada en la llista de 'codiDiagnostics' en la columna especificada
    i afegeix el resultat a una nova columna en el DataFrame.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - columna_entrada: Nom de la columna que conté la llista de codis diagnòstics.
        - nova_columna: Nom de la nova columna on s'emmagatzemaran els resultats.
        - charlson_dict: Diccionari que conté els diferents codis ICD amb els seus respectius valors.

    Retorna:
        - DataFrame modificat amb la nova columna d'índex de Charlson.
    """
    data[nova_columna] = 0  # Inicialitzar la nova columna amb valors predeterminats

    # Recòrrer cada fila del DataFrame
    for index, row in data.iterrows():
        charlson_value = 0  # Inicialitzar el valor de Charlson per aquesta fila

        # Obtenir la llista de codis diagnòstics de l'entrada actual
        diagnostics_llista = row[columna_interes]

        if diagnostics_llista is None or not isinstance(diagnostics_llista, list):
            continue  # Saltar a la següent fila si no hi ha llista de codis diagnòstics válida

        # Iterar sobre cada diccionari en la llista de codis diagnòstics
        for diagnostic_dic in diagnostics_llista:
            # Obtenir la llista de codis de diagnòstic d'aquest diccionari
            codis_diagnostics = diagnostic_dic.get('codiDiagnostics', [])

            # Iterar sobre cada codi de diagnòstic en la llista
            for codi_diagnostic in codis_diagnostics:
                if isinstance(codi_diagnostic, str) and codi_diagnostic:
                    # Buscar el codi en el diccionari de Charlson (charlson_dict)
                    for value, codes in charlson_dict.items():
                        if any(codi_diagnostic.startswith(code) for code in codes):
                            charlson_value += value
                            break  # Sortir del bucle un cop es troba la coincidència

        # Assignar el valor calculat a la nova columna en el DataFrame
        data.loc[index, nova_columna] = charlson_value

    return data


# Funció que retorna el pes més antic a partir de mirar la clau 'data' que conté el diccionari a cada fila 
def obtenir_pes_mes_antic(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Troba el pes més antic en la llista de diccionaris 'pes' de cada fila i ho guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el pes més antic    
        
    Retorna:
        - DataFrame modificat amb la nova columna del pes més antic.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        data_pes = row['pes']

        if not data_pes or len(data_pes) == 0:
            continue  # Passar a la següent fila si 'data_pes' està buit

        # Trobar el diccionari amb la data més antiga en la clau 'data'
        try:
            oldest_data_pes = min(data_pes, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            oldest_pes_weight = oldest_data_pes['valor']
            data.loc[index, nova_columna] = oldest_pes_weight
        except ValueError:
            # En caso d'error  al parsejar la data, continuar amb la següent fila
            continue

    return data


# Funció que retorna el pes més actual, com en el cas anterior revisant la clau 'data'
def obtenir_pes_mes_nou(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Troba el pes més nou en la llista de diccionaris 'pes' de cada fila i ho guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el pes més nou.

    Retorna:
        - DataFrame modificat amb la nova columna del peso més nou.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        pes_data = row['pes']

        if not pes_data or len(pes_data) == 0:
            continue  # Passar a la següent fila si 'pes_data' està buit

        # Trobar el diccionari amb la data més nova/actual en la clau 'data'
        try:
            newest_data_pes = max(pes_data, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            newest_pes = newest_data_pes['valor']
            data.loc[index, nova_columna] = newest_pes
        except ValueError:
            # En cas d'error al parsejar la data, continuar amb la següent fila
            continue

    return data


# Funció que retorna la data que correspon al pes més antic
def obtenir_data_pes_mes_antic(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Troba la data més antiga en la llista de diccionaris 'pes' de cada fila i la guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà la data més antiga.

    Retorna:
        - DataFrame modificat amb la nova columna de la data més antiga.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        data_pes = row['pes']

        if not data_pes or len(data_pes) == 0:
            continue  # Passar a la següent fila si data_pes està buit

        # Trobar el diccionari amb la data més antiga en la clau 'data'
        try:
            oldest_data_pes = min(data_pes, key=lambda x: datetime.strptime(x['data'], '%Y-%m-%d'))
            oldest_date_pes = oldest_data_pes['data']
            data.loc[index, nova_columna] = oldest_date_pes
        except ValueError:
            # En cas d'error al parsejar la data, continuar amb la següent fila
            continue

    return data


# Funció que retorna la primera data en la qual es compleix que hi ha un test MECVV positiu (disfagia+alteració
# seguretat o eficàcia)
def obtenir_primera_data_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Troba la data en la llista de diccionaris 'mecvvs' quan es compleixen certes condicions i la guarda en una
    nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà la data que compleix per primer cop les condicions
        esmentades.

    Retorna:
        - DataFrame modificat amb la nova columna de la data que compleix les condicions.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        mecvvs_data = row['mecvvs']

        if not mecvvs_data or len(mecvvs_data) == 0:
            continue  # Passar a la següent fila si 'mecvvs_data' està buit

        # Buscar la primera data que compleixi les condicions
        for mecvv_data in mecvvs_data:
            if ('disfagia' in mecvv_data and mecvv_data['disfagia'] in ['SI', 'S']) or \
                    ('disfagiaConeguda' in mecvv_data and mecvv_data['disfagiaConeguda'] in ['SI', 'S']):
                if ('alteracioSeguretat' in mecvv_data and mecvv_data['alteracioSeguretat'] in ['SI', 'S']) or \
                        ('alteracioEficacia' in mecvv_data and mecvv_data['alteracioEficacia'] in ['SI', 'S']):
                    data_primera_condicio = datetime.strptime(mecvv_data['data'][:8], '%Y%m%d').strftime('%Y-%m-%d')
                    data.loc[index, nova_columna] = data_primera_condicio
                    break  # Aturar la cerca un cop es troba la data

    return data


# Funció que retorna el pes, tenint en compte la data del primer MECVV positiu. Per a que retorni el pes, la data
# d'aquest ha de coincidir amb la data que hi ha en 'data primer mecvv', amb un interval de 3 dies de marge
def obtenir_pes_coincident_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Troba el pes en la llista de diccionaris 'pes' que coincideix amb la data de 'data primer mecvv' dins d'un rang de
     ±3 dies i guarda el pes corresponent en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el pes trobat.

    Retorna:
        - DataFrame modificat amb la nova columna del peso que coincideixi amb la data del primer MECVV positiu
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        data_primer_mecvv = row['Data primer MECVV']
        pes_kg = row['pes']

        if not data_primer_mecvv or not pes_kg or len(pes_kg) == 0:
            continue  # Passar a la següent fila si no  hi ha cap data en 'data_primer_mecvv', o 'pes_kg' està buit

        try:
            # Convertir la data de 'data_primer_mecvv' al format datetime
            mecvv_datetime = datetime.strptime(data_primer_mecvv, '%Y-%m-%d')

            # Definir els límits del rang de dates (±3 días)
            date_start = mecvv_datetime - timedelta(days=3)
            date_end = mecvv_datetime + timedelta(days=3)

            # Buscar el pes en 'pes_kg' que correspon al rang de dates
            for pes_entry in pes_kg:
                pes_date_str = pes_entry.get('data')
                if pes_date_str:
                    pes_date = datetime.strptime(pes_date_str, '%Y-%m-%d')
                    if date_start <= pes_date <= date_end:
                        data.loc[index, nova_columna] = pes_entry.get('valor')
                        break  # Aturar la cerca després de trobar la primera coincidència
        except ValueError:
            # En cas d'error al parsejar la data, continuar amb la següent fila
            continue

    return data


# Funció per obtenir la resta de dues columnes que contenen valors de tipus object
def restar_columnes_object(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Resta els valors de dues columnas d'un DataFrame i emmagatzema el resultat en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - columna1: Nom de la primera columna per restar.
        - columna2: Nom de la segona columna per restar.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el resultat de la resta.

    Retorna:
        - DataFrame modificat amb la nova columna del resultat de la resta.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        valor1 = row[columna1]
        valor2 = row[columna2]

        try:
            # Intentar convertir els valors a tipus numéricos (flotantes (float) o enteros (int))
            valor1 = float(valor1)
            valor2 = float(valor2)

            # Realitzar la resta i emmagatzemar el resultat en la nova columna
            data.loc[index, nova_columna] = valor1 - valor2
        except (ValueError, TypeError):
            # En cas d'error al convertir o restar, retornar None en la nova columna
            data.loc[index, nova_columna] = None

    return data


# Funció per transformar en 0 i 1 els valors de la columna de 'Creatinina', sent 0 quan <1.5 y 1 quan >1.5
def binaritzar_15_creatinina(data, nom_columna, nova_columna):
    # Converteix els valors de la columna a tipus numèric i omple els valors no vàlids amb NaN
    data[nom_columna] = pd.to_numeric(data[nom_columna], errors='coerce')

    # Aplica una funció lambda a cada fila per a avaluar els valors
    data[nova_columna] = data[nom_columna].apply(lambda x: 1 if (not pd.isna(x) and x > 1.5) else 0)

    return data


## APUNTES
# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es llista de diccionarios []
# float num enteros, int para decimales con punto
