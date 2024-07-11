import itertools

import pandas as pd
from datetime import datetime, timedelta
from scipy.stats import shapiro, ttest_ind, mannwhitneyu, chi2_contingency, kstest
import numpy as np
import matplotlib.pyplot as plt
from prettytable import PrettyTable


# Funció per obtenir les columnes de les malalties segons els seus ICD
def codis_icd(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Funció que realitza una cerca en una llista de diccionaris d'una columna present en un DataFrame de pandas. Busca un
    conjunt de codis específics i assigna 1 si algun d'aquests codis estan present a la llista 'codiDiagnostics' de cada
    fila 'ingres', i 0 si cap dels codis està present. El resultat s'emmagatzema en una nova columna del DataFrame.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades
        - llista: llista de codis que es busquen en la llista de 'codiDiagnostics'.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el resultat.

    Retorna:
        - DataFrame modificat amb la nova columna que indica la presència dels codis buscats en 'codiDiagnostics'
    """
    data[nova_columna] = 0  # Inicialitzar la nova columna amb 0

    for index, fila in data.iterrows():  # Iterar sobre cada fila del DataFrame
        found = False
        if not fila['ingressos']:
            continue
        for ingres in fila['ingressos']:  # Iterar sobre cada ingrés en 'ingressos'
            if not ingres['codiDiagnostics']:
                continue
            for valor in ingres['codiDiagnostics']:  # Iterar sobre cada valor en 'codiDiagnostics'
                if valor and any(codi and valor[:3] == codi[:3] for codi in llista):  # Verificar si els primers tres dígits del valor estan a la llista
                    data.at[index, nova_columna] = 1  # Assignar 1 si els primers tres dígits estan presents
                    found = True
                    break  # Aturar el bucle si es troben els primers tres dígits per aquesta fila
            if found:
                break  # Aturar el bucle si es troben els primers tres dígits per aquesta fila
    return data


# Funció per calcular el nombre d'ingressos que cada pacient ha sigut ingressat
def nombre_ingressos(data: pd.DataFrame, nom_columna: str = 'ingressos', admissions: str = 'Admissions',
                     emergencies: str = 'Emergencies') -> pd.DataFrame:
    """
    Funció per comptar el nombre de diccionaris presents en una llista de diccionaris d'una columna i emmagatzemar-ho
    en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el nombre de diccionaris.

    Retorna:
        - DataFrame modificat amb la nova columna que conté el nombre de diccionaris.
    """
    for index, fila in data.iterrows():
        num_admissions = 0
        num_emergencies = 0
        for ingres in fila[nom_columna]:
            # Si en el diccionari hi ha la clau 'codiDiagnosticPrincipal' vol dir que és un ingrés per admissions,
            # si no, indica que és un ingrés per urgències.
            if 'codiDiagnosticPrincipal' in ingres:
                num_admissions += 1
            else:
                num_emergencies += 1
        data.at[index, admissions] = num_admissions
        data.at[index, emergencies] = num_emergencies
    return data


# Funció que fa un sumatori i retorna el total de dies que el pacient ha estat ingressat
def dies_ingressat_total(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per calcular el total de dies ingressat per cada fila i emmagatzemar-ho en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista d'ingressos.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el total de dies ingressat.

    Retorna:
        - DataFrame modificat amb la nova columna que conté el total de dies ingressat.
    """
    for index, fila in data.iterrows():
        suma_dies = 0
        for ingres in fila[nom_columna]:
            data_ingres = datetime.strptime(ingres['dataIngres'], '%Y-%m-%d')
            # Si la data d'alta no està present, no es pot calcular la diferència de dies
            if 'dataAlta' not in ingres:
                continue
            data_alta = datetime.strptime(ingres['dataAlta'], '%Y-%m-%d')
            diferencia = data_alta - data_ingres
            suma_dies += diferencia.days

        data.at[index, nova_columna] = suma_dies

    return data


# Funció que retorna la data quan està present algun codi de la llista introduïda. Si el codi es repeteix, retorna
# la data més antiga
def obtenir_data_presencia_codi(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Troba la data més antiga (de la clau 'dataIngres') associada a un codi de diagnòstic de la llista 'llista', a la
    llista de diccionaris 'ingressos' de cada fila del DataFrame 'data' i crea una nova columna amb els resultats.

    Paràmetres:
     -data: DataFrame que conté les dades.
    -llista: llista de codis de diagnòstic a cercar.
    -nova_columna: nom de la nova columna on s'emmagatzemaran les dates més antigues si el codi era present.

    Retorna:
    DataFrame modificat amb la nova columna que conté les dates més antigues per codi.
    """
    # Aplicar la funció 'trobar_data_mes_antiga' a cada fila del DataFrame 'data'
    data[nova_columna] = data['ingressos'].apply(lambda x: trobar_data_mes_antiga(x, llista))

    return data


def trobar_data_mes_antiga(ingressos, llista):
    codi_data_mes_antiga = {}

    for ingres in ingressos:
        codis_diagnostics = ingres.get('codiDiagnostics', [])  # Obtenir la llista de codis de diagnòstic per a aquest
        # ingrés
        data_ingres = ingres.get('dataIngres', '')  # Obtenir la data d'ingrés per a aquest ingrés

        for codi in codis_diagnostics:
            if codi in llista:  # Comprovar si el codi de diagnòstic és a la llista d'interès
                if codi not in codi_data_mes_antiga or data_ingres < codi_data_mes_antiga[codi]:
                    # Si el codi no és present al diccionari, o la data d'ingrés actual és més antiga que la registrada,
                    # actualitza la data més antiga per a aquest codi de diagnòstic.
                    codi_data_mes_antiga[codi] = data_ingres

    # Obtenir només les dates més antigues per codi
    dates_mes_antigues = {codi: data for codi, data in codi_data_mes_antiga.items()}

    # Trobar la data més antiga de totes les dates trobades
    data_mes_antiga = min(dates_mes_antigues.values()) if dates_mes_antigues else None

    return data_mes_antiga


# Funció que resta dues columnes que contenen dates
def restar_dates(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Resta dues columnes que contenen dates en format %Y-%m-%d i guarda el resultat en una nova columna.

    Paràmetres:
    -data: DataFrame que conté les dades.
    -columna1: nom de la primera columna amb dates.
    -columna2: nom de la segona columna amb dates.
    -nova_columna: nom de la nova columna on s'emmagatzemaran els resultats de la resta.

    Retorna:
    DataFrame modificat amb la nova columna que conté la diferència entre les dates.
    """
    # Convertir les columnes de dates al tipus datetime
    data[columna1] = pd.to_datetime(data[columna1])
    data[columna2] = pd.to_datetime(data[columna2])

    # Restar les dates i calcular la diferència en valors absoluts
    data[nova_columna] = (data[columna1] - data[columna2]).dt.days.abs()

    return data


# Funció per realitzar la suma de resultats de la columna 'barthel' sense agafar el valor de la clau 'data'
def sumar_barthel(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per sumar els valors de la columna especificada excloent la clau 'data' i emmagatzemar-ho en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.
        - nova_columna: nom de la nova columna on s'emmagatzemarà la suma dels valors excloent 'data'.

    Retorna:
        - DataFrame modificat amb la nova columna de suma total excloent la clau 'data'.
    """
    # Aplicar la funció suma_sense_data a la columna especificada
    data[nova_columna] = data[nom_columna].apply(suma_sense_data)

    return data


def suma_sense_data(diccionari):
    """
    Funció interna per sumar els valors del diccionari excloent la clau 'data'.

    Paràmetres:
        - diccionari: diccionari del qual es volen sumar els valors.

    Retorna:
        - Suma total dels valors excloent la clau 'data'.
    """
    suma_parcial = 0
    # Comprovar que el diccionari no estigui buit
    if not diccionari or not isinstance(diccionari, dict):
        return None
    for clau, valor in diccionari.items():
        if clau != 'data':
            suma_parcial += int(valor)
    return suma_parcial

# Funció per obtenir el pes dels pacients o en el cas que hi hagi més d'un valor, obtenir la seva mitjana
def obtenir_pes_o_mitjana(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per calcular la mitjana dels valors de la clau 'valor' als diccionaris d'una llista.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté les llistes de diccionaris.
        - nova_columna: nom per la nova columna que recollirà la mitjana dels valors.

    Retorna:
        - DataFrame modificat amb una nova columna que conté la mitjana dels valors.
    """
    # Aplica la funció interna calcular_mitjana a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(calcular_mitjana)

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
    # Verificar si l'entrada no és vàlida o si és una llista buida
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
        mitjana = sum(valors) / len(valors)
        return round(mitjana, 2)
    else:
        return None


# Funció que retorna un 1 en cas que en la clau 'disfagia'o 'disfagiaConeguda' (de la llista de diccionaris 'mecvvs')
# hi hagi un 'SI' o 'S'
def disfagia_mecvvs(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per comparar el valor de 'disfagia' o 'disfagiaConeguda' en l'últim diccionari amb 'SI' o 'S' en una llista
    de diccionaris.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.

    Retorna:
        - DataFrame modificat amb una nova columna que conté el resultat desitjat.
    """
    # Aplicar la funció obtenir_ultima_disfagia a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtenir_ultima_disfagia(x) if isinstance(x, list) and len(x) > 0 else None)

    # Convertir la columna obtinguda a tipus "object"
    data[nova_columna] = data[nova_columna].astype(object)

    return data  # Retornar el DataFrame modificat


def obtenir_ultima_disfagia(diccionaris):
    """
    Funció per obtenir el valor de la clau 'disfagia' o 'disfagiaConeguda' de l'últim diccionari vàlid que conté
    aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.

    Retorna:
        - 1 si el valor de 'disfagia' és 'SI' o 'S'.
        - 0 si el valor de 'disfagia' és 'NO' o 'N'.
        - None si la clau 'disfagia' no es troba en cap diccionari vàlid o si el diccionari està buit.
    """
    if not isinstance(diccionaris, list) or not diccionaris:
        return None  # Retornar None si l'entrada no és una llista vàlida o es troba buida

    valor_disfagia = None  # Valor per defecte

    # Iterar cap enrere en la llista de diccionaris
    for dic in reversed(diccionaris):
        if not dic:  # Si el diccionari està buit, retorna None
            return None

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


# Funció que retorna el valor (paraula) de la clau introduïda de l'últim diccionari. Binaritza valors SI/NO a 1/0
# respectivament
def extreure_valors_binaritzants(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure el valor d'una clau específica de l'últim diccionari d'una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.
        - clau: clau el valor de la qual es desitja extreure de cada diccionari.
        - nova_columna: nom de la nova columna on s'emmagatzemaran els valors extrets.

    Retorna:
        - DataFrame modificat amb una nova columna que conté els valors extrets.
    """
    # Aplicar la funció per extreure el valor de la clau a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtenir_valor(x, clau) if isinstance(x, list) and len(x) > 0 else None)

    # Transformar els valors 'SI' o 'S' en 1 i 'NO' o 'N' en 0
    data[nova_columna] = data[nova_columna].apply(
        lambda x: 1 if x == 'SI' or x == 'S' else (0 if x == 'NO' or x == 'N' else x))

    return data  # Retornar el DataFrame modificat


def obtenir_valor(diccionaris, clau):
    """
    Funció interna que obté el valor d'una clau específica de l'últim diccionari vàlid que conté aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.
        - clau: clau el valor de la qual es desitja extreure de cada diccionari.

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


# Funció per obtenir la columna MECV-V positiu que es dona quan el pacient té disfàgia i alteració de la seguretat i/o
# de l'eficàcia. Retorna un 1 en cas que es donin aquestes condicions
def mecvv_positiu(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Funció que troba quan es compleixen certes condicions que indiquen que el test MECV-V ha sortit positiu

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà la primera data que compleix les condicions
        esmentades.

    Retorna:
        - DataFrame modificat amb la nova columna de la data que compleix les condicions.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = 0

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        mecvvs_test = row['mecvvs']

        # Retornar 0 o 1 en funció de si es compleixen o no les condicions
        for mecvv in mecvvs_test:
            if ('disfagia' in mecvv and mecvv['disfagia'] in ['SI', 'S']) or \
                    ('disfagiaConeguda' in mecvv and mecvv['disfagiaConeguda'] in ['SI', 'S']):
                if ('alteracioSeguretat' in mecvv and mecvv['alteracioSeguretat'] in ['SI', 'S']) or \
                        ('alteracioEficacia' in mecvv and mecvv['alteracioEficacia'] in ['SI', 'S']):
                    data.at[index, nova_columna] = 1
                    break  # Si es compleix la condició, es marca com a positiu i se surt del bucle
        else:
            data.at[index, nova_columna] = 0

    return data


# Funció que retorna els valors de les claus introduïdes, tenint en compte que no utilitza les claus com a tal sinó el
# seu contingut (la clau és 'name' però classifica els valors pel que contingui aquesta clau, no per la clau en si)
def obtenir_valors_lab(data: pd.DataFrame, nom_columna: str, paraula_clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per obtenir el valor associat a la paraula clau a la columna de diccionaris i emmagatzemar-ho en una nova
    columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.
        - paraula_clau: paraula clau a buscar dins de la columna 'name' dels diccionaris.
        - nova_columna: nom de la nova columna on s'emmagatzemarà els valors obtinguts.

    Retorna:
        - DataFrame amb la nova columna afegida que conté els valors obtinguts.
    """
    # Llista per emmagatzemar els valors trobats
    valors_trobats = []

    # Iterar sobre cada fila de la columna de diccionaris
    for estructura in data[nom_columna]:
        valor_trobat = None

        # Verificar si l'estructura és una llista de diccionaris que no està buida
        if isinstance(estructura, list) and len(estructura) > 0:
            # Iterar sobre cada diccionari a la llista
            for diccionari in estructura:
                # Verificar si 'name' conté la paraula clau desitjada
                if 'loinc' in diccionari and paraula_clau in diccionari['loinc']:
                    # Obtenir el valor associat a la clau 'value'
                    valor_trobat = diccionari.get('value', None)
                    break  # Aturar la cerca un cop trobat el valor desitjat

        # Afegir el valor trobat a la llista de valors
        valors_trobats.append(valor_trobat)

    # Afegir una nova columna al DataFrame amb els valors trobats
    data[nova_columna] = valors_trobats

    # Retornar el DataFrame modificat amb la nova columna
    return data


# Funció que retorna els valors de la clau que s'introdueixi, mitjançant l'ús de la 'clau_interés', el valor de la
# qual conté el valor. No és aplicable a la columna de labs, ja que aquesta té un format diferent i la clau no conté
# el valor sinó que hi ha les claus 'name' i 'value' que contenen respectivament el nom de la prova i el resultat, per
# tant, no es pot aplicar a aquest diccionari.
def obtenir_valors_clau_interes(data: pd.DataFrame, nom_columna: str, clau_interes: str,
                                nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure els valors de la clau d'interès d'una llista de diccionaris en una columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: nom de la columna que conté la llista de diccionaris.
        - clau_interes: clau d'interès per filtrar l'extracció de valors.
        - nova_columna: nom per la nova columna que contindrà els valors extrets.

    Retorna:
        - DataFrame amb una nova columna que conté els valors de la clau d'interès.
    """
    # Funció per extreure els valors de la clau d'interès per a cada element de la columna
    valors_extrets = []
    for estructura in data[nom_columna]:
        if isinstance(estructura, (dict, list)) and estructura:
            valor_extret = extreu_valor(estructura, clau_interes)
            valors_extrets.append(valor_extret)
        else:
            # Manejar el cas de files buides
            if pd.isna(estructura) or estructura == '':
                valors_extrets.append(None)
            else:
                valors_extrets.append(estructura)

    # Afegir els valors extrets com una nova columna al DataFrame original
    data[nova_columna] = valors_extrets

    return data


def extreu_valor(d, clau):
    """
    Funció interna que extreu el valor associat a una clau específica d'una estructura de dades,
    la qual pot contenir diccionaris i/o llistes imbricades.

    Paràmetres:
        - d: estructura de dades que pot ser un diccionari o una llista de diccionaris.
        - clau: clau el valor de la qual es desitja extreure.

    Retorna:
        - El valor associat a la clau especificada si es troba.
        - None si la clau no es troba en cap diccionari vàlid.
    """
    if isinstance(d, dict):  # Comprovar si d és un diccionari
        if clau in d:  # Comprovar si la clau existeix al diccionari
            return d[clau]  # Retornar el valor associat a la clau
        else:
            for v in d.values():  # Iterar pels valors del diccionari
                resultat = extreu_valor(v, clau)  # Cercar recursivament cada valor per la clau
                if resultat is not None:
                    return resultat  # Retornar el resultat si es troba

    elif isinstance(d, list):  # Comprovar si d és una llista
        for item in d:  # Iterar per cada element de la llista
            resultat = extreu_valor(item, clau)  # Cercar recursivament cada element per la clau
            if resultat is not None:
                return resultat  # Retornar el resultat si es troba

    return None  # Retorna None si la clau no es troba al diccionari o llista


# Funció per calcular el valor de l'índex de Charlson per a un pacient, tenint en compte que cada codi representa un
# valor i que aquests valors es poden anar sumant si el pacient en té més d'un
def index_charlson(data: pd.DataFrame, columna_interes: str, nova_columna: str, charlson_dict: dict) -> pd.DataFrame:
    """
    Funció que calcula l'índex de Charlson per a cada entrada en la llista de 'codiDiagnostics' en la columna
    especificada i afegeix el resultat a una nova columna en el DataFrame.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - columna_entrada: nom de la columna que conté la llista de codis diagnòstics.
        - nova_columna: nom de la nova columna on s'emmagatzemaran els resultats.
        - charlson_dict: diccionari que conté els diferents codis ICD amb els seus respectius valors.

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
            continue  # Saltar a la següent fila si no hi ha llista de codis diagnòstics vàlida

        # Iterar sobre cada diccionari en la llista de codis diagnòstics
        for diagnostic_dic in diagnostics_llista:
            # Obtenir la llista de codis de diagnòstic d'aquest diccionari
            codis_diagnostics = diagnostic_dic.get('codiDiagnostics', [])

            # Iterar sobre cada codi de diagnòstic en la llista
            for codi_diagnostic in codis_diagnostics:
                if isinstance(codi_diagnostic, str) and codi_diagnostic:
                    # Buscar el codi en el diccionari de Charlson anomenat charlson_dict
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
    Funció que troba el pes més antic en la llista de diccionaris 'pes' de cada fila i ho guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el pes més antic
        
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
            # En cas d'error al parsejar la data, continuar amb la següent fila
            continue

    return data


# Funció que retorna el pes més actual, com en el cas anterior revisant la clau 'data'
def obtenir_pes_mes_nou(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Funció que troba el pes més nou en la llista de diccionaris 'pes' de cada fila i ho guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el pes més nou.

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
    Funció que troba la data més antiga en la llista de diccionaris 'pes' de cada fila i la guarda en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà la data més antiga.

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


# Funció que retorna la primera data en la qual es compleix que hi ha un test MECV-V positiu (presència de disfàgia +
# alteració seguretat o eficàcia)
def obtenir_primera_data_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Funció que troba la data més antiga en la llista de diccionaris 'mecvvs' quan es compleixen certes condicions i la guarda en
    una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà la data més antiga que compleixi les condicions
        esmentades.

    Retorna:
        - DataFrame modificat amb la nova columna de la data que compleix les condicions.
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        mecvvs_data = row['mecvvs']
        dates = []

        if mecvvs_data and len(mecvvs_data) > 0:
            # Buscar totes les dates que compleixin les condicions
            for mecvv_data in mecvvs_data:
                if ('disfagia' in mecvv_data and mecvv_data['disfagia'] in ['SI', 'S']) or \
                        ('disfagiaConeguda' in mecvv_data and mecvv_data['disfagiaConeguda'] in ['SI', 'S']):
                    if ('alteracioSeguretat' in mecvv_data and mecvv_data['alteracioSeguretat'] in ['SI', 'S']) or \
                            ('alteracioEficacia' in mecvv_data and mecvv_data['alteracioEficacia'] in ['SI', 'S']):
                        date = datetime.strptime(mecvv_data['data'][:8], '%Y%m%d')
                        dates.append(date)

            # Si hi ha dates que compleixen les condicions, triar la més antiga
            if dates:
                oldest_date = min(dates).strftime('%Y-%m-%d')
                data.loc[index, nova_columna] = oldest_date

    return data


# Funció que retorna el pes, tenint en compte la data del primer MECV-V positiu. Perquè retorni el pes, la data
# d'aquest ha d'haver coincidit amb la data que hi ha en 'Data primer MECV-V', amb un rang de 3 dies de marge
def obtenir_pes_coincident_mecvv(data: pd.DataFrame, nova_columna: str) -> pd.DataFrame:
    """
    Funció que troba el pes en la llista de diccionaris 'pes' que coincideix amb la data de 'Data primer MECV-V' dins
    d'un rang de ±3 dies i guarda el pes corresponent en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el pes trobat.

    Retorna:
        - DataFrame modificat amb la nova columna del pes que coincideixi amb la data del primer MECV-V positiu
    """
    # Inicialitzar la nova columna amb None
    data[nova_columna] = None

    # Iterar sobre cada fila del DataFrame 'data'
    for index, row in data.iterrows():
        data_primer_mecvv = row['Data primer MECV-V']
        pes_kg = row['pes']

        if not data_primer_mecvv or not pes_kg or len(pes_kg) == 0:
            continue  # Passar a la següent fila si no hi ha cap data en 'Data primer MECV-V', o 'pes_kg' està buit

        try:
            # Convertir la data de 'Data primer MECV-V' al format datetime
            mecvv_datetime = datetime.strptime(data_primer_mecvv, '%Y-%m-%d')

            # Definir els límits del rang de dates (±3 dies)
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


# Funció per obtenir la resta de dues columnes que contenen valors de tipus "object"
def restar_columnes_object(data: pd.DataFrame, columna1: str, columna2: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció que resta els valors de dues columnes d'un DataFrame i emmagatzema el resultat en una nova columna.

    Paràmetres:
        - data: DataFrame que conté les dades.
        - columna1: nom de la primera columna a restar.
        - columna2: nom de la segona columna a restar.
        - nova_columna: nom de la nova columna on s'emmagatzemarà el resultat de la resta.

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
            # Intentar convertir els valors a tipus numèric (flotants (float))
            valor1 = float(valor1)
            valor2 = float(valor2)

            # Realitzar la resta i emmagatzemar el resultat en la nova columna
            data.loc[index, nova_columna] = valor1 - valor2
        except (ValueError, TypeError):
            # En cas d'error al convertir o restar, retornar None en la nova columna
            data.loc[index, nova_columna] = None

    return data


# Funció per generar les columnes dels tests MNA, EMINA, Barthel i Canadenca. Posa nom als diferents intervals numèrics
# que els tests poden tenir
def columnes_tests_categorics(df):
    """
    Funció que categoritza resultats de diferents tests i afegeix noves columnes amb valors categòrics al DataFrame.

    Paràmetres:
        - df: DataFrame que conté les dades dels tests.

    Retorna:
        - DataFrame amb noves columnes categòriques basades en els resultats dels tests.
    """
    # Generar els DataFrame dels diferents tests
    df = categoritzar_barthel(df)
    df = categoritzar_mna(df)
    df = categoritzar_emina(df)
    df = categoritzar_canadenca(df)
    return df


def categoritzar_canadenca(df):
    """
    Funció interna que categoritza l'escala neurològica canadenca en 3 categories.

    Paràmetres:
        - df: DataFrame que conté els resultats de l'escala neurològica canadenca.

    Retorna:
        - DataFrame amb una nova columna categòrica basada en els resultats de l'escala neurològica canadenca.
    """
    df['Canadenca resultats'] = pd.to_numeric(df['Canadenca resultats'], errors='coerce')
    # Definir els intervals
    condicions = [
        (df['Canadenca resultats'] > 10) & (df['Canadenca resultats'] <= 11.5),
        (df['Canadenca resultats'] >= 5) & (df['Canadenca resultats'] <= 10),
        (df['Canadenca resultats'] <= 4.5)
    ]
    # Definir els noms dels intervals anteriors
    termes = ['Dèficit neurològic lleu', 'Dèficit neurològic moderat', 'Dèficit neurològic sever']
    df['Canadenca categòrica'] = np.select(condicions, termes, default=None)
    return df


def categoritzar_barthel(df):
    """
    Funció interna que categoritza l'índex de Barthel en 4 categories.

    Paràmetres:
        - df: DataFrame que conté els resultats de l'índex de Barthel.

    Retorna:
        - DataFrame amb una nova columna categòrica basada en els resultats de l'índex de Barthel.
    """
    df['Barthel resultats'] = pd.to_numeric(df['Barthel resultats'], errors='coerce')
    # Definir els intervals
    condicions = [
        (df['Barthel resultats'] > 95),
        (df['Barthel resultats'] > 60) & (df['Barthel resultats'] <= 95),
        (df['Barthel resultats'] > 21) & (df['Barthel resultats'] <= 60),
        (df['Barthel resultats'] <= 20)
    ]
    # Definir els noms dels intervals anteriors
    termes = ['Independent', 'Dependència moderada', 'Dependència severa', 'Dependència total']
    df['Barthel categòric'] = np.select(condicions, termes, default=None)
    return df


def categoritzar_mna(df):
    """
    Funció interna que categoritza l'MNA en 3 categories.

    Paràmetres:
        - df: DataFrame que conté els resultats de l'MNA.

    Retorna:
        - DataFrame amb una nova columna categòrica basada en els resultats de l'MNA.
    """
    df['MNA resultats'] = pd.to_numeric(df['MNA resultats'], errors='coerce')
    # Definir els intervals
    condicions = [
        (df['MNA resultats'] >= 24),
        (df['MNA resultats'] >= 17) & (df['MNA resultats'] <= 23.5),
        (df['MNA resultats'] < 17)
    ]
    # Definir els noms dels intervals anteriors
    termes = ['Estat nutricional normal', 'Risc de malnutrició', 'Malnodrit']
    df['MNA categòric'] = np.select(condicions, termes, default=None)
    return df


def categoritzar_emina(df):
    """
    Funció interna que categoritza l'EMINA en 3 categories.

    Paràmetres:
        - df: DataFrame que conté els resultats de l'EMINA.

    Retorna:
        - DataFrame amb una nova columna categòrica basada en els resultats de l'EMINA.
    """
    df['EMINA resultats'] = pd.to_numeric(df['EMINA resultats'], errors='coerce')
    # Definir els intervals
    condicions = [
        (df['EMINA resultats'] <= 5),
        (df['EMINA resultats'] >= 6) & (df['EMINA resultats'] <= 10),
        (df['EMINA resultats'] >= 11) & (df['EMINA resultats'] <= 15)
    ]
    # Definir els noms dels intervals anteriors
    termes = ['Risc baix', 'Risc moderat', 'Risc alt']
    df['EMINA categòric'] = np.select(condicions, termes, default=None)
    return df


# Funció per classificar en diferents intervals la columna de pèrdua de pes entre ingressos
def categoritzar_perdua_pes(data, columna_origen, columna_nova):
    """
    Funció que converteix els valors numèrics d'una columna de pèrdua de pes a valors categòrics, en una nova columna.

    Paràmetres:
        - data (DataFrame): DataFrame que conté les dades.
        - columna_origen (str): nom de la columna amb els valors numèrics de pèrdua de pes.
        - columna_nova (str): nom de la nova columna per als valors categòrics.

    Retorna: DataFrame modificat amb la nova columna categòrica.
    """
    # Crear una columna temporal per manejar els NaN
    data['categoria_pes_temp'] = data[columna_origen].fillna(-1)

    # Aplicar pd.cut amb els intervals especificats
    data[columna_nova] = pd.cut(
        data['categoria_pes_temp'],
        bins=[-float('inf'), 0, 1, 3, 6, 10, float('inf')],
        labels=['No disponible', '0 kg', '1-3 kg', '3-6 kg', '6-10 kg', '>10 kg'],
        right=False
    )

    # Restaurar els valors None en la nova columna
    data.loc[data[columna_origen].isna(), columna_nova] = 'No disponible'

    # Eliminar la columna temporal
    data.drop(columns=['categoria_pes_temp'], inplace=True)

    return data


# Funció per variables contínues. Comprova si hi ha distribució normal, amb el test de Shapiro-Wilks si és una mida
# mostral de <5.000 o amb el test de Kolmogorov-Smirnov si és >5.000. Si segueix distribució normal farà el test de
# T-test, si no, el test de Mann-Whitney. Finalment, els p-valors obtinguts s'expressen en una hemi-matriu inferior
def test_indepe_plot(category_labels, numeric_values, alpha=0.05):
    """
    Funció que compara grups utilitzant proves T-test o Mann-Whitney U segons la normalitat de les dades i retorna una
    gràfica com a resultat final.

    Paràmetres:
        - category_labels (Series): columa de classificació dels pacients.
        - numeric_values (Series): columna de valors numèrics (edats).
        - alpha (float): nivell de significància pel test de Shapiro-Wilk. Per defecte és 0.05.

    Retorna:
        - None (gràfica de la matriu de p-valors)
    """
    # Agrupar les dades per les etiquetes de categories
    grups = {category: numeric_values[category_labels == category].tolist() for category in category_labels.unique()}

    noms_grups = list(grups.keys())
    num_grups = len(noms_grups)
    matriu_pvalors = np.zeros((num_grups, num_grups))

    for i, (nom_grup1, dades_grup1) in enumerate(grups.items()):
        for j, (nom_grup2, dades_grup2) in enumerate(grups.items()):
            if i >= j:  # Només calcular per la meitat inferior i la diagonal
                # Convertir les dades a sèrie de pandas i intentar de convertir a float
                dades_grup1 = pd.to_numeric(pd.Series(dades_grup1), errors='coerce').dropna()
                dades_grup2 = pd.to_numeric(pd.Series(dades_grup2), errors='coerce').dropna()

                # Comprovar si hi ha suficients dades després d'eliminar NaN
                if len(dades_grup1) <= 3 or len(dades_grup2) <= 3:
                    print(f"No hi ha prou dades per comparar {nom_grup1} i {nom_grup2}")
                    matriu_pvalors[i, j] = np.nan
                    continue

                # Verificar el nombre de mostres
                if len(dades_grup1) >= 5000 or len(dades_grup2) >= 5000:
                    # Utilitzar Kolmogorov-Smirnov test si hi ha més de 5000 pacients en algun dels grups
                    _, p_valor_ks1 = kstest(dades_grup1, 'norm')
                    _, p_valor_ks2 = kstest(dades_grup2, 'norm')

                    if p_valor_ks1 > alpha and p_valor_ks2 > alpha:  # Si ambdós grups són normals:
                        _, p_valor = ttest_ind(dades_grup1, dades_grup2)
                    else:  # Si almenys un dels grups no és normal:
                        _, p_valor = mannwhitneyu(dades_grup1, dades_grup2, alternative='two-sided')
                else:
                    # Utilitzar el test de Shapiro-Wilk si hi ha menys de 5000 mostres als 2 grups
                    _, p_valor_shapiro1 = shapiro(dades_grup1)
                    _, p_valor_shapiro2 = shapiro(dades_grup2)

                    if p_valor_shapiro1 > alpha and p_valor_shapiro2 > alpha:  # Si ambdós grups són normals
                        _, p_valor = ttest_ind(dades_grup1, dades_grup2)
                    else:  # Si almenys un dels grups no és normal
                        _, p_valor = mannwhitneyu(dades_grup1, dades_grup2, alternative='two-sided')

                matriu_pvalors[i, j] = p_valor if not np.isnan(p_valor) else np.nan

    plotejar_matriu(matriu_pvalors, noms_grups)


def plotejar_matriu(matriu, noms_grups):
    """
    Funció interna que genera una gràfica d'hemi-matriu inferior amb els p-valors proporcionats.

    Paràmetres:
        - matriu (np.array): matriu de p-valors a representar.
        - noms_grups (list): llista de noms dels grups.
        - titol (str): títol de la gràfica.

    Retorna:
        - None (gràfica)
    """
    fig, ax = plt.subplots()
    cax = ax.matshow(matriu, cmap='cool')

    for (i, j), val in np.ndenumerate(matriu):
        if i >= j:  # Només mostrar la meitat inferior i la diagonal
            if np.isnan(val):
                ax.text(j, i, 'nan', ha='center', va='center', color='black')
            else:
                color = 'white' if val < 0.05 else 'black'
                ax.text(j, i, f'{val:.4f}', ha='center', va='center', color=color)

    # Configuració del color de fons de la gràfica
    ax.set_facecolor((0, 0, 0, 0))

    # Configuració de colors i etiquetes de la gràfica
    plt.colorbar(cax)
    ax.set_xticks(np.arange(len(noms_grups)))
    ax.set_yticks(np.arange(len(noms_grups)))
    ax.set_xticklabels(noms_grups, rotation=45, ha='left', color='black')
    ax.set_yticklabels(noms_grups, color='black')
    plt.title(f'P-valors de les comparacions entre els grups', color='black')

    # Ajustar la matriu per només mostrar la meitat inferior i la diagonal
    ax.set_xlim(-0.5, len(noms_grups) - 0.5)
    ax.set_ylim(len(noms_grups) - 0.5, -0.5)

    plt.show()


def pairwise_chi2(data_1, data_2):
    """
    Realitza proves de xi-quadrat de manera pairwise entre categories de data_1 contra data_2.

    Paràmetres:
        - data_1 (pd.Series): sèrie de pandas amb dades de la primera variable (categòrica).
        - data_2 (pd.Series): sèrie de pandas amb dades de la segona variable (categòrica).

    Retorna:
    pd.DataFrame: DataFrame amb els resultats del p-valor per cada comparació pairwise.
    """
    categories = data_1.unique()
    results = []

    for cat1, cat2 in itertools.combinations(categories, 2):
        # Filtrar las filas correspondientes a las categorías actuales
        filtered_data = data_1.isin([cat1, cat2])
        contingency_table = pd.crosstab(data_1[filtered_data], data_2[filtered_data])

        chi2, pval, _, _ = chi2_contingency(contingency_table)

        results.append({
            'Categoria 1': cat1,
            'Categoria 2': cat2,
            'Chi-squared': chi2,
            'P-value': pval
        })

    results_df = pd.DataFrame(results)

    return results_df


def test_indepe_bin_plot(data_1, data_2):
    """
    Funció que realitza el test de xi-quadrat per comparar variables categòriques.

    Paràmetres:
        - data_1 (pd.Series): sèrie de pandas amb dades de la primera variable.
        - data_2 (pd.Series): sèrie de pandas amb dades de la segona variable.

    Retorna:
    float: p-valor del test Chi-cuadrado global.
    """
    # Crear la tabla de contingencia
    contingency_table = pd.crosstab(data_1, data_2)
    chi2, pval, _, _ = chi2_contingency(contingency_table)

    # Mostrar los resultados
    print(f'Chi-squared: {chi2:.4f}')
    print(f'P-value: {pval:.4f}')
    print('---')

    if pval < 0.05:
        # Realizar comparaciones pairwise si el p-valor global es significativo
        pairwise_results = pairwise_chi2(data_1, data_2)
        # print(pairwise_results)
        return pval, pairwise_results
    else:
        return pval


# Funció per calcular la mitjana i la desviació estàndard i retornar-les en una taula
def mitjana_i_std_num(llista_dfs, columnes):
    """
    Funció que calcula la mitjana i la desviació estàndard per a cada columna especificada en una llista.

    Paràmetres:
        - llista_dfs (list): llista de tuples on el primer element és el nom del DataFrame i el segon és el DataFrame.
        - columnes (list): llista de noms de columnes a analitzar.

    Retorna:
        - None (valors en una taula)
    """
    # Inicialitza una taula amb les columnes adequades
    resultats_totals = PrettyTable()
    resultats_totals.field_names = ["Columna", "DataFrame", "Mitjana", "Desviació Estàndard"]

    # Comptador per controlar les files per a cada grup de 4
    comptador_fila = 0

    # Itera sobre cada columna d'interès
    for col in columnes:
        # Itera sobre cada DataFrame a la llista
        for nom_df, df in llista_dfs:
            if col in df.columns:
                try:
                    # Intenta convertir la columna a tipus numèric
                    df[col] = pd.to_numeric(df[col],
                                            errors='coerce')  # Els valors no convertibles s'establiran com a NaN
                except ValueError as e:
                    print(f"Error convertint la columna '{col}' a tipus numèric: {str(e)}")
                    continue  # Salta a la següent columna si hi ha un error de conversió

                # Calcula la mitjana i la desviació estàndard
                mean = df[col].mean()
                std = df[col].std()

                # Afegeix una fila a la taula de resultats totals
                resultats_totals.add_row([col, nom_df, f"{mean:.2f}", f"{std:.2f}"])

                # Afegeix un espai en blanc entre els resultats de cada columna
                resultats_totals.add_row(["", "", "", ""])

                # Incrementa el comptador de files i afegeix un espai addicional si el comptador és múltiple de 4
                comptador_fila += 1
                if comptador_fila % 4 == 0:
                    resultats_totals.add_row(["", "", "", ""])  # Afegeix una fila en blanc després de cada grup de 4

    # Imprimeix els resultats totals en forma de taula
    print(resultats_totals)


# Funció per realitzar el compteig de variables categòriques i el seu percentatge, i retornat-ho en una taula
def comptatge_i_percentatge_cat(llista_dfs, columnes):
    """
    Funció que calcula el comptatge i el percentatge de variables per a cada columna especificada en una llista.

    Paràmetres:
        - llista_dfs (list): llista de tuples on el primer element és el nom del DataFrame i el segon és el DataFrame.
        - columnes (list): llista de noms de columnes a analitzar.

    Retorna:
        - None (taula amb els valors)
    """
    # Inicialitza una taula amb les columnes adequades
    resultats_totals = PrettyTable()
    resultats_totals.field_names = ["Columna", "DataFrame", "Valor", "Comptatges", "Percentatges"]

    # Variable per controlar la columna anterior
    col_anterior = None
    primer_passatge = True

    # Itera sobre cada columna d'interès
    for col in columnes:
        # Itera sobre cada DataFrame a la lista
        for nom_df, df in llista_dfs:
            if col in df.columns:
                # Afegeix un espai doble si no és el primer passatge i la columna anterior és diferent
                if not primer_passatge and col != col_anterior:
                    resultats_totals.add_row(["", "", "", "", ""])

                # Calcula el comptatge i el percentatge de les variables
                comptatges = df[col].value_counts()
                percentatges = df[col].value_counts(normalize=True) * 100

                # Dona format als percentatges per afegir el símbol '%' després del resultat
                percentatges_format = percentatges.apply(lambda x: f"{x:.2f}%")

                # Afegeix les files a la taula de resultats totals
                for valor, compte, percentatge in zip(comptatges.index, comptatges.values, percentatges_format.values):
                    resultats_totals.add_row([col, nom_df, valor, compte, percentatge])

                    # Afegeix un espai simple entre les files d'una mateixa columna
                    resultats_totals.add_row(["", "", "", "", ""])

                # Actualitza la columna anterior i marca que ja no és el primer passatge
                col_anterior = col
                primer_passatge = False

    # Imprimeix els resultats totals en forma de taula
    print(resultats_totals)


# Funció per generar una columna que classifica les diferents files en els 3 grups: AMB_PA, AMB_PA_MECVV i SENSE_PA
def segmentacio_bd(df):
    """
    Funció que categoritza les dades en funció de diferents condicions i afegeix una nova columna anomenada
    'split_database' amb les categories corresponents.

    Paràmetres:
        - df (pd.DataFrame): DataFrame de pandas que conté les dades a categoritzar.

    Retorna:
        - pd.DataFrame: DataFrame original amb una nova columna 'split_database' que conté les categories resultants.
    """
    conditions = [
        (df["PA diagnosticada"] == 1.0),
        (df["Dies entre primer ICD pneumònia i primer MECV-V positiu"] < 30) & (df['P diagnosticada'] == 1.0),
        (df["Dies entre primer ICD pneumònia i primer MECV-V positiu"] > 30) & (df['P diagnosticada'] == 1.0)
    ]
    choices = ['AMB_PA', 'AMB_PA_MECVV']
    df['Classificació pacient'] = np.select(conditions, choices, default='SENSE_PA')
    return df



#######################################################################################################################
## APUNTES ##
# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es llista de diccionarios []
# float num enteros, int para decimales con punto
# los tests (mna, emina, barthel...) tienen 2 "categorias":
# - el total que es numerico (mean+-sd) --> ttest/mannwhit
# - los diferentes intervalos que son categoricos (num total/contaje) --> xi

# per numerics --> mean sd
# per categorics --> contatge i %

# min() --> fecha más antigua
# max() --> fecha más reciente

# TODO: poner todo none o nan, mejor none:
# - obtenir_valors_clau_interes: EMINA resultat, MNA resultats, Canadenca resultats
# - obtenir_pes_o_mitjana: Mitjana pes
# - disfagia_mecvvs: Disfàgia MECVV
# - extreure_valors_binaritzants: Alteració eficàcia MECVV, Alteració seguretat MECVV
# - obtenir_valors_lab: Creatinine
# - obtenir_primera_data_mecvv: Data primer MECVV (NaT)
# - obtenir_data_presencia_codi: Data més antiga pneumònia
# - restar_dates: Dies entre primer ICD pneumònia i primer MECVV positiu
# son todos float64 i todos tienen none en el codigo
