import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import shapiro, ttest_ind, mannwhitneyu, chi2_contingency
import numpy as np
import matplotlib.pyplot as plt
from tabulate import tabulate


# TODO: comprueba que los Noms de las funciones sean descriptivos y concisos. OK
# TODO: Agrega una descripción de la funcionalidad de las funciones. OK
# TODO: Comprueba si se pueden simplificar las funciones y si se pueden reutilizar partes de código. OK
# TODO: elige un idioma y mantenlo consistente en todo el código. OK

# Funció per obtenir les columnes de les malalties segons els seus ICD
def codis_ICD(data: pd.DataFrame, llista: list, nova_columna: str) -> pd.DataFrame:
    """
    Funció que realitza una cerca en una llista de diccionaris d'una columna present en un DataFrame de pandas. Busca un
    conjunt de codis específics i assigna 1 si algun d'aquests codis està present a la llista 'codiDiagnostics' de cada
    fila 'ingres', i 0 si cap dels codis està present. El resultat s'emmagatzema en una nova columna del DataFrame.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades
        - llista: Llista de codi que es busquen en la llista de 'codiDiagnostics'.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el resultat.

    Retorna:
        - DataFrame modificat amb la nova columna que indica la presència dels codis buscats en 'codiDiagnostics'
    """
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
def nombre_ingressos(data: pd.DataFrame, nom_columna: str = 'ingressos', admissions: str = 'Admissions',
                     emergencies: str = 'Emergencies') -> pd.DataFrame:
    """
    Funció per comptar el nombre de diccionaris en una llista de diccionaris d'una columna i emmagatzemar-ho en una
    nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el nombre de diccionaris.

    Retorna:
        - DataFrame modificat amb la nova columna que conté el nombre de diccionaris.
    """
    for index, fila in data.iterrows():
        num_admissions = 0
        num_emergencies = 0
        for ingres in fila[nom_columna]:
            # Si en el diccionari hi ha la clau 'codiDiagnosticPrincipal' vol dir que és un ingrés per admissions, sino
            # és un ingrés per urgències.
            if 'codiDiagnosticPrincipal' in ingres:
                num_admissions += 1
            else:
                num_emergencies += 1
        data.at[index, admissions] = num_admissions
        data.at[index, emergencies] = num_emergencies
    return data


# Funció que fa un sumatori de tots els dies en total que el pacient ha estat ingressat
def dies_ingressat_total(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per calcular el total de dies ingressat per cada fila i emmagatzemar-ho en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista d'ingressos.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el total de dies ingressat.

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


# Funció per realitzar la suma de resultats de la columna 'barthel' sense agafar el valor de la clau 'data'. No pot
# ser comparada amb cap clau que contingui resultat/total de la prova (com és el cas de la EMINA o la Canadenca),
# ja que no existeix aquesta clau
def sumar_barthel(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per sumar els valors de la columna especificada excloent la clau 'data' i emmagatzemar-ho en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà la suma dels valors excloent 'data'.

    Retorna:
        - DataFrame modificat amb la nova columna de suma total excloent la clau 'data'.
    """
    # Aplicar la funció suma_sense_data a la columna especificada per fer la suma
    data[nova_columna] = data[nom_columna].apply(suma_sense_data)

    return data


def suma_sense_data(diccionari):
    """
    Funció interna per sumar els valors del diccionari excloent la clau 'data'.

    Paràmetres:
        - diccionari: Diccionari del qual es vol sumar els valors.

    Retorna:
        - Suma total dels valors excloent la clau 'data'.
    """
    suma_parcial = 0
    # Comprovem que el diccionari no estigui buit
    if not diccionari or not isinstance(diccionari, dict):
        return None
    for clau, valor in diccionari.items():
        if clau != 'data':
            suma_parcial += int(valor)
    return suma_parcial


# Funció aplicable a EMINA i Canadenca (a Barthel no es pot aplicar, ja que no conté clau amb resultat/total). Realitza
# un sumatori de les claus del test, obviant algunes quan sigui necessari, i aquest sumatori ho compara amb la clau que
# conté el resultat/total del test. Si els valors coincideixen, retorna el sumatori.
def sumar_i_comparar(data: pd.DataFrame, nom_columna: str, claus_excloure: list, clau_comparacio: str,
                     nova_columna: str) -> pd.DataFrame:
    """
    Funció per sumar els valors d'una llista de diccionaris en una columna i comparar el resultat amb una clau específica.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - claus_excloure: Llista de claus a excloure del sumatori.
        - clau_comparacio: Clau amb la qual vols comparar el sumatori.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà el resultat de la comparació.

    Retorna:
        - DataFrame modificat amb una nova columna que conté el resultat de la comparació.
    """
    # Aplicar la funció a la columna especificada per fer el sumatori i comparar
    data[nova_columna] = data[nom_columna].apply(
        lambda x: suma_compara_diccionaris(x, claus_excloure, clau_comparacio) if isinstance(x, list) else None)

    return data


# Funció per sumar i comparar els valors dels diccionaris
def suma_compara_diccionaris(diccionaris, claus_excloure, clau_comparacio):
    if not diccionaris or not isinstance(diccionaris, list):
        return None

    suma_parcial = 0

    for diccionari in diccionaris:
        if isinstance(diccionari, dict):
            for clau, valor in diccionari.items():
                if clau not in claus_excloure and isinstance(valor, str) and ',' in valor:
                    valor = valor.replace(',', '.')  # Reemplaçar la coma per un punt per a nombres decimals
                if clau not in claus_excloure and isinstance(valor, str) and valor.replace('.', '', 1).isdigit():
                    suma_parcial += float(valor)

            if clau_comparacio in diccionari:
                if suma_parcial == float(diccionari[clau_comparacio]):
                    return suma_parcial

    return None


# Funció per obtenir el pes dels pacients o en el cas que hi hagi més d'un valor, obtenir la seva mitjana
def obtenir_pes_o_mitjana(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per calcular la mitjana dels valors de la clau 'valor' als diccionaris d'una llista.

    Paràmetres:
        - data: DataFrame de pandes que conté les dades.
        - nom_columna: Nom de la columna que conté les llistes de diccionaris.
        - nova_columna: Nom per la nova columna que contindrà la mitjana dels valors.

    Retorna:
        - DataFrame modificat amb una nova columna que conté la mitjana dels valors.
    """
    # Aplica el mètode estàtic calcular_mitjana a la columna especificada del DataFrame
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

# Funció que retorna un 1 en cas de que en la clau 'disfagia'o 'disfagiaConeguda' (de la llista de diccionaris 'mecvvs')
# hi hagi un 'SI' o 'S'
def disfagia_mecvvs(data: pd.DataFrame, nom_columna: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per comparar el valor de 'disfagia' en l'últim diccionari amb 'SI' o 'S' en una llista de diccionaris.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.

    Retorna:
        - DataFrame modificat amb una nova columna que conté el resultat desitjat.
    """
    # Aplicar la funció obtenir_ultima_disfagia a la columna especificada del DataFrame
    data[nova_columna] = data[nom_columna].apply(
        lambda x: obtenir_ultima_disfagia(x) if isinstance(x, list) and len(x) > 0 else None)

    # Convertir la columna obtinguda a tipus object
    data[nova_columna] = data[nova_columna].astype(object)

    return data  # Retornar el DataFrame modificat


def obtenir_ultima_disfagia(diccionaris):
    """
    Obté el valor de la clau 'disfagia' o 'disfagiaConeguda' de l'últim diccionari vàlid que conté aquesta clau.

    Paràmetres:
        - diccionaris: llista de diccionaris.

    Retorna:
        - 1 si el valor de 'disfagia' és 'SI' o 'S'.
        - 0 si el valor de 'disfagia' és 'NO' o 'N'.
        - None si la clau 'disfagia' no es troba en cap diccionari vàlid o si el diccionari és buit.
    """
    if not isinstance(diccionaris, list) or not diccionaris:
        return None  # Retornar None si l'entrada no és una llista vàlida o està buida

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
def extreure_valors_binaritzants(data: pd.DataFrame, nom_columna: str, clau: str, nova_columna: str) -> pd.DataFrame:
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

    # Transformar els valors 'SI' o 'S' en 1 i 'NO' o 'N' en 0
    data[nova_columna] = data[nova_columna].apply(
        lambda x: 1 if x == 'SI' or x == 'S' else (0 if x == 'NO' or x == 'N' else x))

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


# Funció que retorna els valors de les claus introduïdes, tenint en compte que no utilitza les claus com a tal sino el
# seu contingut (la clau es 'name' pero classifica pel que contigui aquesta clau, no per la clau en si)
def obtenir_valors_lab(data: pd.DataFrame, nom_columna: str, paraula_clau: str, nova_columna: str) -> pd.DataFrame:
    """
    Funció per obtenir el valor associat a la paraula clau a la columna de diccionaris i emmagatzemar-ho en una nova columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - paraula_clau: Paraula clau a buscar dins de la columna 'name' dels diccionaris.
        - nova_columna: Nom de la nova columna on s'emmagatzemarà els valors obtinguts.

    Retorna:
        - DataFrame amb la nova columna afegida que conté els valors obtinguts.
    """
    # Llista per emmagatzemar els valors trobats
    valors_trobats = []

    # Iterar sobre cada fila de la columna de diccionaris
    for estructura in data[nom_columna]:
        valor_trobat = None

        # Verificar si l'estructura és una llista de diccionaris no buida
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


# Funció que retorna els valors que conté la clau que introdueixis, mitjançant l'ús de la clau_interés, el valor de la
# qual conté el resultat. No és aplicable a la columna de labs, ja que aquesta té un format diferent i la clau no conté
# el valor sinó que hi ha les claus 'name' i 'value' que contenen respectivament el nom de la prova i el resultat, per
# tant, no es pot aplicar aquesta funció.
def obtenir_valors_clau_interes(data: pd.DataFrame, nom_columna: str, clau_interes: str,
                                nova_columna: str) -> pd.DataFrame:
    """
    Funció per extreure els valors de la clau d'interès d'una llista de diccionaris en una columna.

    Paràmetres:
        - data: DataFrame de pandas que conté les dades.
        - nom_columna: Nom de la columna que conté la llista de diccionaris.
        - clau_interes: Clau d'interès per filtrar l'extracció de valors.
        - nova_columna: Nom per la nova columna que contindrà els valors extrets.

    Retorna:
        - DataFrame amb una nova columna que conté els valors de la clau d'interès.
    """
    # Funció per extreure els valors de la clau d'interès per a cada element de la columna
    valors_extrets = []
    for estructura in data[nom_columna]:
        if isinstance(estructura, (dict, list)) and estructura:
            valor_extret = extract_value(estructura, clau_interes)
            valors_extrets.append(valor_extret)
        else:
            # Manejar el caso de filas vacías
            if pd.isna(estructura) or estructura == '':
                valors_extrets.append(None)
            else:
                valors_extrets.append(estructura)

    # Afegir els valors extrets com una nova columna al DataFrame original
    data[nova_columna] = valors_extrets

    return data


def extract_value(d, key):
    if isinstance(d, dict):
        if key in d:
            return d[key]
        else:
            for v in d.values():
                result = extract_value(v, key)
                if result is not None:
                    return result
    elif isinstance(d, list):
        for item in d:
            result = extract_value(item, key)
            if result is not None:
                return result
    return None


# Funció per calcular el valor de l'índex de Charlson per a un pacient, tenint en compte que cada codi representa un
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


# Funció per generar les columnes dels tests MNA, EMINA, Barthel i Canadenca
def columnes_tests_categorics(df):
    """Categoritza resultats i afegeix nous resultats amb valors categòrics al DataFrame."""
    df = categoritzar_barthel(df)
    df = categoritzar_mna(df)
    df = categoritzar_emina(df)
    df = categoritzar_canadenca(df)
    return df


def categoritzar_canadenca(df):
    """Categoritza l'escala neurològica canadenca en 3 categories."""
    df['Canadenca resultats'] = pd.to_numeric(df['Canadenca resultats'], errors='coerce')
    conditions = [
        (df['Canadenca resultats'] > 10) & (df['Canadenca resultats'] <= 11.5),
        (df['Canadenca resultats'] >= 5) & (df['Canadenca resultats'] <= 10),
        (df['Canadenca resultats'] <= 4.5)
    ]
    choices = ['Dèficit neurològic lleu', 'Dèficit neurologic moderat', 'Dèficit neurològic sever']
    df['Canadenca categòrica'] = np.select(conditions, choices, default='Desconegut')
    return df


def categoritzar_barthel(df):
    """Categoritza l'índex de Barthel en 4 categories."""
    df['Barthel resultats'] = pd.to_numeric(df['Barthel resultats'], errors='coerce')
    conditions = [
        (df['Barthel resultats'] > 95),
        (df['Barthel resultats'] > 60) & (df['Barthel resultats'] <= 95),
        (df['Barthel resultats'] > 21) & (df['Barthel resultats'] <= 60),
        (df['Barthel resultats'] <= 20)
    ]
    choices = ['Independent', 'Dependència moderada', 'Dependència severa', 'Dependència total']
    df['Barthel categòric'] = np.select(conditions, choices, default='Desconegut')
    return df


def categoritzar_mna(df):
    """Categoritza l'MNA en 3 categories."""
    df['MNA resultats'] = pd.to_numeric(df['MNA resultats'], errors='coerce')
    conditions = [
        (df['MNA resultats'] >= 24),
        (df['MNA resultats'] >= 17) & (df['MNA resultats'] <= 23.5),
        (df['MNA resultats'] < 17)
    ]
    choices = ['Estat nutricional normal', 'Risc de malnutrició', 'Malnodrit']
    df['MNA categòric'] = np.select(conditions, choices, default='Desconegut')
    return df


def categoritzar_emina(df):
    """Categoritza l'EMINA en 3 categories."""
    df['EMINA resultats'] = pd.to_numeric(df['EMINA resultats'], errors='coerce')
    conditions = [
        (df['EMINA resultats'] <= 5),
        (df['EMINA resultats'] >= 6) & (df['EMINA resultats'] <= 10),
        (df['EMINA resultats'] >= 11) & (df['EMINA resultats'] <= 15)
    ]
    choices = ['Risc baix', 'Risc moderat', 'Risc alt']
    df['EMINA categòric'] = np.select(conditions, choices, default='Desconegut')
    return df


# Funció per generar un plot dels p-valors per variables contínues, un cop s'ha comprovat la normalitat i s'ha fet un
# T-test o Mann-Whitney segons correspongui
def test_indepe_plot(grups: dict, alpha=0.05):
    """
    Compara grups utilitzant proves t-test o Mann-Whitney U segons la normalitat de les dades.

    Paràmetres:
    grupos (dict): Un diccionari on les claus són els noms dels grups
                   i els valors són llistes d'observacions per a cada grup.
    alpha (float): Nivell de significància pel test de Shapiro-Wilk. Per defecte és 0.05.

    Retorna:
    None
    """

    def plotejar_matriu(matriu, noms_grups, titol):
        """
        Genera un gràfico d'hemi-matriu superior amb els p-valors proporcionats.

        Paràmetres:
        matriu (np.array): Matriu de p-valors a representar.
        noms_grups (list): Llista de noms dels grups.
        titol (str): Títol del gràfic.

        Retorna:
        None
        """
        fig, ax = plt.subplots()
        cax = ax.matshow(matriu, cmap='viridis')

        for (i, j), val in np.ndenumerate(matriu):
            if i >= j:  # Només mostrar la meitat superior i la diagonal
                if np.isnan(val):
                    ax.text(j, i, 'nan', ha='center', va='center', color='black')
                else:
                    color = 'white' if val < 0.05 else 'black'
                    ax.text(j, i, f'{val:.4f}', ha='center', va='center', color=color)
        # Configuració del color de fons per fer transparent la meitat superior
        ax.set_facecolor((0, 0, 0, 0.5))

        plt.colorbar(cax)
        ax.set_xticks(np.arange(len(noms_grups)))
        ax.set_yticks(np.arange(len(noms_grups)))
        ax.set_xticklabels(noms_grups, rotation=45, ha='left')
        ax.set_yticklabels(noms_grups)
        plt.xlabel('Grups')
        plt.ylabel('Grups')
        plt.title(titol)

        # Ajustar la matriu per només mostrar la meitat superior i la diagonal
        ax.set_xlim(-0.5, len(noms_grups) - 0.5)
        ax.set_ylim(len(noms_grups) - 0.5, -0.5)

        plt.show()

    noms_grups = list(grups.keys())
    num_grups = len(noms_grups)
    matriu_pvalors = np.zeros((num_grups, num_grups))

    for i, (nom_grup1, dades_grup1) in enumerate(grups.items()):
        for j, (nom_grup2, dades_grup2) in enumerate(grups.items()):
            if i >= j:  # Només calcular per la meitat superior i la diagonal
                # Convertir les dades a sèrie de pandas i intentar de convertir a float
                dades_grup1 = pd.to_numeric(pd.Series(dades_grup1), errors='coerce').dropna()
                dades_grup2 = pd.to_numeric(pd.Series(dades_grup2), errors='coerce').dropna()

                # Realitza el test de Shapiro-Wilk per comprovar la normalitat d'ambdós grups
                if len(dades_grup1) > 0 and len(dades_grup2) > 0:
                    _, p_valor_shapiro1 = shapiro(dades_grup1)
                    _, p_valor_shapiro2 = shapiro(dades_grup2)

                    if p_valor_shapiro1 > alpha and p_valor_shapiro2 > alpha:  # Si ambdós grups són normals
                        stat, p_valor = ttest_ind(dades_grup1, dades_grup2)
                    else:  # Si almenys un dels grups no és normal
                        stat, p_valor = mannwhitneyu(dades_grup1, dades_grup2, alternative='two-sided')

                    matriu_pvalors[i, j] = p_valor
                else:
                    matriu_pvalors[i, j] = np.nan  # Assignar NaN si alguna de les sèries està buïda

    plotejar_matriu(matriu_pvalors, noms_grups, 'P-valors de les Comparacions dels Grups')


#####
# Funció per fer el plot del p-valor de les variables binàries (0 o 1)
def test_indepe_bin_plot(groups: dict, filter_func=None):
    """
    Realiza el test de chi-cuadrado para comparar variables dicotómicas.

    Parámetros:
    groups (dict): Un diccionario donde las claves son los nombres de los grupos
                   y los valores son listas de observaciones dicotómicas (0 o 1) o
                   variables categóricas (F/M, por ejemplo) para cada grupo.
    filter_func (function): Una función opcional para filtrar los datos de cada grupo.

    Retorna:
    None
    """

    def plot_matrix(matrix, group_names, title, filter_func):
        """
        Genera un gráfico de hemi-matriz superior con los p-valores proporcionados.

        Parámetros:
        matrix (np.array): Matriz de p-valores a representar.
        group_names (list): Lista de nombres de los grupos.
        title (str): Título del gráfico.
        filter_func (function): Una función para filtrar los datos, opcional.

        Retorna:
        None
        """
        fig, ax = plt.subplots()
        cax = ax.matshow(matrix, cmap='magma')

        for i in range(len(group_names)):
            for j in range(len(group_names)):
                if i >= j:
                    val = matrix[i, j]
                    if np.isnan(val):
                        ax.text(j, i, 'nan', ha='center', va='center', color='black')
                    else:
                        color = 'white' if val < 0.05 else 'black'
                        ax.text(j, i, f'{val:.4f}', ha='center', va='center', color=color)

        ax.set_facecolor((0, 0, 0, 0.5))

        plt.colorbar(cax)
        ax.set_xticks(np.arange(len(group_names)))
        ax.set_yticks(np.arange(len(group_names)))
        ax.set_xticklabels(group_names, rotation=45, ha='left')
        ax.set_yticklabels(group_names)
        plt.xlabel('Grupos')
        plt.ylabel('Grupos')
        plt.title(title)

        ax.set_xlim(-0.5, len(group_names) - 0.5)
        ax.set_ylim(len(group_names) - 0.5, -0.5)

        plt.show()

    group_names = list(groups.keys())
    num_groups = len(group_names)
    p_values_matrix = np.zeros((num_groups, num_groups))

    for i, (name1, data1) in enumerate(groups.items()):
        for j, (name2, data2) in enumerate(groups.items()):
            if i >= j:
                # Aplicar función de filtrado si se proporciona
                data1 = list(filter(filter_func, data1))
                data2 = list(filter(filter_func, data2))

                # Validar si las listas no están vacías
                if len(data1) == 0 or len(data2) == 0:
                    p_values_matrix[i, j] = np.nan
                    continue

                # Construir la tabla de contingencia
                count_00 = sum((x == 0 and y == 0) for x, y in zip(data1, data2) if x is not None and y is not None)
                count_01 = sum((x == 0 and y == 1) for x, y in zip(data1, data2) if x is not None and y is not None)
                count_10 = sum((x == 1 and y == 0) for x, y in zip(data1, data2) if x is not None and y is not None)
                count_11 = sum((x == 1 and y == 1) for x, y in zip(data1, data2) if x is not None and y is not None)

                print(f"Table for {name1} and {name2}:")
                print(f"{count_00} | {count_01}")
                print(f"{count_10} | {count_11}")

                contingency_table = np.array([[count_00, count_01], [count_10, count_11]])

                print(f"Contingency Table for {name1} and {name2}:")
                print(contingency_table)

                # Validar que la tabla de contingencia tenga valores suficientes para realizar el test
                if contingency_table.sum() == 0 or (contingency_table < 5).sum() > 0:
                    p_values_matrix[i, j] = np.nan
                    continue

                # Calcular el test de chi-cuadrado
                _, p_value, _, _ = chi2_contingency(contingency_table)

                p_values_matrix[i, j] = p_value

    plot_matrix(p_values_matrix, group_names, 'P-valors de les Comparacions de Grups', filter_func)


######

# Funció per calcular la mitjana i la desviació estàndard
def mitjana_i_std_num(df, columnes: list):
    """
    Calcula la mitjana i la desviació estàndard per a cada columna especificada en una llista.

    Paràmetres:
    df (DataFrame): El DataFrame que conté les dades.
    columnes (list): Llista de noms de columnes a analitzar.

    Retorna:
    None
    """
    for col in columnes:
        if col in columnes:
            try:
                # Intenta convertir la columna a tipus numèric
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')  # Els valors no convertibles s'establiran
                # com a Nan
            except ValueError as e:
                print(f"Error convertint la columna '{col}' a tipus numèric: {str(e)}")
                continue  # Salta a la següent columna si hi ha un error de conversió

            # Calcula la mitjana i la desviació estàndard
            mean = df[col].mean()
            std = df[col].std()

            # Imprimeix els resultats
            print(f"Valors de la columna {col}:")
            print(f"Mitjana: {mean:.2f}")
            print(f"Desviació estàndard: {std:.2f}")
            print("\n")
        else:
            print(f"Columna '{col}' absent en el DataFrame.")


# Funció per realitzar el compteig de variables categòriques i el seu percentatge
def comptatge_i_percentatge_cat(df, columnes):
    """
    Calcula el compteig i percentatge de variables per a cada columna especificada en una llista.

    Paràmetres:
    df (DataFrame): El DataFrame que conté les dades.
    columnes (list): Llista de noms de columnes a analitzar

    Retorna:
    None
    """
    for col in columnes:
        if col in df.columns:
            # Genera una còpia independent del DataFrame original, per evitar l'error "SettingWithCopyWarning"
            df_copy = df.copy()

            # Si la columna és 'Creatinine', la tracta com si fos categòrica
            if col == 'Creatinine':
                # Converteix els valors numèrics en 0 o 1 segons siguin menors o majors de 1.5
                df_copy.loc[:, 'Malaltia renal crònica'] = (pd.to_numeric(df[col], errors='coerce') > 1.5).astype(int)
                col = 'Malaltia renal crònica'  # Canviem el nom de la columna Creatinine a Malaltia renal crònica

            # Calcula el compteig i percentatge de les variables
            counts = df_copy[col].value_counts()
            percentages = df_copy[col].value_counts(normalize=True) * 100

            # Formateja els percentatges per agregar el símbol '%' després del resultat
            percentages_formatted = percentages.map("{:.2f}%".format)

            # Crea un DataFrame pels resultats
            Resultats = pd.DataFrame({
                'Comptatges': counts,
                'Percentatges': percentages_formatted
            })

            # Imprimeix els resultats en forma de taula
            print(f"Resum per la columna {col}:\n")
            print(tabulate(Resultats, headers='keys', tablefmt='psql'))
            print("\n")

        else:
            print(f"Columna '{col}' absent al DataFrame.")

## APUNTES
# Los que tienen PA vs los que creemos que la tienen vs los que no. X fenotipo
# pes es llista de diccionarios []
# float num enteros, int para decimales con punto
# los tests (mna, emina, barthel...) tienen 2 "categorias":
# - el total que es numerico (mean+-sd) --> ttest/mannwhit
# - los diferentes intervalos que son categorico (num total/contaje) --> xi
# Filtrar valores entre 10 y 15
# filter_func = lambda x: 10 <= x <= 15
