import json

import pandas as pd

from auxiliary_functions import (obtenir_data_presencia_codi, restar_dates, codis_ICD, nombre_ingressos,
                                 dies_ingressat_total, obtenir_pes_o_mitjana, disfagia_mecvvs, sumar_barthel,
                                 obtenir_valors_lab, extreure_valors_binaritzants, obtenir_valors_clau_interes,
                                 index_charlson, obtenir_pes_mes_antic, obtenir_pes_mes_nou, obtenir_data_pes_mes_antic,
                                 obtenir_primera_data_mecvv, obtenir_pes_coincident_mecvv, restar_columnes_object)
from listas import (P_list, charlson_dict, pathology_dict, laboratoris_dict)

if __name__ == "__main__":

    # Carreguem el fitxer json amb les dades
    with open('data/origin/bbdd_pneumonia_aspirativa.json', encoding='utf-8') as arxiu:
        dades_raw = json.load(arxiu)
    data = pd.DataFrame(dades_raw)

    # Construim una columna per cada patologia.
    for key, value in pathology_dict.items():
        data = codis_ICD(data, value, key)

    # Funció per calcular els índexs de Charlson de cada pacient, a partir del diccionari 'charlson_dict'
    # que conté els codis amb els valors corresponents.
    data = index_charlson(data, 'ingressos', 'Charlson', charlson_dict)

    # Funció que indica quants cops ha ingressat el pacient. Aixó ho fa comptant el nombre de diccionaris que hi ha a la
    # columna 'ingressos'
    data = nombre_ingressos(data, 'ingressos', 'Admissions', 'Emergencies')

    # Funció que retorna la suma dels dies totals que ha estat ingressat el pacient, basant-se en fer una suma amb el
    # resultat de la resta de les claus 'dataAlta' i 'dataIngres' (que es troben a la columna 'ingressos'), i creant la
    # nova columna 'Dias_totals_ingressat'
    data = dies_ingressat_total(data, 'ingressos', 'Dies totals ingressat')

    # Funció que realitza un sumatori de tots els ítems del test de Barthel, sense tenir en compte l'última clau 'data'
    data = sumar_barthel(data, 'barthel', 'Barthel resultats')

    # Funció que extreu el resultat l'última clau del test emina anomenada 'resultats'
    data = obtenir_valors_clau_interes(data, 'emina', 'resultat', 'EMINA resultats')

    # Funció que extreu el resultat de l'última clau del test mna anomenada 'resultats'
    data = obtenir_valors_clau_interes(data, 'mna', 'resultat', 'MNA resultats')

    # Funció que extreu el resultat de l'última clau del test canadenca anomenada 'total'
    data = obtenir_valors_clau_interes(data, 'canadenca', 'total', 'Canadenca resultats')

    # Funció que proporciona la mitjana de tots els pesos (en el cas que hi hagi més d'un valor de pes) o l'únic valor
    # de pes que es disposi del pacient.
    data = obtenir_pes_o_mitjana(data, 'pes', 'Mitjana pes')

    # Funció que itera fins a trobar l'última vegada que van aparèixer les claus 'disfàgia' o 'disfàgiaConeguda' a la
    # llista de diccionaris 'mecvvs' i que retorna 1 o 0 si el pacient té respectivament un sí o un no en aquestes claus
    data = disfagia_mecvvs(data, 'mecvvs', 'Disfàgia MECVV')

    # Funció que itera fins a trobar l'últim diccionari amb la clau d'interès que retorna els seus valors. En aquest
    # cas, essent aquests 1 si és SÍ, i 0 si és NO.
    data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioEficacia',
                                        'Alteració eficàcia MECVV')
    data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioSeguretat',
                                        'Alteració seguretat MECVV')

    # Funció que permet obtenir de la llista de diccionaris 'labs' el valor de les diferents proves realitzades
    # mitjançant l'ús del paràmetre 'clau_interes'. Aquest permet extreure el valor present en la clau 'value' del
    # diccionari que contingui la 'clau_interes'
    for key, value in laboratoris_dict.items():
        data = obtenir_valors_lab(data, 'labs', value, key)

    # Funció que retorna el pes més antic registrat de la columna 'pes'
    data = obtenir_pes_mes_antic(data, 'Pes més antic')

    # Funció que retorna el pes més actual
    data = obtenir_pes_mes_nou(data, 'Pes més nou')

    # Funció que retorna la data que correspon al pes més antic
    data = obtenir_data_pes_mes_antic(data, 'Data pes més antic')

    # Funció que retorna la data en la qual el MECVV va donar positiu (disfàgia + alteració seguretat o eficàcia)
    # per primer cop.
    data = obtenir_primera_data_mecvv(data, 'Data primer MECVV')

    # Funció que retorna el pes si la seva data es troba en un rang de 3 dies abans o després de la data que hi ha a la
    # columna 'Data primer MECVV'
    data = obtenir_pes_coincident_mecvv(data, 'Pes coincident primer MECVV')

    # Funció que obté la pèrdua de pes en restar la columna amb el pes més antic ('pes més antic') menys el pes en el
    # qual aproximadament el MECVV va donar positiu ('pes coindicent primer mecvv')
    data = restar_columnes_object(data, 'Pes més antic', 'Pes coincident primer MECVV',
                                  'Pèrdua pes entre ingressos')

    # Funció que obté la pèrdua de pes total en restar les columnes 'pes més antic' menys 'pes més nou'
    data = restar_columnes_object(data, 'Pes més antic', 'Pes més nou', 'Pèrdua pes total')

    # Funció que retorna la data més antiga de totes les vegades que han diagnosticat un codi de pneumònia
    data = obtenir_data_presencia_codi(data, P_list, 'Data més antiga pneumònia')

    # Funció per saber els dies de diferència que hi ha entre 2 columnes. En aquest cas, basant-se en la resta de les
    # columnes 'Data primer MECVV' i 'Data més antiga pneumònia'
    data = restar_dates(data, 'Data primer MECVV', 'Data més antiga pneumònia',
                        'Dies entre primer ICD pneumònia i primer MECVV positiu')

    # Dataframe per utilitzar en els documents JupyterNotebook
    data.to_pickle('./data/processed/dataframe.pkl')
