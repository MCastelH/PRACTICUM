import json

import pandas as pd
from auxiliary_functions import (obtenir_data_presencia_codi, restar_dates, codis_ICD, nombre_ingressos,
                                 dies_ingressat_total, binaritzar_15_creatinina, sumar_i_comparar,
                                 obtenir_pes_o_mitjana, disfagia_mecvvs, sumar_barthel, obtenir_valors_lab,
                                 extreure_valors_binaritzants, obtenir_valors_clau_interes, index_charlson,
                                 obtenir_pes_mes_antic, obtenir_pes_mes_nou, obtenir_data_pes_mes_antic,
                                 obtenir_primera_data_mecvv, obtenir_pes_coincident_mecvv, restar_columnes_object)

from listas import (PA_list, P_list, disfagia_list, Main_respiratory_infections_list, LRTI_list,
                    COPD_exacerbations_list,
                    Pulmonary_fibrosis_fibrotorax_list, priorfalls_list, delirium_list, dementia_list, depresyndr_list,
                    uriincont_list, fecincont_list, pressulc_list, immob_list, conf_list, osteopor_list,
                    sarcopenia_list,
                    sleepdisturb_list, chrpain_list, iatrog_list, constipation_list, CVdisease_list, heartdisease_list,
                    ND_list, DM_list, hepatopat_list, neopl_list, AcuteRenalF_list, dizsyn_list, VIH_list,
                    psicosis_list, nutridef_list, charlson_dict)

if __name__ == "__main__":
    with open('./data/origin/bbdd_pneumonia_aspirativa.json') as arxiu:
        dades = json.load(arxiu)

    data = pd.DataFrame(dades)

    # Funció que verifica a la clau 'codiDiagnostics', de la llista de diccionaris 'ingressos', si hi ha algun codi
    # de la llista introduïda, i retorna 1 o 0 si l'ha trobat o no
    data = codis_ICD(data, PA_list, 'PA diagnosticada')
    data = codis_ICD(data, disfagia_list, 'DO diagnosticada')
    data = codis_ICD(data, P_list, 'P diagnosticada')
    data = codis_ICD(data, Main_respiratory_infections_list,
                     'Infeccions respiratòries principals diagnosticades')
    data = codis_ICD(data, LRTI_list, 'LRTI diagnosticada')
    data = codis_ICD(data, COPD_exacerbations_list, 'Exacerbacions de COPD diagnosticades')
    data = codis_ICD(data, Pulmonary_fibrosis_fibrotorax_list,
                     'Fibrosi pulmonar i fibrotòrax diagnosticades')

    data = codis_ICD(data, priorfalls_list, 'Caigudes prèvies')
    data = codis_ICD(data, delirium_list, 'Deliris')
    data = codis_ICD(data, dementia_list, 'Demència')
    data = codis_ICD(data, depresyndr_list, 'Síndrome depressiu')
    data = codis_ICD(data, uriincont_list, 'Incont.uri')
    data = codis_ICD(data, fecincont_list, 'Incont.fec')
    data = codis_ICD(data, pressulc_list, 'Úlceres pressió')
    data = codis_ICD(data, immob_list, 'Immobilitat')
    data = codis_ICD(data, conf_list, 'Confusió')
    data = codis_ICD(data, osteopor_list, 'Osteoporosi')
    data = codis_ICD(data, sarcopenia_list, 'Sarcopènia')
    data = codis_ICD(data, sleepdisturb_list, 'Probl.son')
    data = codis_ICD(data, chrpain_list, 'Dolor crònic')
    data = codis_ICD(data, iatrog_list, 'Iatrogènic')
    data = codis_ICD(data, constipation_list, 'Restrenyiment')

    data = codis_ICD(data, CVdisease_list, 'CV')
    data = codis_ICD(data, heartdisease_list, 'Probl.cor')
    data = codis_ICD(data, ND_list, 'Neurodegeneratives')
    data = codis_ICD(data, DM_list, 'DM')
    data = codis_ICD(data, hepatopat_list, 'Hepatopaties')
    data = codis_ICD(data, neopl_list, 'Neoplàsies')
    data = codis_ICD(data, AcuteRenalF_list, 'ARF')
    data = codis_ICD(data, dizsyn_list, 'Marejos')
    data = codis_ICD(data, VIH_list, 'VIH')
    data = codis_ICD(data, psicosis_list, 'Psicosi')
    data = codis_ICD(data, nutridef_list, 'Def.nutri')

    # Funció per calcular els índexs de Charlson de cada pacient, a partir del diccionari 'charlson_dict'
    # que conté els codis amb els valors corresponents
    data = index_charlson(data, 'ingressos', 'Charlson', charlson_dict)

    # Funció que indica quants cops ha ingressat el pacient. Aixó ho fa comptant el nombre de diccionaris que hi ha a la
    # columna 'ingressos'
    data = nombre_ingressos(data, 'ingressos', 'Nombre ingressos')

    # Funció que retorna la suma dels dies totals que ha estat ingressat el pacient, basant-se en fer una suma amb el
    # resultat de la resta de les claus 'dataAlta' i 'dataIngres' (que es troben a la columna 'ingressos'), i creant la
    # nova columna 'Dias_totals_ingressat'
    data = dies_ingressat_total(data, 'ingressos', 'Dies totals ingressat')

    # Funció que realitza un sumatori de tots els ítems del test de Barthel, sense tenir en compte l'última clau 'data'
    data = sumar_barthel(data, 'barthel', 'Barthel resultats')

    # Funció que fa un sumatori de tots els elements de la prova EMINA, sense tenir en compte les dues darreres claus
    # 'dataValoracio' i 'resultat'. A més, compara si la suma és igual al valor de la columna 'resultat' i, si és així,
    # retorna la suma. Si la llista està buida (longitud=0) o els valors de la suma i 'resultat' no són els mateixos,
    # retorna NaN
    data = sumar_i_comparar(data, 'emina', ['dataValoracio', 'resultat'], 'resultat',
                            'EMINA sumatori comparat')

    # Funció que extreu el resultat l'última clau del test emina anomenada 'resultats'
    data = obtenir_valors_clau_interes(data, 'emina', 'resultat', 'EMINA resultats')

    # Funció que extreu el resultat de l'última clau del test mna anomenada 'resultats'
    data = obtenir_valors_clau_interes(data, 'mna', 'resultat', 'MNA resultats')


    # Funció que proporciona la mitjana de tots els pesos (en el cas que hi hagi més d'un valor de pes) o l'únic valor
    # de pes que es disposi del pacient
    data = obtenir_pes_o_mitjana(data, 'pes', 'Mitjana pes')

    # Funció que compara la suma de certs ítems de l'escala canadenca amb la clau 'total', i si són iguals, retorna
    # la suma. Per fer la suma, no considera les claus: 'total', 'dataValoracio' i 'horaValoracio'. Si la fila està
    # buida (no hi ha diccionari), retorna NaN.
    data = sumar_i_comparar(data, 'canadenca', ['total', 'dataValoracio', 'horaValoracio'],
                            'total', 'Canadenca sumatori comparat')

    # Funció que itera fins a trobar l'última vegada que van aparèixer les claus 'disfàgia' o 'disfàgiaConeguda' a la
    # llista de diccionaris 'mecvvs' i que retorna 1 o 0 si el pacient té respectivament un sí o un no en aquestes claus
    data = disfagia_mecvvs(data, 'mecvvs', 'Disfàgia MECVV')

    # Funció que itera fins trobar l'últim diccionari amb la clau d'interès que retorna els seus valors. En aquest cas, essent
    # aquests 1 si és SÍ, i 0 si és NO.
    data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioEficacia',
                                        'Alteració eficàcia MECVV')
    data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioSeguretat',
                                        'Alteració seguretat MECVV')

    # Funció que permet obtenir de la llista de diccionaris 'labs' el valor de les diferents proves realitzades
    # mitjançant l'ús del paràmetre 'clau_interes'. Aquest permet extreure el valor present en la clau 'value' del
    # diccionari que contingui la 'clau_interes'
    data = obtenir_valors_lab(data, 'labs', 'ALBÚMINA Sèrum', 'Albúmina')

    data = obtenir_valors_lab(data, 'labs', 'PROTEÏNES TOTALS Sèrum',
                              'Proteïnes totals')

    data = obtenir_valors_lab(data, 'labs', 'HEMOGLOBINA',
                              'Hb')

    data = obtenir_valors_lab(data, 'labs', 'COLESTEROL Sèrum',
                              'Colesterol total')

    data = obtenir_valors_lab(data, 'labs', 'LEUCÒCITS',
                              'Leucos')

    data = obtenir_valors_lab(data, 'labs', 'LIMFÒCITS %',
                              'Limfos')

    data = obtenir_valors_lab(data, 'labs', 'PROTEÏNA C REACTIVA Sèrum',
                              'Prot C react')

    data = obtenir_valors_lab(data, 'labs', 'UREA Sèrum',
                              'Urea')

    data = obtenir_valors_lab(data, 'labs', 'F. G. ESTIMAT (MDRD) Sèrum',
                              'FGE MDRD')

    data = obtenir_valors_lab(data, 'labs', 'F. G. ESTIMAT (CKD-EPI) Sèrum',
                              'FGE CDK-EPI')

    data = obtenir_valors_lab(data, 'labs', 'CREATININA Sèrum',
                              'Creatinina')

    # Funció per binaritzar els valors de la columna 'Creatinina', sent 1 si el valor era >1.5 i 0 si el valor era <1.5
    data = binaritzar_15_creatinina(data, 'Creatinina', 'Creatinina >1.5 binària')

    # Funció que retorna el pes més antic registrat de la columna 'pes'
    data = obtenir_pes_mes_antic(data, 'Pes més antic')

    # Funció que retorna el pes més actual
    data = obtenir_pes_mes_nou(data, 'Pes més nou')

    # Funció que retorna la data que correspon al pes més antic
    data = obtenir_data_pes_mes_antic(data, 'Data pes més antic')

    # Funció que retorna la data en la qual el MECVV va donar positiu (disfàgia + alteració seguretat o eficàcia)
    # per primer cop
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
