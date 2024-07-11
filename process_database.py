import json
import pandas as pd
import logging
from tqdm import tqdm
from auxiliary_functions import (obtenir_data_presencia_codi, restar_dates, codis_icd, nombre_ingressos,
                                 dies_ingressat_total, obtenir_pes_o_mitjana, disfagia_mecvvs, sumar_barthel,
                                 obtenir_valors_lab, extreure_valors_binaritzants, obtenir_valors_clau_interes,
                                 index_charlson, obtenir_pes_mes_antic, obtenir_pes_mes_nou, obtenir_data_pes_mes_antic,
                                 obtenir_primera_data_mecvv, obtenir_pes_coincident_mecvv, restar_columnes_object,
                                 columnes_tests_categorics, categoritzar_perdua_pes, segmentacio_bd)
from listas import (P_list, charlson_dict, pathology_dict, laboratoris_dict)

# Configuració del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logging.info("Inici del processament de l'script.")

    # Carregar l'arxiu json amb les dades
    try:
        with open('data/origin/pacientsPneumoniaAspirativaTotal.json', encoding='utf-8') as arxiu:
            dades_raw = json.load(arxiu)
        logging.info("Arxiu carregat correctament.")
    except Exception as e:
        logging.error(f"Error en carregar l'arxiu: {e}")
        raise

    try:
        data = pd.DataFrame(dades_raw)
        logging.info("DataFrame creat amb èxit.")

        # Filtrar per edat >= 65
        data = data[data['edat'] >= 65]
        logging.info(f"Dades filtrades per edat: {len(data)} registres trobats.")

        # Modificar la columna del sexe
        data['sexe'] = data['sexe'].map({'F': 0, 'M': 1})
        data.rename(columns={'sexe': 'sexe M'}, inplace=True)

        # Construir una columna per cada patologia.
        # Visualitzar el progrés de la construcció de les columnes.
        logging.info("Construint columnes per a cada patologia.")
        # Ús de la llibreria tqdm per a visualitzar el progrés de la construcció de les columnes.
        for key, value in tqdm(pathology_dict.items()):
            data = codis_icd(data, value, key)

        # Calcular índexs de Charlson
        logging.info("Calculant índexs de Charlson.")
        data = index_charlson(data, 'ingressos', 'Charlson', charlson_dict)

        # Comptar ingressos
        logging.info("Comptant ingressos.")
        data = nombre_ingressos(data, 'ingressos', 'Admissions', 'Emergències')

        # Suma dels dies ingressat
        logging.info("Sumant dies ingressat.")
        data = dies_ingressat_total(data, 'ingressos', 'Dies totals ingressat')

        # Suma dels ítems del test de Barthel
        logging.info("Sumant ítems del test de Barthel.")
        data = sumar_barthel(data, 'barthel', 'Barthel resultats')

        # Funcions d'extracció i obtenció de dades
        logging.info("Extracció de dades d'interès.")
        data = obtenir_valors_clau_interes(data, 'emina', 'resultat',
                                           'EMINA resultats')
        data = obtenir_valors_clau_interes(data, 'mna', 'resultat',
                                           'MNA resultats')
        data = obtenir_valors_clau_interes(data, 'canadenca', 'total',
                                           'Canadenca resultats')
        data = obtenir_pes_o_mitjana(data, 'pes', 'Mitjana pes')
        data = disfagia_mecvvs(data, 'mecvvs', 'Disfàgia MECV-V')
        data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioEficacia',
                                            'Alteració eficàcia MECV-V')
        data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioSeguretat',
                                            'Alteració seguretat MECV-V')

        logging.info("Extracció de dades de laboratoris.")
        for key, value in tqdm(laboratoris_dict.items()):
            data = obtenir_valors_lab(data, 'labs', value, key)

        # Construir columna especial de laboratoris: malaltia renal crònica
        data['Creatinina'] = pd.to_numeric(data['Creatinina'], errors='coerce')
        data['Malaltia renal crònica'] = (data['Creatinina'] > 1.5).astype(int)

        # Pes més antic i més nou
        logging.info("Càlcul de pes més antic i més nou.")
        data = obtenir_pes_mes_antic(data, 'Pes més antic')
        data = obtenir_pes_mes_nou(data, 'Pes més nou')

        # Dates relacionades amb el pes i primer diagnòstic de MECV-V
        logging.info("Càlcul de dates relacionades amb el pes i el primer diagnòstic de MECV-V.")
        data = obtenir_data_pes_mes_antic(data, 'Data pes més antic')
        data = obtenir_primera_data_mecvv(data, 'Data primer MECV-V')
        data = obtenir_pes_coincident_mecvv(data, 'Pes coincident primer MECV-V')

        # Càlcul de pèrdua de pes
        logging.info("Càlcul de pèrdua de pes.")
        data = restar_columnes_object(data, 'Pes més antic', 'Pes coincident primer MECV-V',
                                      'Pèrdua pes entre ingressos')
        data = restar_columnes_object(data, 'Pes més antic', 'Pes més nou',
                                      'Pèrdua pes total')

        # Data més antiga de diagnòstic de pneumònia
        logging.info("Data més antiga de diagnòstic de pneumònia.")
        data = obtenir_data_presencia_codi(data, P_list, 'Data més antiga pneumònia')

        # Dies entre diagnòstics
        logging.info("Dies entre diagnòstics.")
        data = restar_dates(data, 'Data primer MECV-V', 'Data més antiga pneumònia',
                            'Dies entre primer ICD pneumònia i primer MECV-V positiu')

        # Construir columnes categòriques pels tests EMINA, MNA, Barthel i Canadenca
        logging.info("Construint columnes categòriques pels tests EMINA, MNA, Barthel i Canadenca.")
        data = columnes_tests_categorics(data)

        # Construir columna pèrdua de pes entre ingressos categòrica
        logging.info("Construint columna categòrica per la pèrdua de pes entre ingressos")
        data = categoritzar_perdua_pes(data, 'Pèrdua pes entre ingressos',
                                       'Pèrdua pes entre ingressos categòrica')

        # Crear una columna categòrica amb una categoria per cada condició d'estudi que ens permetrà fer una anàlisi
        # de les condicions que tenen els pacients per cada condició d'estudi.
        data = segmentacio_bd(data)

        # Guardar DataFrame per a posterior ús en JupyterNotebook
        data.to_pickle('./data/processed/dataframe.pkl')
        logging.info("Processament finalitzat i dades guardades.")

    except Exception as e:
        logging.error(f"Error durant el processament de les dades: {e}")
        raise

#%%
