import json
import pandas as pd
import logging

from auxiliary_functions import (obtenir_data_presencia_codi, restar_dates, codis_ICD, nombre_ingressos,
                                 dies_ingressat_total, obtenir_pes_o_mitjana, disfagia_mecvvs, sumar_barthel,
                                 obtenir_valors_lab, extreure_valors_binaritzants, obtenir_valors_clau_interes,
                                 index_charlson, obtenir_pes_mes_antic, obtenir_pes_mes_nou, obtenir_data_pes_mes_antic,
                                 obtenir_primera_data_mecvv, obtenir_pes_coincident_mecvv, restar_columnes_object,
                                 columnes_tests_categorics)
from listas import (P_list, charlson_dict, pathology_dict, laboratoris_dict)

# Configuració del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logging.info("Inici del processament de l'script.")

    # Carregar l'arxiu json amb les dades
    try:
        with open('data/origin/bbdd_pneumonia_aspirativa.json', encoding='utf-8') as arxiu:
            dades_raw = json.load(arxiu)
        logging.info("Arxiu carregat correctament.")
    except Exception as e:
        logging.error(f"Error en carregar l'arxiu: {e}")
        raise

    try:
        data = pd.DataFrame(dades_raw)
        logging.info("DataFrame creat amb èxit.")

        # Filtrar per edat > 65
        data = data[data['edat'] > 65]
        logging.info(f"Dades filtrades per edat: {len(data)} registres trobats.")

        # Construir una columna per cada patologia.
        for key, value in pathology_dict.items():
            data = codis_ICD(data, value, key)

        # Calcular índexs de Charlson
        data = index_charlson(data, 'ingressos', 'Charlson', charlson_dict)

        # Comptar ingressos
        data = nombre_ingressos(data, 'ingressos', 'Admissions', 'Emergències')

        # Suma dels dies ingressat
        data = dies_ingressat_total(data, 'ingressos', 'Dies totals ingressat')

        # Suma dels ítems del test de Barthel
        data = sumar_barthel(data, 'barthel', 'Barthel resultats')

        # Funcions d'extracció i obtenció de dades
        data = obtenir_valors_clau_interes(data, 'emina', 'resultat',
                                           'EMINA resultats')
        data = obtenir_valors_clau_interes(data, 'mna', 'resultat',
                                           'MNA resultats')
        data = obtenir_valors_clau_interes(data, 'canadenca', 'total',
                                           'Canadenca resultats')
        data = obtenir_pes_o_mitjana(data, 'pes', 'Mitjana pes')
        data = disfagia_mecvvs(data, 'mecvvs', 'Disfàgia MECVV')
        data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioEficacia',
                                            'Alteració eficàcia MECVV')
        data = extreure_valors_binaritzants(data, 'mecvvs', 'alteracioSeguretat',
                                            'Alteració seguretat MECVV')

        for key, value in laboratoris_dict.items():
            data = obtenir_valors_lab(data, 'labs', value, key)

        # Pes més antic i més nou
        data = obtenir_pes_mes_antic(data, 'Pes més antic')
        data = obtenir_pes_mes_nou(data, 'Pes més nou')

        # Dates relacionades amb el pes i primer diagnòstic de MECVV
        data = obtenir_data_pes_mes_antic(data, 'Data pes més antic')
        data = obtenir_primera_data_mecvv(data, 'Data primer MECVV')
        data = obtenir_pes_coincident_mecvv(data, 'Pes coincident primer MECVV')

        # Càlcul de pèrdua de pes
        data = restar_columnes_object(data, 'Pes més antic', 'Pes coincident primer MECVV',
                                      'Pèrdua pes entre ingressos')
        data = restar_columnes_object(data, 'Pes més antic', 'Pes més nou',
                                      'Pèrdua pes total')

        # Data més antiga de diagnòstic de pneumònia
        data = obtenir_data_presencia_codi(data, P_list, 'Data més antiga pneumònia')

        # Dies entre diagnòstics
        data = restar_dates(data, 'Data primer MECVV', 'Data més antiga pneumònia',
                            'Dies entre primer ICD pneumònia i primer MECVV positiu')

        # Construir columnes categòriques pels tests EMINA, MNA, Barthel i Canadenca
        data = columnes_tests_categorics(data)

        # Guardar DataFrame per a ús en JupyterNotebook
        data.to_pickle('./data/processed/dataframe.pkl')
        logging.info("Processament finalitzat i dades guardades.")

    except Exception as e:
        logging.error(f"Error durant el processament de les dades: {e}")
        raise
