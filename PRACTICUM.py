# usar diccionarios mejor envez de convertir a df

import json
import pandas as pd
from auxiliary_functions import (valores_codigos, contar_diccionarios, dias_ingreso_total, asignar_intervalo_edad,
                                 sumar_barthel, sumar_emina, obtener_ultimo_resultat, obtener_valor_promedio,
                                 canadenca_comparada, disfagia_mecvvs, extraer_valor_clave,
                                 extraer_valor_clave_simple, extraer_name_value_to_column, cci)
from listas import (PA_list, P_list, disfagia_list, Main_respiratory_infections_list, LRTI_list, COPD_exacerbations_list,
                    Pulmonary_fibrosis_fibrotorax_list, priorfalls_list, delirium_list, dementia_list, depresyndr_list,
                    uriincont_list, fecincont_list, pressulc_list, immob_list, conf_list ,osteopor_list, sarcopenia_list,
                    sleepdisturb_list, chrpain_list, iatrog_list, constipation_list, CVdisease_list, heartdisease_list,
                    ND_list, DM_list, hepatopat_list, neopl_list, AcuteRenalF_list, dizsyn_list, VIH_list, psicosis_list,
                    nutridef_list)



if __name__ == "__main__":
    with open('./data/origin/bbdd_pneumonia_aspirativa.json') as archivo:
        datos = json.load(archivo)

    data = pd.DataFrame(datos)

    # Función que verifica si alguno de los valores a buscar de la lista PA_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista=PA_list, nueva_columna='PA_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista disfagia_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista=disfagia_list, nueva_columna='DO_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista P_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista=P_list, nueva_columna='P_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista Main_respiratory_infections_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista = Main_respiratory_infections_list,
                           nueva_columna= 'Main_respiratory_infections_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista LRTI_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista = LRTI_list, nueva_columna='LRTI_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista COPD_exacerbations_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista= COPD_exacerbations_list, nueva_columna='COPD_exacerbations_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista Pulmonary_fibrosis_fibrotorax_list están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data=data, lista= Pulmonary_fibrosis_fibrotorax_list,
                           nueva_columna= 'Pulmonary_fibrosis_fibrotorax_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de las siguientes listas están en la
    # lista llamada 'codiDiagnostics'
    data = valores_codigos(data, priorfalls_list, 'caidas_previas')
    data = valores_codigos(data, delirium_list, 'delirios')
    data = valores_codigos(data, dementia_list, 'demencia')
    data = valores_codigos(data, depresyndr_list, 'sindrome_depresivo')
    data = valores_codigos(data, uriincont_list, 'incont_uri')
    data = valores_codigos(data, fecincont_list, 'incont_fec')
    data = valores_codigos(data, pressulc_list, 'ulceras_presion')
    data = valores_codigos(data, immob_list, 'immobilitat')
    data = valores_codigos(data, conf_list, 'confusio')
    data = valores_codigos(data, osteopor_list, 'osteoporosis')
    data = valores_codigos(data, sarcopenia_list, 'sarcopenia')
    data = valores_codigos(data, sleepdisturb_list, 'problsueño')
    data = valores_codigos(data, chrpain_list, 'dolor_cron')
    data = valores_codigos(data, iatrog_list, 'iatrogenico')
    data = valores_codigos(data, constipation_list, 'estreñimiento')

    data = valores_codigos(data, CVdisease_list, 'CV')
    data = valores_codigos(data, heartdisease_list, 'probl_corazon')
    data = valores_codigos(data, ND_list, 'neurodegenerativas')
    data = valores_codigos(data, DM_list, 'DM')
    data = valores_codigos(data, hepatopat_list, 'hepatopatias')
    data = valores_codigos(data, neopl_list, 'neoplasias')
    data = valores_codigos(data, AcuteRenalF_list, 'ARF')
    data = valores_codigos(data, dizsyn_list, 'mareos')
    data = valores_codigos(data, VIH_list, 'VIH')
    data = valores_codigos(data, psicosis_list, 'psicosis')
    data = valores_codigos(data, nutridef_list, 'def_nutri')
    # Función para hacer columna de chronic renal disease (creatinina): con esta función obtengo todos los valores del parametro
    # indicado en nombre_interes
    data = extraer_name_value_to_column(data, 'labs', 'CREATININA Sèrum',
                                        'creatinina')

    # Funcion para hacer la columna de Charlson, que al detectar determinados codigos, da un valor u otro, teniendo en
    # cuenta que pueden irse acumulando
    data = cci()

    # Función que indica cuantas veces ha ingresado el paciente, en base a contar el número de diccionarios que hay
    # en 'ingressos', generando la nueva columna Num_ingresos
    data = contar_diccionarios(data, 'ingressos')

    # Función que devuelve un sumatorio de los dias en total que ha estado ingresado el paciente, en base a hacer un
    # sumatorio con el resultado de la resta de las claves 'dataAlta' i 'dataIngres' (situadas en la columna
    # 'ingressos', i generando la nueva columna Dias_totales_ingresado
    data = dias_ingreso_total(data, 'ingressos')

    # Función que clasifica usando un intervalo de 10 en 10 años, a las edades de los pacientes de la columna edat
    data = asignar_intervalo_edad(data, 'edat')

    # Función que hace un sumatorio de todos los ítems del test de Barthel, sin tener en cuenta la última clave 'data'
    data = sumar_barthel(data, 'barthel')

    # Función que hace un sumatorio de todos los ítems del test emina, sin tener en cuenta las últimas 2 claves
    # 'dataValoració' y 'resultat'. Además compara si el sumatorio es el mismo que el valor de la columna 'resultat', y
    # si es así, introduce el sumatorio. Si la lista está vacia (tiene longitud=0) o los valores del sumatorio
    # i 'resultat' no son los mismos, devuelve None (por ahora devuelve NaN)
    data = sumar_emina(data, 'emina', 'EMINA_sumatorios_comparados')

    # Función que extrae la última clave del test emina llamada 'resultats'
    data = obtener_ultimo_resultat(data, 'emina', 'emina_resultats')

    # Función que extrae la última clave del test mna llamada 'resultats'
    data = obtener_ultimo_resultat(data, 'mna', 'mna_resultats')

    # Funcion que proporciona el promedio de los todos los pesos (en caso de que haya más de un valor de peso) o el
    # único valor de peso que tenga el paciente registrado
    data = obtener_valor_promedio(data, 'pes')

    # Función que compara el sumatorio de determinados ítems de la escala canadenca con la clave 'total',
    # y si es igual te devuelve el sumatorio. Para hacer el sumatorio no tiene en cuenta las claves: 'total',
    # 'dataValoracio' y 'horaValoracio'. Si la fila está vacia (no hay diccionario) devuelve NaN.
    data = canadenca_comparada(data, 'canadenca')

    # Función que itera hasta encontrar la última vez que apareció disfagia o disfagia coneguda en la lista de
    # diccionarios mecvvs y que devuelve 1 o 0 si el paciente tiene respectivamente un si o un no en dichas claves
    data = disfagia_mecvvs(data, 'mecvvs')

    # Función que itera hasta encontrar el último diccionario con la clave de interes y que devuelve sus valores
    # sinedo estos: 1 si es SI, y 0 si es NO
    data = extraer_valor_clave(data, 'mecvvs', 'alteracioEficacia',
                               'alteracioEficacia_mecvvs')
    data = extraer_valor_clave(data, 'mecvvs', 'alteracioSeguretat',
                               'alteracioSeguretat_mecvvs')

    # Función que itera hasta encontrar el último diccionario con la clave de interés y devuelve su valor talcual
    data = extraer_valor_clave_simple(data, 'mecvvs', 'viscositat',
                                      'viscositat_mecvvs')

    data = extraer_valor_clave_simple(data, 'mecvvs', 'volum',
                                      'volumn_mecvvs')

    # Función que permite obtener de la lista de diccionarios 'labs', el valor de las diferentes pruebas realizadas,
    # mediante el uso del parametro 'nombre_interes', que permite extraer este valor de interés de la clave requerida
    data = extraer_name_value_to_column(data, 'labs', 'ALBÚMINA Sèrum', 'albumina')

    data = extraer_name_value_to_column(data, 'labs', 'PROTEÏNES TOTALS Sèrum',
                                        'proteinas totales')

    data = extraer_name_value_to_column(data, 'labs', 'HEMOGLOBINA',
                                       'Hb')

    data = extraer_name_value_to_column(data, 'labs', 'COLESTEROL Sèrum',
                                        'colesterol total')

    data = extraer_name_value_to_column(data, 'labs', 'LEUCÒCITS',
                                        'leucos')

    data = extraer_name_value_to_column(data, 'labs', 'LIMFÒCITS %',
                                       'limfos')

    data = extraer_name_value_to_column(data, 'labs', 'PROTEÏNA C REACTIVA Sèrum',
                                        'prot C react')

    data = extraer_name_value_to_column(data, 'labs', 'UREA Sèrum',
                                        'urea')

    data = extraer_name_value_to_column(data, 'labs', 'F. G. ESTIMAT (MDRD) Sèrum',
                                        'FGE MDRD')

    data = extraer_name_value_to_column(data, 'labs', 'F. G. ESTIMAT (CKD-EPI) Sèrum',
                                        'FGE CDK-EPI')


    # DF para usar en jupyter
    data.to_pickle('./data/processed/dataframe.pkl')





