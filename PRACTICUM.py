# usar diccionarios mejor envez de convertir a df

import json
import pandas as pd
from datetime import datetime
from auxiliary_functions import (valores_codigos, contar_diccionarios, dias_ingreso_total, asignar_intervalo_edad,
                                 sumar_barthel, sumar_emina, obtener_ultimo_resultat, obtener_valor_promedio,
                                 canadenca_comparada)
from listas import PA_list, P_list, disfagia_list

if __name__ == "__main__":
    with open('./data/origin/bbdd_pneumonia_aspirativa.json') as archivo:
        datos = json.load(archivo)

    data = pd.DataFrame(datos)

    # Función que verifica si alguno de los valores a buscar de la lista PA_list está en alguna
    # lista llamada 'codiDiagnostics' en la fila
    data = valores_codigos(data=data, lista=PA_list, nombre_columna='PA_diagnosticada')

    # Función que verifica si alguno de los valores a buscar de la lista disfagia_list está en alguna
    # lista llamada 'codiDiagnostics' en la fila
    data = valores_codigos(data=data, lista=disfagia_list, nombre_columna='DO_diagnosticada')

    # Función que indica cuantas veces ha ingresado el paciente, en base a contar el número de diccionarios que hay
    # en 'ingressos'
    data = contar_diccionarios(data, 'ingressos')

    # Función que devuelve un sumatorio de los dias en total que ha estado ingresado el paciente, en base a hacer un
    # sumatorio con el resultado de la resta de las claves 'dataAlta' i 'dataIngres'
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





