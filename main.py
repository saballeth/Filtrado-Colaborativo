import requests
import psycopg2
import json
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

# Constantes
URL_API = 'https://arqui-sistema-recomendacion-85b7038cdf33.herokuapp.com/api/inmueblesPorUsuario/get_filtro/'
URL_API_LOGIN = 'https://arqui-sistema-recomendacion-85b7038cdf33.herokuapp.com/api/login/'
LOGIN_DATA = {'username': 'diego3026', 'password': '3174748557d'}
DB_CONFIG = {
    'host': 'inmuebles.postgres.database.azure.com',
    'database': 'postgres',
    'user': 'gidsyc',
    'password': 'Semillero2024'
}
CONSULTA_INMUEBLES = 'SELECT id FROM inmobiliaria_inmueble'
CONSULTA_USUARIOS = 'SELECT id, username FROM inmobiliaria_usuario'
PESOS = [5, 4, 1]
NOMBRE_ARCHIVO_JSON = 'datosLimpios.json'

def normalizacion(valor, valor_maximo, valor_minimo):
    return (valor - valor_minimo) / (valor_maximo - valor_minimo)

class InmuebleUsuarioNormalizado:
    def __init__(self, inmueble, usuario, favorito, calificacion, clics, pesos):
        self.inmueble = inmueble
        self.usuario = usuario
        self.favorito = favorito
        self.calificacion = calificacion
        self.clics = clics
        self.pesos = pesos

    def get_clasificacion(self):
        return self.pesos[0] * self.favorito + self.pesos[1] * self.calificacion + self.pesos[2] * self.clics

def get_datos_api():
    response_login = requests.post(url=URL_API_LOGIN, data=LOGIN_DATA)
    if response_login.status_code != 200:
        raise Exception(f'Error en la petición login: {response_login.status_code}')
    token = response_login.json()['access']
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(URL_API, headers=headers)
    if response.status_code != 200:
        raise Exception(f'Error en la petición: {response.status_code}')
    return response.json()

def consultar_base_de_datos(config, consulta):
    try:
        conexion = psycopg2.connect(**config)
        cursor = conexion.cursor()
        cursor.execute(consulta)
        nombres_columnas = [desc[0] for desc in cursor.description]
        resultados = cursor.fetchall()
        cursor.close()
        conexion.close()
        return nombres_columnas, resultados
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error al acceder a la base de datos: {error}')
        return None

def inicializar_datos_limpios(resultados, resultados_user):
    datosLimpios = {}
    for item in resultados_user:
        usuario = item[1]
        if usuario not in datosLimpios:
            datosLimpios[usuario] = {}
            for inmueble in resultados:
                id_inmueble = inmueble[0]
                datosLimpios[usuario][id_inmueble] = None
    return datosLimpios

def procesar_datos_api(data, datosLimpios, pesos):
    calificaciones = [item['calificacion'] for item in data if item['calificacion'] is not None]
    clics = [item['numeroDeClicks'] for item in data if item['numeroDeClicks'] is not None]
    favoritos = [item['favorito'] for item in data if item['favorito'] is not None]

    promedio_calificaciones = sum(calificaciones) / len(calificaciones) if calificaciones else 1
    max_clics = max(clics) if clics else 0
    promedio_clics = sum(clics) / len(clics) if clics else 0
    promedio_favoritos = sum(favoritos) / len(favoritos) if favoritos else 0

    for value in data:
        id_inmueble = value.get('inmueble')
        usuario = value.get('usuario')
        calificacion = normalizacion(value['calificacion'], 5, 1) if value['calificacion'] is not None else promedio_calificaciones
        favorito = value.get('favorito', promedio_favoritos)
        clics = normalizacion(value['numeroDeClicks'], max_clics, 0) if max_clics > 0 and value['numeroDeClicks'] is not None else promedio_clics

        if id_inmueble and usuario:
            inmueble_por_usuario = InmuebleUsuarioNormalizado(id_inmueble, usuario, favorito, calificacion, clics, pesos)
            datosLimpios[usuario][id_inmueble] = inmueble_por_usuario.get_clasificacion()
    return datosLimpios

def guardar_json(datos, nombre_archivo):
    with open(nombre_archivo, 'w') as archivo_json:
        json.dump(datos, archivo_json, indent=4)
    print(f"Datos guardados en {nombre_archivo}")

def calcular_similitud_coseno(datos):
    ratings_df = pd.DataFrame(datos).T
    cosine_sim = cosine_similarity(ratings_df.fillna(0))
    cosine_sim_df = pd.DataFrame(cosine_sim, index=ratings_df.index, columns=ratings_df.index)
    return cosine_sim_df, ratings_df

def predict_ratings(user, ratings_df, sim_df):
    user_ratings = ratings_df.loc[user]  # Valoraciones del usuario objetivo
    sim_scores = sim_df[user]  # Similitudes del usuario con otros usuarios
    
    sim_scores = sim_scores.drop(user)
    ratings_df = ratings_df.drop(user)
    
    weighted_sum = (ratings_df.T * sim_scores).sum(axis=1)
    sim_sum = sim_scores.sum()
    
    predicted_ratings = weighted_sum / sim_sum
    predicted_ratings = predicted_ratings[user_ratings.isna()]
    predicted_ratings = predicted_ratings[predicted_ratings > 0]

    return predicted_ratings.sort_values(ascending=False)

def main():
    columnas, resultados = consultar_base_de_datos(DB_CONFIG, CONSULTA_INMUEBLES)
    columnas_user, resultados_user = consultar_base_de_datos(DB_CONFIG, CONSULTA_USUARIOS)
    
    if resultados and resultados_user:
        datosLimpios = inicializar_datos_limpios(resultados, resultados_user)
        datos_api = get_datos_api()
        datosLimpios = procesar_datos_api(datos_api, datosLimpios, PESOS)
        similitud_coseno_dataframe, ratings_df = calcular_similitud_coseno(datosLimpios)
        print(similitud_coseno_dataframe)
        predicciones = predict_ratings("camilo", ratings_df, similitud_coseno_dataframe)
        print("Predicciones de valoraciones:\n", predicciones)
        recomendaciones = predicciones.index.tolist()
        print(f"inmuebles recomendados: {recomendaciones}")

        guardar_json(datosLimpios, NOMBRE_ARCHIVO_JSON)
    else:
        print('No se obtuvieron resultados de la base de datos.')

if __name__ == "__main__":
    main()
