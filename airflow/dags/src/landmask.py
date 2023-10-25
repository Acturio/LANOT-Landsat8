# python landmask.py --geoid='019045' --date='20231022' --dataPath='/data/landsat/output/data'
'''
Script que genera la mascara de tierra binaria a partir de un archivo geotiff de Landsat 8
'''

from utils import get_band_pathfiles
from datetime import datetime
from glob import glob
import geopandas as gpd
import numpy as np
import argparse
import rasterio
import os
import re

#### Definición de parámetros
#geoid = "018046"
#date_target = '20231015'
#data_dir = '/data/landsat/output/data'

def landmask(path_input, path_output, path_tmp):
    # Abre el archivo TIFF de entrada en modo lectura
    with rasterio.open(path_input, 'r+') as src:
        # Obtiene las coordenas del cuadrado de la imagen
        x_min = src.bounds.left
        x_max = src.bounds.right
        y_min = src.bounds.bottom
        y_max = src.bounds.top
        # Obtiene la resolución de la imagen
        res = src.res[0]
        # Obtiene el numero de filas y columnas de la imagen
        rows = src.height
        cols = src.width

    # Abre el archivo vectorial de linea de costa
    #coast = gpd.read_file('./data/land_2_UTM16N_20m_SPlaya_2021.geojson')
    coast = gpd.read_file('/data/landsat/output/data/landmask/land_2_UTM16N_20m_SPlaya_2021.geojson')
    # Realiza un recorte de la linea de costa al cuadrado de la imagen
    coast = coast.cx[x_min:x_max, y_min:y_max]

    # Escribe el archivo vectorial de linea de costa
    coast.to_file(path_tmp + 'coast.geojson', driver='GeoJSON')
    # Obtiene el nombre de salida a partir del nombre de entrada, agregando el sufijo _landmask
    name = path_input.split('/')[-1].split('.')[0][:-3] + '_landmask.tif'
    
    print(f'\npath_output: {path_output}\n')
    print(f'\nname: {name}\n')
    

    # Rasteriza el archivo vectorial de linea de costa, con la misma extension, numero de filas y columnas y resolución que la imagen, dando valores de 1 al oceano y 0 a la tierra
    os.system('gdal_rasterize -burn 1 -tr ' + str(res) + ' ' + str(res) + ' -te ' + str(x_min) + ' ' + str(y_min) + ' ' + str(x_max) + ' ' + str(y_max) + ' -ot Byte -l coast ' + path_tmp + 'coast.geojson ' + path_tmp + name)
    
    # Nuevo valor que deseas asignar como "no data"
    new_no_data_value = 0
    # Abre el archivo TIFF de entrada en modo lectura
    with rasterio.open(path_tmp + name, 'r+') as src:
        # Cambia el valor de "no data" en los metadatos
        src.nodata = new_no_data_value
        # Lee los datos del raster como una matriz numpy
        data = src.read(1)  # 1 indica que estamos leyendo la primera banda
        # Obtén los metadatos del archivo original
        profile = src.profile
        # Cambia los valores de 1 a 0 y de 0 a 1
        data[data == 1] = 2
        data[data == 0] = 1
        data[data == 2] = 0
        # Abre el archivo de salida TIFF en modo escritura
        with rasterio.open(path_output + name, 'w', **profile) as dst:
            # Asigna el valor de no data
            dst.nodata = new_no_data_value
            # Escribe los datos actualizados en el archivo de salida
            dst.write(data, 1)  # 1 indica que estamos escribiendo en la primera banda
    

def main():

    print("\nRunning Scripts...\n")

    parser = argparse.ArgumentParser()
    parser.add_argument('-gi', '--geoid', required=True, help='IDs od scenes for download')
    parser.add_argument('-dt', '--date', required=True, help='date scene for download')
    parser.add_argument('-dp', '--dataPath', required=True, help='Folder path for new products')
    args = parser.parse_args()

    geoid		 =  args.geoid
    date         =  args.date
    data_dir     =  args.dataPath
    data_path    =  data_dir + "/" + geoid

    scenes_folder = os.listdir(f'{data_dir}/{geoid}')
    fecha_datetime = datetime.fromisoformat(str(date))
    date_target = fecha_datetime.strftime("%Y%m%d")
    
    print(f'\GeoId target: {geoid}')
    print(f'\nDate target: {date_target}')
    print(f'\nScenes folder: {scenes_folder}')

    #### Selección y filtro de escena
    date_pattern = re.compile(fr".*_{date_target}")
    selected_scene = [s for s in scenes_folder if date_pattern.match(s)][0]

    print(f"\n Selected scene filtered: {selected_scene} \n")
    files_path = f"{data_dir}/{geoid}/{selected_scene}/"
    file = get_band_pathfiles(files_path)[0]

    # Ejecuta la función
    print('Processing: ' + file)
    path_tmp = '/data/landsat/output/data/tmp/'
    landmask(path_input = file, path_output = files_path, path_tmp = path_tmp)

    # Elimina el archivo temporal
    os.system('rm -r ' + path_tmp+'*')


if __name__ == '__main__':
    main()




