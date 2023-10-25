# python pipeline_test.py --geoid='017047' --date='20231016' --dataPath='/data/landsat/output/data'
from datetime import datetime
from utils import *
import os
import time
import rasterio
import argparse
import numpy as np

#### Definición de parámetros
#geoid = "018045"
#date_target = '20231015'
#data_dir = '/data/landsat/output/data'

if __name__ == '__main__':
    # User input    
    parser = argparse.ArgumentParser()
    parser.add_argument('-gi', '--geoid', required=True, help='IDs od scenes for download')
    parser.add_argument('-dt', '--date', required=True, help='date scene for download')
    parser.add_argument('-dp', '--dataPath', required=True, help='Folder path for new products')
    args = parser.parse_args()

    geoid		 =  args.geoid
    date         =  args.date
    data_dir     =  args.dataPath
    data_path    =  data_dir + "/" + geoid

    print(f'fecha ingresada: {date}, de tipo: {type(date)}\n')
    fecha_datetime = datetime.fromisoformat(str(date))
    date_target = fecha_datetime.strftime("%Y%m%d")

    print("\nRunning Scripts...\n")
    print(f"Compute FAI for {geoid} scene and {date_target} date...\n")

    startTime = time.time()

    scenes_folder = os.listdir(f'{data_dir}/{geoid}')
    print(f'Looking scenes in: {scenes_folder}')

    #### Selección y filtro de escena
    date_pattern = re.compile(fr".*_{date_target}")
    selected_scene = [s for s in scenes_folder if date_pattern.match(s)][0]
    print(f"\n Selected scene filtered: {selected_scene} \n")

    print(f"\nReading data ... \n")
    #### Lectura de bandas espaciales
    files_path = f"{data_dir}/{geoid}/{selected_scene}"

    #### Lectura de parámetros de radianza y reflectancia
    lines_ref_add, lines_ref_mult = get_reflectance_parameters(files_path)
    lines_rad_add, lines_rad_mult = get_radiance_parameters(files_path)
    band_pathfiles = get_band_pathfiles(files_path)
    print(f"\n... Done!")

    print(f"\nInitializing reflectance transformation ... \n")
    #### Transformación por reflectancia
    reflectance_conv = reflectance_transformation(
        ref_mult_params = lines_ref_add, 
        ref_add_params = lines_ref_add, 
        band_pathfiles = band_pathfiles
    )
    print(f"\n... Done!")

    print(f"\nInitializing radiance transformation ... \n")
    #### Transformación por radianza
    files, radiance_conv = radiance_transformation(
        rad_mult_params = lines_rad_mult, 
        rad_add_params = lines_rad_add, 
        band_pathfiles = band_pathfiles
    )
    print(f"\n... Done!")

    pattern_scene_center_time = re.compile(r"SCENE_CENTER_TIME")
    pattern_sun_azimuth = re.compile(r"SUN_AZIMUTH")
    pattern_date_acquired = re.compile(r"DATE_ACQUIRED")
    pattern_latitude = re.compile(r"_LAT_")
    pattern_longitude = re.compile(r"_LON_")
    latitude_corners = []
    longitude_corners = []

    with open(glob.glob(files_path + "/" + "*MTL.txt")[0], "r") as metadata:
        for line in metadata:
            if pattern_scene_center_time.search(line) != None:
                scene_center_time = line.rstrip('\n').split(' = ')[1][1:-1]
            if pattern_sun_azimuth.search(line) != None:
                scene_sun_azimuth = float(line.rstrip('\n').split(' = ')[1])
            if pattern_date_acquired.search(line) != None:
                scene_date_acquired = line.rstrip('\n').split(' = ')[1].split('-')
                year = scene_date_acquired[0]
                month = scene_date_acquired[1]
                day = scene_date_acquired[2]
            if pattern_latitude.search(line) != None:
                latitude_corners.append(float(line.rstrip('\n').split(' = ')[1]))
            if pattern_longitude.search(line) != None:
                longitude_corners.append(float(line.rstrip('\n').split(' = ')[1]))

    print(f"\nInitializing Rayleigh transformation ... \n")
    #### Corrección de Rayleigh
    rayleigh_conv = rayleigh_correction(
        radiance_conv = radiance_conv, 
        time_str = scene_center_time,
        year = year, month = month, day = day, 
        latitude = np.mean(latitude_corners),
        longitude = np.mean(longitude_corners),   
        solar_a = scene_sun_azimuth
        )
    print(f"\n... Done!")

    print(f"\nInitializing FAI compute ... \n")
    #### Cálculo de Índice de Alga Flotante (FAI)
    fai_reflectance = Fai(lambda_data = reflectance_conv, rayleigh_data = rayleigh_conv)
    
    print(f"\nComputing intersection of land/water mask ... \n")
    #### Intersección con máscara de tierra

    

    original_crs = rasterio.open(band_pathfiles[1])
    fai_w_mask = fai_reflectance.copy()
    if geoid != '017047':
        mask = rasterio.open(glob.glob(files_path + "/" + "*_landmask.tif")[0]).read(1)

        print(f'\nDimension of original matrix: {fai_w_mask.shape}')
        print(f'Dimension of landmask matrix: {mask.shape}\n')

        fai_w_mask = np.where(mask == 1, fai_reflectance, None)

    
    file_location = f"{files_path}/{band_pathfiles[1][:-7].split('/')[-1]}_FAI.TIF"
    print(f"\nSaving new FAI file in location: \n\n {file_location}")
    
    #### Persistencia de nuevo archivo FAI
    save_tiff_wcrs(
        original_crs = original_crs, 
        data = fai_w_mask, 
        path = file_location
        )    

    print(f"\n... \n... \n... \n\n¡¡SUCCEESS!!\n")
    
    executionTime = round((time.time() - startTime), 1)
    print(f'(Total time: {executionTime} seconds)')


