from src.utils import *
import glob
import re
import ipython_genutils
from Py6S import *
from osgeo import gdal
import numpy as np

###########################################################
#### Lectura de parámetros de radiancia y reflectancia ####
###########################################################

ruta_archivos = "data/data_20150721"

#ruta_archivos = "../017047/LC08_L1GT_017047_20150103_20170302_01_T2"
#os.chdir(ruta_archivos)
#os.chdir("work")
#os.getcwd()

lines_ref_add, lines_ref_mult = get_reflectance_parameters(ruta_archivos)

lines_rad_add, lines_rad_mult = get_radiance_parameters(ruta_archivos)

#######################################
#### Lectura de bandas satelitales ####
#######################################

band_pathfiles = get_band_pathfiles(ruta_archivos)

reflectance_conv = reflectance_transformation(
    ref_mult_params = lines_ref_add, 
    ref_add_params = lines_ref_add, 
    band_pathfiles = band_pathfiles
    )

files, radiance_conv = radiance_transformation(
    rad_mult_params = lines_rad_mult, 
    rad_add_params = lines_rad_add, 
    band_pathfiles = band_pathfiles
    )

#######################################
####### Correción de Rayleigh  ########
#######################################


pattern_scene_center_time = re.compile(r"SCENE_CENTER_TIME")
pattern_sun_azimuth = re.compile(r"SUN_AZIMUTH")
pattern_date_acquired = re.compile(r"DATE_ACQUIRED")
pattern_latitude = re.compile(r"_LAT_")
pattern_longitude = re.compile(r"_LON_")
latitude_corners = []
longitude_corners = []

with open(glob.glob(ruta_archivos + "/" + "*MTL.txt")[0], "r") as metadata:
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

rayleigh_conv = rayleigh_correction(
    radiance_conv = radiance_conv, 
    time_str = scene_center_time,
    year = year, month = month, day = day, 
    latitude = 20.22962, # np.mean(latitude_corners), 
    longitude = -86.41885, # np.mean(longitude_corners),  
    solar_a = scene_sun_azimuth
    )

#################################################
####### Cálculo de Floating Algae Index  ########
#################################################

fai_reflectance = Fai(lambda_data = reflectance_conv, rayleigh_data = rayleigh_conv)

fai_reflectance_std = (fai_reflectance - np.mean(fai_reflectance))/(np.std(fai_reflectance) )
fai_reflectance_sca = (fai_reflectance - np.min(fai_reflectance))/(np.max(fai_reflectance) - np.min(fai_reflectance)) 


#########################################
########## Escritura de TIFF  ###########
#########################################

dt = fai_reflectance_sca.copy()

path_output = "data/test1.tif"
driver = gdal.GetDriverByName('GTiff')
filas = dt.shape[0]
colums = dt.shape[1]
class_dt = driver.Create(path_output, colums, filas, eType = gdal.GDT_Float32)
class_dt = class_dt.GetRasterBand(1).WriteArray(dt)








