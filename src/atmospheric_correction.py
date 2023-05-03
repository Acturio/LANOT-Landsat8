from src.utils import *
import matplotlib.pyplot as plt
import os, glob
import re
import ipython_genutils
from Py6S import *
from osgeo import gdal
from natsort import natsorted
#import pandas as pd
import numpy as np


###########################################################
#### Lectura de parámetros de radiancia y reflectancia ####
###########################################################

#ruta_archivos = "data/data_20150721"
ruta_archivos = "data/data_20150721"
ruta_archivos = "../017047/LC08_L1GT_017047_20150103_20170302_01_T2"

#os.chdir(ruta_archivos)
#os.getcwd()

lines_ref_mult = []
lines_ref_add = []
linenum = 0
pattern_ref_mult = re.compile(r"REFLECTANCE_MULT_BAND_")
pattern_ref_add = re.compile(r"REFLECTANCE_ADD_BAND_")
with open(glob.glob(ruta_archivos + "/" + "*MTL.txt")[0], "r") as metadata:
    for line in metadata:
        if pattern_ref_mult.search(line) != None:
            linenum += 1
            lines_ref_mult.append((linenum, line.rstrip('\n')))

        if pattern_ref_add.search(line) != None:
            linenum += 1
            lines_ref_add.append((linenum, line.rstrip('\n')))


lines_ref_add, lines_ref_mult = get_reflectance_parameters(ruta_archivos)



lines_rad_mult = []
lines_rad_add = []
linenum = 0
pattern_rad_mult = re.compile(r"RADIANCE_MULT_BAND_")
pattern_rad_add = re.compile(r"RADIANCE_ADD_BAND_")
with open(glob.glob(ruta_archivos + "/" + "*MTL.txt")[0], "r") as metadata:
    for line in metadata:
        if pattern_rad_mult.search(line) != None:
            linenum += 1
            lines_rad_mult.append((linenum, line.rstrip('\n')))

        if pattern_rad_add.search(line) != None:
            linenum += 1
            lines_rad_add.append((linenum, line.rstrip('\n')))


lines_rad_add, lines_rad_mult = get_radiance_parameters(ruta_archivos)

#######################################
#### Lectura de bandas satelitales ####
#######################################

#band_pathfiles = natsorted(glob.glob(ruta_archivos + "/" + "*_B*TIF"))
band_pathfiles = get_band_pathfiles(ruta_archivos)


files = {}
reflectance_conv = {}

# Reflectance
print("\n Initializing reflectance conversion... \n")
for i in range(0, len(lines_ref_mult)):
    nir_img = gdal.Open(band_pathfiles[i])
    band = nir_img.ReadAsArray()
    files["B"+str(int(i)+1)] = band
    reflectance_mult_band = float(lines_ref_mult[i][1].split(" = ")[1])
    reflectance_add_band = float(lines_ref_add[i][1].split(" = ")[1])
    reflectance_conv["B"+str(int(i)+1)] = (reflectance_mult_band * band) + reflectance_add_band
    print("Band " + str(int(i)+1)) 
    print("Reflectance multiplicative factor: " + str(reflectance_mult_band))
    print("Reflectance additive factor: " + str(reflectance_add_band) + "\n")

reflectance_conv = reflectance_transformation(
    ref_mult_params = lines_ref_add, 
    ref_add_params = lines_ref_add, 
    band_pathfiles = band_pathfiles
    )


radiance_conv = {}
# Radiance
print("\n Initializing radiance conversion... \n")
for i in range(0, len(lines_rad_mult)):
    nir_img = gdal.Open(band_pathfiles[i])
    band = nir_img.ReadAsArray()
    files["B"+str(int(i)+1)] = band
    radiance_mult_band = float(lines_rad_mult[i][1].split(" = ")[1])
    radiance_add_band = float(lines_rad_add[i][1].split(" = ")[1])
    radiance_conv["B"+str(int(i)+1)] = (radiance_mult_band * band) + radiance_add_band
    print("Band " + str(int(i)+1)) 
    print("Radiance multiplicative factor: " + str(radiance_mult_band))
    print("Radiance additive factor: " + str(radiance_add_band) + "\n")


files, radiance_conv = radiance_transformation(
    rad_mult_params = lines_rad_mult, 
    rad_add_params = lines_rad_add, 
    band_pathfiles = band_pathfiles
    )

# files["B1"]
# reflectance_conv["B1"]
# radiance_conv["B1"]

# Una fila y dos columnas
fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow(files["B1"])
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow(radiance_conv["B1"])
axes[1].set_title("Band 1 - Radiance conversion") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()

#################################################################

s = SixS()

t = "16:09:45.2093320Z"
(hora, minuto, segundo) = t.split(':')
decimal_time = int(hora) + (int(minuto) / 60) + (float(segundo[0:-1])/3600)

s.geometry.month = 7
s.geometry.day = 21
s.geometry.gmt_decimal_hour = decimal_time
s.geometry.latitude = 20.22962
s.geometry.longitude = -86.41885
s.geometry.solar_a = 84.99899008

s.altitudes.set_sensor_satellite_level()
s.altitudes.set_target_sea_level()

# Wavelength of 0.5nm
s.ground_reflectance = GroundReflectance.HomogeneousLambertian(GroundReflectance.ClearWater)
s.atmos_profile = AtmosProfile.FromLatitudeAndDate(20.22962, "2015-07-21")
s.aero_profile = AeroProfile.PredefinedType(AeroProfile.Maritime)
s.atmos_corr = AtmosCorr.AtmosCorrLambertianFromRadiance(320)

wavelengths = [0.44, 0.48, 0.56, 0.660, 0.865, 1.60] # B1 - B6
rayleigh_conv = {}

for j in range(0, len(wavelengths)):
    
    # atmospheric correction result (Rayleigh): 
    # y=xa*ref -xb ; acr = y/(1.+xc*y)
    print("\n Rayleigh correction for band: " + str(j + 1))

    s.wavelength = Wavelength(wavelengths[j])
    s.run()
    coef_xa = s.outputs.values['coef_xa']
    coef_xb = s.outputs.values['coef_xb']
    coef_xc = s.outputs.values['coef_xc']

    reflectance_pixels = radiance_conv["B"+str(int(j)+1)].copy()
    y = coef_xa * reflectance_pixels - coef_xb
    rayleigh_conv["B"+str(int(j)+1)] = y/(1. + coef_xc * y)
    print(" Ok!")
    #print(s.outputs.fulltext)



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

#######################################################################

fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow(files["B1"])
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow(rayleigh_conv["B1"])
axes[1].set_title("Band 1 - Rayleigh Correction") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()

####################################################

# fai_radiance = Fai(lambda_data = radiance_conv, rayleigh_data = rayleigh_conv)
fai_reflectance = Fai(lambda_data = reflectance_conv, rayleigh_data = rayleigh_conv)

fai_reflectance_std = (fai_reflectance - np.mean(fai_reflectance))/(np.std(fai_reflectance) )
fai_reflectance_sca = (fai_reflectance - np.min(fai_reflectance))/(np.max(fai_reflectance) - np.min(fai_reflectance)) 

#######################################################################

fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow( ((files["B1"] - np.mean(files["B1"]))/(np.std(files["B1"]) )) )
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow( fai_reflectance_std )
axes[1].set_title("Floating Algae Index") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()

#######################################################################

fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow( files["B1"] )
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow( fai_reflectance_sca ) 
axes[1].set_title("Floating Algae Index") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()

#######################################################################


# dt = rayleigh_conv["B6"].copy()
dt = fai_reflectance_sca.copy()

path_output = "data/test1.tif"
driver = gdal.GetDriverByName('GTiff')
filas = dt.shape[0]
colums = dt.shape[1]
class_dt = driver.Create(path_output, colums, filas, eType=gdal.GDT_Float32)
class_dt = class_dt.GetRasterBand(1).WriteArray(dt)
