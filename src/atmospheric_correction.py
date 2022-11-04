import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os, glob
import re
import ipython_genutils
from Py6S import *
from osgeo import gdal
from natsort import natsorted


###########################################################
#### Lectura de parámetros de radiancia y reflectancia ####
###########################################################

ruta_archivos = "../data_20150721"
os.chdir(ruta_archivos)
os.getcwd()

lines_ref_mult = []
lines_ref_add = []
linenum = 0
pattern_ref_mult = re.compile(r"REFLECTANCE_MULT_BAND_")
pattern_ref_add = re.compile(r"REFLECTANCE_ADD_BAND_")
with open(glob.glob("*MTL.txt")[0], "r") as metadata:
    for line in metadata:
        if pattern_ref_mult.search(line) != None:
            linenum += 1
            lines_ref_mult.append((linenum, line.rstrip('\n')))

        if pattern_ref_add.search(line) != None:
            linenum += 1
            lines_ref_add.append((linenum, line.rstrip('\n')))


lines_rad_mult = []
lines_rad_add = []
linenum = 0
pattern_rad_mult = re.compile(r"RADIANCE_MULT_BAND_")
pattern_rad_add = re.compile(r"RADIANCE_ADD_BAND_")
with open(glob.glob("*MTL.txt")[0], "r") as metadata:
    for line in metadata:
        if pattern_rad_mult.search(line) != None:
            linenum += 1
            lines_rad_mult.append((linenum, line.rstrip('\n')))

        if pattern_rad_add.search(line) != None:
            linenum += 1
            lines_rad_add.append((linenum, line.rstrip('\n')))


# for element in lines_ref_mult:
#     print("Line ", str(element[0]), ": " + element[1].split(' = ')[1])
# 
# for element in lines_ref_add:
#     print("Line ", str(element[0]), ": " + element[1].split(' = ')[1])
# 
# for element in lines_rad_mult:
#     print("Line ", str(element[0]), ": " + element[1])
# 
# for element in lines_rad_add:
#     print("Line ", str(element[0]), ": " + element[1])


#######################################
#### Lectura de bandas satelitales ####
#######################################

band_pathfiles = natsorted(glob.glob("*_B*TIF"))

files = {}
reflectance_conv = {}
radiance_conv = {}

# Reflectance
# print("\n Initializing reflectance conversion... \n")
# for i in range(0, len(lines_ref_mult)):
#     nir_img = gdal.Open(band_pathfiles[i])
#     band = nir_img.ReadAsArray()
#     files["B"+str(int(i)+1)] = band
#     reflectance_mult_band = float(lines_ref_mult[i][1].split(" = ")[1])
#     reflectance_add_band = float(lines_ref_add[i][1].split(" = ")[1])
#     reflectance_conv["B"+str(int(i)+1)] = (reflectance_mult_band * band) + reflectance_add_band
#     print("Band " + str(int(i)+1)) 
#     print("Reflectance multiplicative factor: " + str(reflectance_mult_band))
#     print("Reflectance additive factor: " + str(reflectance_add_band) + "\n")
#     # Radiancef add: " + str(reflectance_add_band) + "\n")

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

# files["B1"]
# reflectance_conv["B1"]
# radiance_conv["B1"]

# Una fila y dos columnas
fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow(files["B4"])
axes[0].set_title("Band 4 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow(radiance_conv["B4"])
axes[1].set_title("Band 4 - Radiance conversion") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()

#################################################################

s = SixS()

t = "16:09:45.2093320Z"
(hora, minutos, segundo) = t.split(':')
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
s.atmos_corr = AtmosCorr.AtmosCorrLambertianFromRadiance(500)
wavelengths = [0.44, 0.48, 0.56, 0.655, 0.865, 1.61, 2.20, 0.59] # B1 - B8

for wv in wavelengths:
    s.wavelength = Wavelength(0.44)
    s.run()
    coef_xa = s.outputs.values['coef_xa']
    coef_xb = s.outputs.values['coef_xb']
    coef_xc = s.outputs.values['coef_xc']
    print(coef_xa)
    print(coef_xb)
    print(coef_xc)
    
    reflectance_pixel = radiance_conv["B1"].copy()
    y = coef_xa * reflectance_pixel - coef_xb
    acr = y/(1. + coef_xc * y)


# atmospheric correction result (Rayleigh): 
# y=xa*ref -xb ; acr = y/(1.+xc*y)

print(s.outputs.fulltext)


fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow(files["B1"])
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow(acr)
axes[1].set_title("Band 1 - Rayleigh Correction") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()