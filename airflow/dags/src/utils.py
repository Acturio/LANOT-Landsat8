import json
import requests
import sys
import re
import threading
import yaml
import glob
import rasterio
from rasterio.crs import CRS
import numpy as np
from Py6S import *
from osgeo import gdal
from natsort import natsorted


def get_reflectance_parameters(path):

    lines_ref_mult = []
    lines_ref_add = []
    linenum = 0

    pattern_ref_mult = re.compile(r"REFLECTANCE_MULT_BAND_")
    pattern_ref_add = re.compile(r"REFLECTANCE_ADD_BAND_")

    with open(glob.glob(path + "/" + "*MTL.txt")[0], "r") as metadata:
        for line in metadata:
            if pattern_ref_mult.search(line) != None:
                linenum += 1
                for band in ['BAND_4', 'BAND_5', 'BAND_6']:
                    if band in line.rstrip('\n'):
                        lines_ref_mult.append((linenum, line.rstrip('\n')))
                
            if pattern_ref_add.search(line) != None:
                linenum += 1
                for band in ['BAND_4', 'BAND_5', 'BAND_6']:
                    if band in line.rstrip('\n'):
                        lines_ref_add.append((linenum, line.rstrip('\n')))
    
    return((lines_ref_add, lines_ref_mult))



def get_radiance_parameters(path):

    lines_rad_mult = []
    lines_rad_add = []
    linenum = 0

    pattern_rad_mult = re.compile(r"RADIANCE_MULT_BAND_")
    pattern_rad_add = re.compile(r"RADIANCE_ADD_BAND_")

    with open(glob.glob(path + "/" + "*MTL.txt")[0], "r") as metadata:
        for line in metadata:
            if pattern_rad_mult.search(line) != None:
                linenum += 1
                for band in ['BAND_4', 'BAND_5', 'BAND_6']:
                    if band in line.rstrip('\n'):
                        lines_rad_mult.append((linenum, line.rstrip('\n')))

            if pattern_rad_add.search(line) != None:
                linenum += 1
                for band in ['BAND_4', 'BAND_5', 'BAND_6']:
                    if band in line.rstrip('\n'):
                        lines_rad_add.append((linenum, line.rstrip('\n')))

    return((lines_rad_add, lines_rad_mult))



def get_band_pathfiles(path):
    
    return(natsorted(glob.glob(path + "/" + "*_B*TIF")))



def reflectance_transformation(ref_mult_params, ref_add_params, band_pathfiles):

    files = {}
    reflectance_conv = {}
    
    # Reflectance
    print("\n Initializing reflectance conversion... \n")
    for i in range(0, len(ref_mult_params)):
        nir_img = rasterio.open(band_pathfiles[i])
        band = nir_img.read(1)
        #nir_img = gdal.Open(band_pathfiles[i])
        #band = nir_img.ReadAsArray()
        files["B" + str(int(i)+4)] = band
        reflectance_mult_band = float(ref_mult_params[i][1].split(" = ")[1])
        reflectance_add_band = float(ref_add_params[i][1].split(" = ")[1])
        reflectance_conv["B"+str(int(i)+4)] = (reflectance_mult_band * band) + reflectance_add_band
        print("Band " + str(int(i)+4)) 
        print("Reflectance multiplicative factor: " + str(reflectance_mult_band))
        print("Reflectance additive factor: " + str(reflectance_add_band) + "\n")
        nir_img.close()

    return(reflectance_conv)



def radiance_transformation(rad_mult_params, rad_add_params, band_pathfiles):

    files = {}
    radiance_conv = {}

    # Radiance
    print("\n Initializing radiance conversion... \n")
    for i in range(0, len(rad_mult_params)):
        nir_img = rasterio.open(band_pathfiles[i])
        band = nir_img.read(1)
        #nir_img = gdal.Open(band_pathfiles[i])
        #band = nir_img.ReadAsArray()
        files["B"+str(int(i)+4)] = band
        radiance_mult_band = float(rad_mult_params[i][1].split(" = ")[1])
        radiance_add_band = float(rad_add_params[i][1].split(" = ")[1])
        radiance_conv["B"+str(int(i)+4)] = (radiance_mult_band * band) + radiance_add_band
        print("Band " + str(int(i)+4)) 
        print("Radiance multiplicative factor: " + str(radiance_mult_band))
        print("Radiance additive factor: " + str(radiance_add_band) + "\n")
        nir_img.close()

    return((files, radiance_conv))



def rayleigh_correction(radiance_conv, time_str, year, month, day, latitude, longitude, solar_a):
    
    s = SixS()
    print("Six Sigma Initialization ... \n")

    (hora, minuto, segundo) = time_str.split(':')
    decimal_time = int(hora) + (int(minuto) / 60) + (float(segundo[0:-1])/3600)

    s.geometry.month = int(month)
    s.geometry.day = int(day)
    s.geometry.gmt_decimal_hour = decimal_time
    s.geometry.latitude = latitude
    s.geometry.longitude = longitude
    s.geometry.solar_a = solar_a

    s.altitudes.set_sensor_satellite_level()
    s.altitudes.set_target_sea_level()

    # Wavelength of 0.5nm
    s.ground_reflectance = GroundReflectance.HomogeneousLambertian(GroundReflectance.ClearWater)
    s.atmos_profile = AtmosProfile.FromLatitudeAndDate(latitude, year + "-" + month + "-" + day)
    s.aero_profile = AeroProfile.PredefinedType(AeroProfile.Maritime)
    s.atmos_corr = AtmosCorr.AtmosCorrLambertianFromRadiance(320)

    #wavelengths = [0.443, 0.4825, 0.5625, 0.655, 0.865, 1.60]
    wavelengths = [0.655, 0.865, 1.61] 
    rayleigh_conv = {}

    for j in range(0, len(wavelengths)):

        # atmospheric correction result (Rayleigh): 
        # y=xa*ref -xb ; acr = y/(1.+xc*y)
        print("\n Rayleigh correction for band: " + str(j + 4))

        s.wavelength = Wavelength(wavelengths[j])
        s.run()

        coef_xa = s.outputs.values['coef_xa']
        coef_xb = s.outputs.values['coef_xb']
        coef_xc = s.outputs.values['coef_xc']

        reflectance_pixels = radiance_conv["B" + str(int(j) + 4)].copy()
        y = coef_xa * reflectance_pixels - coef_xb
        rayleigh_conv["B" + str(int(j) + 4)] = y/(1. + coef_xc * y)

    return(rayleigh_conv)



def scale_matrix(data, factor, plus):
    return( ((data - np.min(data))/(np.max(data) - np.min(data)) * factor) + plus )



def tif_save(data, save_path, ext):

    ruta_output = save_path + "/" + save_path.split('/')[-1] + "_" + ext + ".TIF"
    dt = data.copy()
    driver = gdal.GetDriverByName('GTiff')
    filas = dt.shape[0]
    colums = dt.shape[1]
    class_dt = driver.Create(ruta_output, colums, filas, eType=gdal.GDT_Float32)
    class_dt = class_dt.GetRasterBand(1).WriteArray(dt)


def save_tiff_wcrs(original_crs, data, path):

    perfil = original_crs.profile
    crs = original_crs.crs

    perfil.update(
        dtype = rasterio.float32,  
        count = 1,  
        compress = 'deflate', 
    )

    with rasterio.open(path, 'w', **perfil) as dst:
        dst.write(np.expand_dims(data, axis=0))  # Escribir el FAI en la banda 1
        # Establecer el CRS en el nuevo archivo
        dst.crs = crs




def Fai(lambda_data, rayleigh_data):

    lambda_red = lambda_data["B4"].copy()
    lambda_nir = lambda_data["B5"].copy()
    lambda_swir = lambda_data["B6"].copy()
  
    num = lambda_nir - lambda_red
    denom = lambda_swir - lambda_red
    adjust = np.divide(num, denom, out = np.zeros_like(num), where = denom != 0)
    
    R_nir = rayleigh_data["B4"] + (( rayleigh_data["B6"] - rayleigh_data["B4"] ) * adjust )
    fai = rayleigh_data["B5"] - R_nir

    return(fai)