from src.utils import *
import matplotlib.pyplot as plt
import ipython_genutils
import numpy as np

ruta_archivos = "../017047/LC08_L1GT_017047_20150103_20170302_01_T2"

lines_ref_add, lines_ref_mult = get_reflectance_parameters(ruta_archivos)
lines_rad_add, lines_rad_mult = get_radiance_parameters(ruta_archivos)

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
    latitude = np.mean(latitude_corners),
    longitude = np.mean(longitude_corners),   
    solar_a = scene_sun_azimuth
    )

tif_save(data = reflectance_conv['B4'], path = ruta_archivos, ext = "B4_RAC")
tif_save(data = reflectance_conv['B5'], path = ruta_archivos, ext = "B5_RAC")
tif_save(data = reflectance_conv['B6'], path = ruta_archivos, ext = "B6_RAC")


fig, axes = plt.subplots(nrows=1, ncols=2)

# Ploteamos la primera imagen en el lado izquierdo
axes[0].imshow( ((files["B1"] - np.mean(files["B1"]))/(np.std(files["B1"]) )) )
axes[0].set_title("Band 1 - Original") # Titulo
axes[0].set_ylabel("Latitude") # nombre del eje Y
axes[0].set_xlabel("Longitude") # nombre del eje X

# Ploteamos la segunda imagen en el lado derecho
axes[1].imshow( fai_reflectance )
axes[1].set_title("Floating Algae Index") # Titulo
axes[1].set_ylabel("Latitude") # nombre del eje Y
axes[1].set_xlabel("Longitude") # nombre del eje X
plt.show()


fai_reflectance = Fai(lambda_data = reflectance_conv, rayleigh_data = rayleigh_conv)

fai_reflectance_std = (fai_reflectance - np.mean(fai_reflectance))/(np.std(fai_reflectance) )
fai_reflectance_rescaled = scale_matrix(fai_reflectance_std, factor = 1000, plus = 0)

np.quantile(fai_reflectance, [0, .01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1])


tif_save(data = fai_reflectance, path = ruta_archivos, ext = "FAI")

