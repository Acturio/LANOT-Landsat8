import json
import requests
import sys
import re
import threading
import yaml
import glob
import numpy as np
from Py6S import *
from osgeo import gdal
from natsort import natsorted

#path = "others/scripts/data_20221020/" # Fill a valid download path
maxthreads = 7 # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)

threads = []
#scenesFile = 'others/scripts/scenes.txt'

def read_yaml_file(yaml_file):
    """
    Load yaml cofigurations
    :param: file_name
    :return: cofigurations
    """
    config = None
    try:
        with open(yaml_file, 'r') as f:
            credentials = yaml.safe_load(f)
            config = credentials['glovis_api']
    except:
        raise FileNotFoundError('Could not load the file')

    return config

#credentials = read_yaml_file('landsat8/auth/api_key.yaml')
#username = credentials['usr']
#password = credentials['pwd']
#serviceUrl = credentials['host_m2m']
#host = credentials['host_espa']


# Send http request
def sendRequest(url, data, apiKey = None, exitIfNoResponse = True):
    json_data = json.dumps(data)
    
    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}              
        response = requests.post(url, json_data, headers = headers)  
    
    try:
      httpStatusCode = response.status_code 
      if response == None:
          print("No output from service")
          if exitIfNoResponse: sys.exit()
          else: return False
      output = json.loads(response.text)
      if output['errorCode'] != None:
          print(output['errorCode'], "- ", output['errorMessage'])
          if exitIfNoResponse: sys.exit()
          else: return False
      if  httpStatusCode == 404:
          print("404 Not Found")
          if exitIfNoResponse: sys.exit()
          else: return False
      elif httpStatusCode == 401: 
          print("401 Unauthorized")
          if exitIfNoResponse: sys.exit()
          else: return False
      elif httpStatusCode == 400:
          print("Error Code", httpStatusCode)
          if exitIfNoResponse: sys.exit()
          else: return False
    except Exception as e: 
          response.close()
          print(e)
          if exitIfNoResponse: sys.exit()
          else: return False
    response.close()
    
    return output['data']

def downloadFile(url, path):
    sema.acquire()
    try:        
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        if path != "" and path[-1] != "/":
            filename = "/" + filename
        open(path+filename, 'wb').write(response.content)
        print(f"Downloaded {filename}\n")
        sema.release()
    except Exception as e:
        print(f"Failed to download from {url}. Will try to re-download.")
        sema.release()
        runDownload(threads, url, path)
    
def runDownload(threads, url, path):
    thread = threading.Thread(target=downloadFile, args=(url, path))
    threads.append(thread)
    thread.start()

def espa_api(endpoint, verb='get', body=None, uauth=None):
    """ Suggested simple way to interact with the ESPA JSON REST API """
    auth_tup = uauth if uauth else (username, password)
    response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=body)
    print('{} {}'.format(response.status_code, response.reason))
    data = response.json()
    if isinstance(data, dict):
        messages = data.pop("messages", None)  
        if messages:
            print(json.dumps(messages, indent=4))
    try:
        response.raise_for_status()
    except Exception as e:
        print(e)
        return None
    else:
        return data


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
                lines_ref_mult.append((linenum, line.rstrip('\n')))

            if pattern_ref_add.search(line) != None:
                linenum += 1
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
                lines_rad_mult.append((linenum, line.rstrip('\n')))

            if pattern_rad_add.search(line) != None:
                linenum += 1
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
        nir_img = gdal.Open(band_pathfiles[i])
        band = nir_img.ReadAsArray()
        files["B" + str(int(i)+1)] = band
        reflectance_mult_band = float(ref_mult_params[i][1].split(" = ")[1])
        reflectance_add_band = float(ref_add_params[i][1].split(" = ")[1])
        reflectance_conv["B"+str(int(i)+1)] = (reflectance_mult_band * band) + reflectance_add_band
        print("Band " + str(int(i)+1)) 
        print("Reflectance multiplicative factor: " + str(reflectance_mult_band))
        print("Reflectance additive factor: " + str(reflectance_add_band) + "\n")

    return(reflectance_conv)



def radiance_transformation(rad_mult_params, rad_add_params, band_pathfiles):

    files = {}
    radiance_conv = {}

    # Radiance
    print("\n Initializing radiance conversion... \n")
    for i in range(0, len(rad_mult_params)):
        nir_img = gdal.Open(band_pathfiles[i])
        band = nir_img.ReadAsArray()
        files["B"+str(int(i)+1)] = band
        radiance_mult_band = float(rad_mult_params[i][1].split(" = ")[1])
        radiance_add_band = float(rad_add_params[i][1].split(" = ")[1])
        radiance_conv["B"+str(int(i)+1)] = (radiance_mult_band * band) + radiance_add_band
        print("Band " + str(int(i)+1)) 
        print("Radiance multiplicative factor: " + str(radiance_mult_band))
        print("Radiance additive factor: " + str(radiance_add_band) + "\n")

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

    wavelengths = [0.44, 0.48, 0.56, 0.660, 0.865, 1.60] 
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

        reflectance_pixels = radiance_conv["B" + str(int(j) + 1)].copy()
        y = coef_xa * reflectance_pixels - coef_xb
        rayleigh_conv["B" + str(int(j) + 1)] = y/(1. + coef_xc * y)

    return(rayleigh_conv)



def Fai(lambda_data, rayleigh_data):

  lambda_red = lambda_data["B4"].copy()
  lambda_nir = lambda_data["B5"].copy()
  lambda_swir = lambda_data["B6"].copy()
  
  num = lambda_nir - lambda_red
  denom = lambda_swir - lambda_red
  adjust = np.divide(num, denom, out = np.ones_like(num), where = denom!=0)
  # adjust = (num) / (denom) # np.divide(num, denom, out = np.zeros_like(num), where = denom!=0)

  R_nir = rayleigh_data["B4"] + (( rayleigh_data["B6"] - rayleigh_data["B4"] ) * adjust*10 )
  fai = rayleigh_data["B5"] - R_nir

  return(fai)