# python download_data.py --filetype='band' --sceneId='018046' --credPath='../oauth/api_key.yaml' --savePath='../data'
# python download_data.py --listScenes --filetype='band' --credPath='../auth/api_key.yaml' --savePath='../data/data_20150721' --scenesFile='../test/scenes'
import time
import re
import os
import threading
import datetime
import argparse
from general_utils import sendRequest, read_yaml_file, runDownload
#from landsat8.src.utils import sendRequest, read_yaml_file, runDownload

maxthreads = 7 # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time

threads = []

if __name__ == '__main__':     
    # User input    
    parser = argparse.ArgumentParser()
    parser.add_argument('-dt', '--date', required=True, help='date scene for download')
    parser.add_argument('-ft', '--filetype', required=True, choices=['bundle', 'band'], help='File types to download, "bundle" for bundle files and "band" for band files')
    parser.add_argument('-cp', '--credPath', required=True, help='local path with credentials in yaml format')
    parser.add_argument('-sp', '--savePath', required=True, help='Folder path for new products')
    parser.add_argument('-l', '--listScenes', default=False, action="store_true", help="Boolean: the scenes como from list")
    parser.add_argument('-sf', '--scenesFile', required=False, help='File with IDs od scenes for download')
    parser.add_argument('-id', '--sceneId', required=True, help='IDs od scenes for download')
    args = parser.parse_args()
    
    date_target      =  args.date
    filetype         =  args.filetype
    scenesFile       =  args.scenesFile # 'test/scenes.txt'
    sceneId			 =  args.sceneId
    path             =  args.savePath + "/" + sceneId # "data/"
    credentials_path =  args.credPath
    listScenes       =  args.listScenes
    credentials      =  read_yaml_file(credentials_path) # 'landsat8/auth/api_key.yaml'
    
    username    =  credentials['usr']
    password    =  credentials['pwd']
    serviceUrl  =  credentials['host_m2m']

    print("\nRunning Scripts...\n")
    print(date_target)
    print("... \n ...")
    #print(date_target + datetime.timedelta(days = 1))
    startTime = time.time()
        
    # Login
    payload = {'username' : username, 'password' : password}    
    apiKey = sendRequest(serviceUrl + "login", payload)    
    print("API Key: " + apiKey + "\n")
    
    if listScenes == True:

        # Read scenes
        f = open(scenesFile, "r")
        lines = f.readlines()   
        f.close()
        header = lines[0].strip()
        datasetName = header[:header.find("|")]
        idField = header[header.find("|")+1:]
    
        print("Scenes details:")
        print(f"Dataset name: {datasetName}")
        print(f"Id field: {idField}\n")

        entityIds = []
    
        lines.pop(0)
        for line in lines:        
            entityIds.append(line.strip())


    else:
        # Search scenes     
        datasetName = 'landsat_ot_c2_l1'
        idField = 'displayId'
        payload = {
            'datasetName' : datasetName,        # 'landsat_ot_c2_l1', # dataset alias  olitirs8_collection_2_l2
            'maxResults' : 10,                  # max results to return
            'startingNumber' : 1, 
            'sceneFilter' : {
                "spatialFilter": {
                    "filterType": "mbr",
                    "lowerLeft": {
                        "latitude": 19.17620,
                        "longitude": -87.5069
                    },
                    "upperRight": {
                        "latitude": 21.26945,
                        "longitude": -79.0#-85.3191
                    }
                },
                "acquisitionFilter": {
                    "end": date_target, 
                    "start": date_target
                    # "end": datetime.datetime.today().strftime("%Y-%m-%d"), 
                    # "start": datetime.datetime.today().strftime("%Y-%m-%d")
                }
            } # scene filter
        }

        print("\n Entity Ids founded:\n")
        results = sendRequest(serviceUrl + "scene-search", payload, apiKey)  
        #results
        
        entityIds = []
        for result in results["results"]:
            print(result['displayId'])
            entityIds.append(result['displayId'])

        # Checking availability of data
        if len(entityIds) == 0:
            raise IndexError("\nScene is not available yet. Try again later\n")
        
        wrs_pattern = re.compile(fr".*_{sceneId}")
        entityIds = [s for s in entityIds if wrs_pattern.match(s)]
        print("\n Entity Ids filtered:\n")
        print(entityIds)
    
    # Add scenes to a list
    listId = f"temp_{datasetName}_list" # customized list id
    payload = {
        "listId": listId,
        'idField' : idField,
        "entityIds": entityIds,
        "datasetName": datasetName
    }

    print("\n Adding scenes to list...\n")
    count = sendRequest(serviceUrl + "scene-list-add", payload, apiKey)    
    print("Added", count, "scenes\n")
    
    # Get download options
    payload = {
        "listId": listId,
        "datasetName": datasetName
    }
      
    print("Getting product download options...\n")
    products = sendRequest(serviceUrl + "download-options", payload, apiKey)
    print("Got product download options\n")
    
    # Select products
    downloads = []
    if filetype == 'bundle':
        # select bundle files
        for product in products:        
            if product["bulkAvailable"]:               
                downloads.append({"entityId":product["entityId"], "productId":product["id"]})
    elif filetype == 'band':
        # select band files
        for product in products:  
            if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                for secondaryDownload in product["secondaryDownloads"]:
                    if secondaryDownload["bulkAvailable"]:
                        downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
    else:
        # select all available files
        for product in products:        
            if product["bulkAvailable"]:               
                downloads.append({"entityId":product["entityId"], "productId":product["id"]})
                if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                    for secondaryDownload in product["secondaryDownloads"]:
                        if secondaryDownload["bulkAvailable"]:
                            downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
    
    if len(downloads) == 0:
        raise IndexError("\nScene is not available yet. Try again later\n")

    
    # Remove duplicates and unnecesary files
    filtered_downloads = []
    unique_downloads = [dict(t) for t in {tuple(d.items()) for d in downloads}]
    for i in unique_downloads:
        for ext in ['B4_TIF', 'B5_TIF', 'B6_TIF', 'MTL_TXT']:
            if ext in i['entityId']:
                filtered_downloads.append(i)
    downloads = filtered_downloads.copy()
    
    # Remove the list
    payload = {
        "listId": listId
    }
    sendRequest(serviceUrl + "scene-list-remove", payload, apiKey)                
    
    # Send download-request
    payLoad = {
        "downloads": downloads,
        "label": label,
        'returnAvailable': True
    }
    
    folder = downloads[0]["entityId"][:-8]
    print("\n")
    new_file_path = path + "/" + folder
    new_folder_path = os.path.join(os.getcwd(), new_file_path)
    os.makedirs(new_folder_path)
    print(f"Creating folder: {new_folder_path}")
    time.sleep(3)
    
    print(f"Sending download request ...\n")
    results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
    print(f"Done sending download request\n") 

      
    for result in results['availableDownloads']:       
        print(f"Get download url: {result['url']}\n" )
        runDownload(threads, result['url'], new_file_path)

    preparingDownloadCount = len(results['preparingDownloads'])
    preparingDownloadIds = []
    if preparingDownloadCount > 0:
        for result in results['preparingDownloads']:  
            preparingDownloadIds.append(result['downloadId'])
  
        payload = {"label" : label}                
        # Retrieve download urls
        print("Retrieving download urls...\n")
        results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
        if results != False:
            for result in results['available']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n" )
                    runDownload(threads, result['url'], new_file_path)
                
            for result in results['requested']:   
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n" )
                    runDownload(threads, result['url'], new_file_path)
        
        # Don't get all download urls, retrieve again after 30 seconds
        while len(preparingDownloadIds) > 0: 
            print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
            time.sleep(30)
            results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
            if results != False:
                for result in results['available']:                            
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        print(f"Get download url: {result['url']}\n" )
                        runDownload(threads, result['url'], new_file_path)
    
    print("\nGot download urls for all downloads\n")                
    # Logout
    endpoint = "logout"  
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:        
        print("Logged Out\n")
    else:
        print("Logout Failed\n")  
     
    print("Downloading files... Please do not close the program\n")
    for thread in threads:
        thread.join()
            
    print("Complete Downloading")
    
    executionTime = round((time.time() - startTime), 2)
    print(f'Total time: {executionTime} seconds')



