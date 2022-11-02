# python download_data.py --filetype='band' --credPath='../auth/api_key.yaml' --savePath='../data/data_20201020'
# python download_data.py --listScenes --filetype='band' --credPath='../auth/api_key.yaml' --savePath='../data/data_20150721' --scenesFile='../test/scenes'
import time
import re
import threading
import datetime
import argparse
from utils import sendRequest, read_yaml_file, runDownload
#from landsat8.src.utils import sendRequest, read_yaml_file, runDownload

maxthreads = 7 # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time

threads = []
# The entityIds/displayIds need to save to a text file such as scenes.txt.
# The header of text file should follow the format: datasetName|displayId or datasetName|entityId. 
# landsat_ot_c2_l2|displayId
# LC08_L2SP_012025_20201231_20210308_02_T1
# LC08_L2SP_012027_20201215_20210314_02_T1


if __name__ == '__main__':     
    # User input    
    parser = argparse.ArgumentParser()
    parser.add_argument('-ft', '--filetype', required=True, choices=['bundle', 'band'], help='File types to download, "bundle" for bundle files and "band" for band files')
    parser.add_argument('-cp', '--credPath', required=True, help='local path with credentials in yaml format')
    parser.add_argument('-sp', '--savePath', required=True, help='Folder path for new products')
    parser.add_argument('-l', '--listScenes', default=False, action="store_true", help="Boolean: the scenes como from list")
    parser.add_argument('-sf', '--scenesFile', required=False, help='File with IDs od scenes for download')
    args = parser.parse_args()
    
    filetype         =  args.filetype
    scenesFile       =  args.scenesFile # 'test/scenes.txt'
    path             =  args.savePath # "data/"
    credentials_path =  args.credPath
    listScenes       =  args.listScenes
    # filetype         =  'band'
    # scenesFile       =  'landsat8/test/scenes.txt'
    # credentials      =  read_yaml_file('landsat8/auth/api_key.yaml')
    credentials      =  read_yaml_file(credentials_path) # 'landsat8/auth/api_key.yaml'
    username    =  credentials['usr']
    password    =  credentials['pwd']
    serviceUrl  =  credentials['host_m2m']

    print("\nRunning Scripts...\n")
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
                        "longitude": -85.3191
                    }
                },
                "acquisitionFilter": {
                    "end": "2022-10-20", 
                    "start": "2022-10-20"
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
        
        
        wrs_pattern = re.compile(r".*_018046")
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
    
    print(f"Sending download request ...\n")
    results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
    print(f"Done sending download request\n") 

      
    for result in results['availableDownloads']:       
        print(f"Get download url: {result['url']}\n" )
        runDownload(threads, result['url'], path)

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
                    runDownload(threads, result['url'], path)
                
            for result in results['requested']:   
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n" )
                    runDownload(threads, result['url'], path)
        
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
                        runDownload(threads, result['url'], path)
    
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



