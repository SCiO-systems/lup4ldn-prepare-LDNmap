import gdal
import numpy as np
import os
import sys
import requests
import json
import boto3
from botocore.exceptions import ClientError
import logging

s3 = boto3.client('s3')


def lambda_handler(event, context):

    body = json.loads(event['body'])
    json_file = body
    #get  input json and extract geojson
    try:
        project_id = json_file["project_id"]
        polygon_list = json_file["polygons_list"]
        roi_shape = json_file["ROI"]
        if roi_shape==None:
            roi_shape = requests.get(json_file["ROI_file_url"])
            roi_shape = json.loads(roi_shape.text) #.replace("'",'"')
        land_degradation_url = json_file["land_degradation"]
    except Exception as e:
        print("Input JSON field have an error.")
        raise(e)
    
    #for aws
    path_to_tmp = "/tmp/"


    def get_bucket_from_URL(url):
        part1 = url.split(".s3.")[0]
        part2 = part1.split("https://")[1]
        return part2
    
    def get_object_from_URL(url):
        part2 = url.split(".amazonaws.com/")[1]
        return part2

    def get_server_from_URL(url):
        part1 = url.split(".s3.")[1]
        part2 = part1.split(".amazonaws.com")[0]
        return part2
    
    target_bucket = get_bucket_from_URL(land_degradation_url)
    object_name = get_object_from_URL(land_degradation_url)
    path_to_local_save_file = path_to_tmp + "tmp_file.tif"
    
    
    try:
        response = s3.download_file(target_bucket, object_name, path_to_local_save_file)
    except ClientError as e:
        logging.error(e)
        return {
            "statusCode": 500,
            "body": json.dumps(e)
        }
        
    
    # READ FILE
    try:
        my_array_tif = gdal.Open(path_to_local_save_file)
        my_array = my_array_tif.ReadAsArray() #*0 if only the intersecting areas to be shown
        final_mask = np.zeros(my_array.shape)
        
    except Exception as e:
        print("if ''NoneType' object has no attribute', probably the file path is wrong")
        return {
            "statusCode": 500,
            "body": json.dumps(e)
        }
        
    for idx,polygon in enumerate(polygon_list):
        if polygon["polygon"]==None:
            pol = requests.get(polygon["polygon_url"])
            pol = json.loads(pol.text) #.replace("'",'"')
            
        input_json_for_API = {
            "project_id" : project_id,
            "polygon" : pol,
            "ROI" : roi_shape
            }
        
        response = requests.post("https://lambda.qvantum.polygons-intersection.scio.services", json = input_json_for_API)
        
        if response.text=="not intersecting geometries":
            continue
        else:
            json_file = response.json()
            
            with open(path_to_tmp + "inersection_file_" + str(idx) + ".json", 'w') as f:
                json.dump(json_file, f)
                        
            gdal_warp_kwargs_target_area = {
                'format': 'GTiff',
                'cutlineDSName' : json.dumps(json_file),
                'cropToCutline' : False,
                'height' : None,
                'width' : None,
                'srcNodata' : -32768.0,
                'dstNodata' : -32768.0,
                'creationOptions' : ['COMPRESS=LZW']
            }
            
        save_intersection_path = path_to_tmp + "inersection_file_" + str(idx) + ".tif"
        try:
            gdal.Warp(save_intersection_path,path_to_local_save_file, **gdal_warp_kwargs_target_area)
        except Exception as e:
            print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
            return {
                "statusCode": 500,
                "body": json.dumps(e)
            }
            
        try:
            intersect_area_tif = gdal.Open(save_intersection_path)
            intersect_area_array = intersect_area_tif.ReadAsArray()
            intersect_mask = np.where(intersect_area_array!=-32768,1,0)
            intersect_area_array = np.where(intersect_area_array!=-32768,polygon["value"],0)
            
            
            my_array = np.where(intersect_mask,intersect_area_array,my_array) 
            # 1st uncomment for only the intersecting areas to be shown
            # final_mask = np.logical_or(final_mask,intersect_mask)
        except Exception as e:
            print("if ''NoneType' object has no attribute', probably the file path is wrong")    
            return {
                "statusCode": 500,
                "body": json.dumps(e)
            }
            
    # 2nd uncomment for only the intersecting areas to be shown            
    # my_array = np.where(final_mask,my_array,-32768)

    def save_arrays_to_tif(output_tif_path, array_to_save, old_raster_used_for_projection):
    
        if len(array_to_save.shape)==2:
            array_to_save = np.expand_dims(array_to_save,axis=0)
    
        no_bands, width, height = array_to_save.shape
        
        gt = old_raster_used_for_projection.GetGeoTransform()
        wkt_projection = old_raster_used_for_projection.GetProjectionRef()
    
        driver = gdal.GetDriverByName("GTiff")
        DataSet = driver.Create(output_tif_path, height, width, no_bands, gdal.GDT_Int16,['COMPRESS=LZW']) #gdal.GDT_Int16
    
        #for wgs84 covering the whole world
        # geo_trans = (-180.0,360.0/height,0.0,90,0.0,-180/width)
        DataSet.SetGeoTransform(gt)
        DataSet.SetProjection(wkt_projection)
    
    
        #no data value
        ndval = -32768
        for i, image in enumerate(array_to_save, 1):
            DataSet.GetRasterBand(i).WriteArray(image)
            DataSet.GetRasterBand(i).SetNoDataValue(ndval)
        DataSet = None
        return
    
    
    ldn_map_save_path = path_to_tmp + "cropped_ldn_map.tif"
    
    save_arrays_to_tif(ldn_map_save_path, my_array,my_array_tif)
    

    object_name = project_id + "/" + "cropped_ldn_map.tif"
    # Upload the file
    try:
        response = s3.upload_file(ldn_map_save_path, target_bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return {
            "statusCode": 500,
            "body": json.dumps(e)
                }

    s3_lambda_path = "https://" + target_bucket + ".s3.us-east-2.amazonaws.com/"    
    
    my_output = {
        "ldn_map" :  s3_lambda_path + object_name
        }
    return {
        "statusCode": 200,
        "body": json.dumps(my_output)
        }  