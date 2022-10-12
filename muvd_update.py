#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:     intern
#
# Author:      rnicolescu
#
# Created:     17/05/2022
# Copyright:   (c) rnicolescu 2022
# Licence:     intern
#-------------------------------------------------------------------------------
#### All input files are stored localy and not need to be uploaded here. 
print "Arcpy library loading..."

from arcpy import env
from xlrd import open_workbook
from collections import OrderedDict
import arcpy
import os
import glob
import time
import xlrd
import csv

print "Arcpy loaded!"
time1 = time.time()
print "---> Loading shapefiles..."

# Globale
ROOT_PATH = os.getcwd()
muvd_path = os.path.join(ROOT_PATH, "GM2-DEF-DDP-00032_-B_Modele_Donnees_VN4-MUVD.xlsx") # here can  be added the xlsx model same as the input model source
csv_path = os.path.join(ROOT_PATH, "processing_muvd.csv")
csv_out_path = os.path.join(ROOT_PATH, "output.csv")


#### Here I have extracted from the document the columns that I need to be processed
if not os.path.exists(os.path.join(ROOT_PATH, csv_path)):
    book = open_workbook(os.path.join(ROOT_PATH, muvd_path), encoding_override="utf-8")
    sheet = book.sheet_by_index(2)
    sheetdict = {}

    for rownum in range(sheet.nrows):
        sheetdict[sheet.cell(rownum,3)] = [sheet.cell(rownum, 3),sheet.cell(rownum, 7), sheet.cell(rownum, 8), sheet.cell(rownum, 12),sheet.cell(rownum, 15),
                                           sheet.cell(rownum, 18),sheet.cell(rownum, 19), sheet.cell(rownum, 20), sheet.cell(rownum, 21), sheet.cell(rownum, 22),
                                           sheet.cell(rownum,23), sheet.cell(rownum,26)]

    with open(os.path.join(ROOT_PATH,"processing_muvd.csv") , "w") as file:
        for value in sheetdict.values():
            file.write("{}".format(value).replace("text:u", "").replace("'", "").replace("[", "").replace("]", "").replace(" ","").replace("empty:", "") + "\n".replace("u", "").replace("number:", "").replace(".0", ""))

# Bellow it can be modified if the output .csv is other than expected

text = open(os.path.join(ROOT_PATH, csv_path), "r")
text = ''.join([i for i in text])
if "number:" in text or ".0" in text:
    text = text.replace("number:", "")
    text = text.replace(".0", "")
    # # output.csv is the output file opened in write mode
x = open(os.path.join(ROOT_PATH,"output.csv"), "w")
# all the replaced text is written in the output.csv file
x.writelines(text)
os.unlink(os.path.join(ROOT_PATH, csv_path))
x.close()

# source = r"C:\New folder\GMAPSCOTO5VN4GF00SHP10"

source = raw_input("Please add the path of shapefiles to process:")
if source[-1] == "\\":
    source = source[0:-1]

# main_source = os.path.join(*source.split("\\")[0:-1]).replace(":", ":\\") + "\\{}___result".format(source.split("\\")[-1]) # path with a folder up than original
main_source =  source.split("\\")[-1].replace(":", "\\") + "__result"
delta_source = os.path.join(main_source, "DELTA")

print "---> Converting CART_TRACK_C, ROAD_C, TRAIL_C in LAND_TRANSPORTATION_WAY. Please wait..."

def merge_shapefiles():
    global ROOT_PATH, csv_out_path
    #Here it will merge 3 shapefiles into one like specifications
    tmp_list = ["CART_TRACK_C.shp", "ROAD_C.shp", "TRAIL_C.shp"]
    to_merge = []
    myPath = source
    for shp in tmp_list:
        shp_path = os.path.join(myPath, shp)
        if os.path.exists(shp_path):
            to_merge.append(shp_path)
    merge_path = os.path.join(main_source, "CAP075.shp")
    arcpy.Merge_management(to_merge, merge_path)


print "---> Creating destination folder and starting processing files. Please wait..."
def folder_shape():
    global main_source
    time1 = time.time()
    global shape_files, ROOT_PATH, source

    print "---> Loading shapefiles source..."

    if not os.path.exists(main_source):
        os.makedirs(main_source)

    env.workspace = os.path.join(source)
    env.overwriteOutput = True
    print "---> Loading workspace..."

    # # Here I have copied all the shapefiles to other folder and rename them like specifications
    print "---> Creating output folder and adding new shapefiles name..."
    with open(os.path.join(ROOT_PATH, "shape_convertors.txt")) as readFile:
        rename = [x.strip().split(",") for x in readFile]
        fc_list = arcpy.ListFeatureClasses()
        for old, new in rename:
            if old in fc_list:
                arcpy.Copy_management(old, main_source + "\\{}.shp".format(new))

    print "---> Creating DELTA output folder and adding new shapefiles name..."
    env.workspace = os.path.join(source, "DELTA")
    env.overwriteOutput = True
    if not os.path.exists(delta_source):
        os.makedirs(delta_source)
    for shpDlt in glob.glob(os.path.join(source, "DELTA")):
        fn = os.path.basename(shpDlt)
        with open(os.path.join(ROOT_PATH, "delta_converters.txt")) as readFile:
            rename = [x.strip().split(",") for x in readFile]
            fc_list = arcpy.ListFeatureClasses()
            shp_conv = "ADMINISTRATIVE_REGION_S.shp"
            for old, new in rename:
                if old in fc_list:
                    arcpy.Copy_management(old, delta_source + "\\{}.shp".format(new))
                    if old == shp_conv:
                        arcpy.Copy_management(old, delta_source + "\\{}.shp".format("SFA002"))


def add_fields():
    print "---> Adding new fields conf MUVD excel sheet. Please wait..."
    global shp_process, ROOT_PATH, csv_out_path, shape_files
    # Here I have process the .csv file and extract from it the columns that I need to be processed
    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        reader = csv.reader(file)

        env.workspace = main_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)

        # Here I verify each shapefile from putput to corespond with the name from the .csv file and add new fields
        exclude_fld = ["FNA", "CUD", "SSD", "UUI", "DSC", "IKO", "NFI", "VOI", "ADR", "CCN", "SDP"]
        tip_int = ["BNF", "DZC", "LTN", "NOS", "NPL"]
        tip_real = ["ZVH", "HGT", "LZN", "WID", "LNU", "WDU", "HCA", "UHC", "UBC", "CMO", "CSD", "DMT", "DZP", "WD1","WD2", "WD5", "RMW", "NWD", "ZVA"]
        for cell in reader:
            for key, vals in features.items():
                shp_cell = cell[0][-1] + cell[1] + ".shp"
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "FNA"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "CUD"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=20)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "SSD"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=20)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "UUI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=36)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "DSC"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "IKO"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=13)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "NFI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=18)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "VOI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=36)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "ADR"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "CCN"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "SDP"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] in tip_int):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="SHORT")
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] in tip_real):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="FLOAT")
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (not cell[2] in exclude_fld) and (not cell[2] in tip_int) and (not cell[2] in tip_real):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=80)

def update_fields():
    global shp_process, ROOT_PATH, csv_out_path, shape_files
    print "---> Updating fields.Please wait..."
    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        # Here I use the reader to complete all the fields with values like specifications from .csv file
        reader = csv.reader(file)

        env.workspace = main_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)

        for shp in glob.glob(main_source + "\\*.shp"):
            fc = os.path.basename(shp)
            fields = [x.name for x in arcpy.ListFields(fc)]

        bad_list = []
        for field in fields:
            with arcpy.da.SearchCursor(shp, field) as cursor:
                for row in cursor:
                    if row[0] in ["", None, " "]:
                        bad_list.append(field)
                        break

        # Here I have made attribute updates from the .csv file
        for cell in reader:
            shp_cell = cell[0][-1] + cell[1] + ".shp"
            extracted = cell[11][8:].split(":")
            for key, vals in features.items():
                if cell[11] != "":
                    if (cell[2] != "") and (shp_cell in key) and (cell[2] in extracted[0])  and (cell[2] != extracted[0]): #New fields take olf fields values
                        arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                    if (extracted[0] == "ZI005_NFN") and (cell[0][-1] + cell[1] + ".shp" in key):
                        arcpy.CalculateField_management(key, "NFI", "!ZI005_NFN!", "Python_9.3")
                    if (shp_cell in key) and (vals == cell[2]) and (len(extracted[0]) == 3 or len(extracted[0]) == 4) and (extracted[0] != cell[2]): # de schimbat inapoi cell[2] in vals
                         arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                    if (shp_cell in key) and (cell[2] != "") and  (len(extracted[0]) == 4) and (cell[2][:2] in extracted[0]) and (cell[9] == "1"): # FFN2 -> FFN_2, PPO2 -> PPO_2 + field updates
                        arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[9] == "1") and (cell[10] == "U"):  # Here I processed all values with U priority and update the values
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[9] == "1"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[10] == "X"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "M") and (cell[8] == "X") and (cell[9] == "1"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[2] in bad_list) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "M") and (cell[9] == "1") :
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[5] == "M") and (cell[6] == "M") and (cell[9] == "1"):
                    if (cell[4] == cell[9]) and (cell[5] == "M") and (cell[6] == "M"):
                        arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
        #
        # # Here, all the empty fields will be completed with values like specifications
        tip_int = ["BNF", "DZC", "LTN", "NOS", "NPL"]
        tip_real = ["ZVH", "HGT", "LZN", "WID", "LNU", "WDU", "HCA", "UHC", "UBC", "CMO", "CSD", "DMT", "DZP", "WD1", "WD2", "WD5", "RMW", "NWD", "ZVA"]

        # Am parcurs fiecare fisier shape din folderul ouput cu fisierul csv si am inceput modificarile sa faca update la atributele goale
        for shp in glob.glob(main_source + "\\*.shp"):
            fc = os.path.basename(shp)
            fields = arcpy.ListFields(fc)
            for field in fields:
                with arcpy.da.UpdateCursor(shp, field.name) as cursor:
                    for row in cursor:
                        if (row[0] in ["", None]) and (not field.name in tip_int) and (not field.name in tip_real):
                            row[0] = "noInformation"
                        if (field.name in tip_int):
                            row[0] = -32767
                        if (field.name in tip_real):
                            row[0] = -32767.0
                        cursor.updateRow(row)

def delete_fields():
    global shp_process, ROOT_PATH, csv_out_path, shape_files

    print "---> Deleting fields from shapefiles that are not in MUVD excel sheet. Please wait..."
    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        # Here I have delete all remaining fields that are not in the .csv file
        reader = csv.reader(file)
        env.workspace = main_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        csv_dict = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)

        for cell in reader:
            shp_name = cell[0][-1] + cell[1] + ".shp"
            if not shp_name in csv_dict:
                csv_dict[shp_name] = {}
            csv_dict[shp_name][cell[2]] = ""

        for key in features.keys():
            for fld in features[key]:
                if not fld in ["FID", "Shape"]:
                    if not fld in csv_dict[key]:
                        arcpy.DeleteField_management(in_table=key, drop_field=[fld])

def fcode_field():
    global ROOT_PATH, shp_process, shp_process, ROOT_PATH, csv_out_path, shape_files

    print "---> Adding 'FCODE' field conf MUVD excel sheet. Please wait..."
    for file in glob.glob(main_source + "\\*.shp"):
        fn = os.path.basename(file)
        arcpy.AddField_management(in_table=fn, field_name="FCODE", field_type="TEXT", field_length=6)
        expression = "!FCODE!.replace(!FCODE!,'" + str(fn[1:6]) + "')"
        arcpy.CalculateField_management(file, 'FCODE', expression, 'PYTHON_9.3')

### -- Here I repeated all the processes previously for the other folder
def delta_folder():
    global  ROOT_PATH, csv_out_path, shapeDELTA_files, shpDelta_process

    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        reader = csv.reader(file)

        env.workspace = delta_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)
        exclude_fld = ["FNA", "CUD", "SSD", "UUI", "DSC", "IKO", "NFI", "VOI", "ADR", "CCN", "SDP"]
        tip_int = ["BNF", "DZC", "LTN", "NOS", "NPL"]
        tip_real = ["ZVH", "HGT", "LZN", "WID", "LNU", "WDU", "HCA", "UHC", "UBC", "CMO", "CSD", "DMT", "DZP", "WD1","WD2", "WD5", "RMW", "NWD", "ZVA"]
        for cell in reader:
            for key, vals in features.items():
                shp_cell = cell[0][-1] + cell[1] + ".shp"
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "FNA"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "CUD"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=20)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "SSD"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=20)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "UUI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=36)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "DSC"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "IKO"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=13)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "NFI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=18)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "VOI"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=36)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "ADR"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "CCN"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] == "SDP"):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=254)
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] in tip_int):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="SHORT")
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (cell[2] in tip_real):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="LONG")
                if (not cell[2] in vals) and (key == shp_cell) and (cell[5] == "M") and (not cell[2] == "") and (not cell[2] in exclude_fld) and (not cell[2] in tip_int) and (not cell[2] in tip_real):
                    arcpy.AddField_management(in_table=key, field_name=cell[2], field_type="TEXT", field_length=80)


    print "---> Updating DELTA shapefiles fields.Please wait..."
    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        reader = csv.reader(file)
        env.workspace = delta_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)

        for shp in glob.glob(delta_source + "\\*.shp"):
            fc = os.path.basename(shp)
            fields = [x.name for x in arcpy.ListFields(fc)]

        bad_list = []
        for field in fields:
            with arcpy.da.SearchCursor(shp, field) as cursor:
                for row in cursor:
                    if row[0] in ["", None, " "]:
                        bad_list.append(field)
                        break

        for cell in reader:
            shp_cell = cell[0][-1] + cell[1] + ".shp"
            extracted = cell[11][8:].split(":")
            for key, vals in features.items():
                if cell[11] != "":
                    if (extracted[0] == "ZI005_FNA") and (cell[0][-1] + cell[1] + ".shp" in key):
                        arcpy.CalculateField_management(key, "FNA", "!ZI005_FNA1!", "Python_9.3")
                    if (cell[2] != "") and (shp_cell in key) and (cell[2] in extracted[0]) and (cell[2] != extracted[0]) and ( extracted[0] != "ZI005_FNA"):
                        arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                    if (extracted[0] == "ZI005_NFN") and (cell[0][-1] + cell[1] + ".shp" in key):
                        arcpy.CalculateField_management(key, "NFI", "!ZI005_NFN1!", "Python_9.3")
                    if (shp_cell in key) and (vals == cell[2]) and (len(extracted[0]) == 3 or len(extracted[0]) == 4) and (extracted[0] != cell[2]):
                        arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                    if (shp_cell in key) and (cell[2] != "") and (len(extracted[0]) == 4) and (cell[2][:2] in extracted[0]) and (cell[9] == "1"):
                        arcpy.CalculateField_management(key, cell[2], "!" + extracted[0] + "!", "Python_9.3")
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[9] == "1") and (cell[10] == "U"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[9] == "1"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "O") and (cell[10] == "X"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[4] != "") and (cell[5] == "M") and (
                        cell[6] == "M") and (cell[8] == "X") and (cell[9] == "1"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[2] in bad_list) and (cell[4] != "") and (cell[5] == "M") and (cell[6] == "M") and (cell[9] == "1"):
                    arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')
                if (shp_cell in key) and (cell[2] in vals) and (cell[5] == "M") and (cell[6] == "M") and (cell[9] == "1"):
                    if (cell[4] == cell[9]) and (cell[5] == "M") and (cell[6] == "M"):
                        arcpy.CalculateField_management(key, cell[2], cell[4], 'PYTHON_9.3')


        tip_int = ["BNF", "DZC", "LTN", "NOS", "NPL"]
        tip_real = ["ZVH", "HGT", "LZN", "WID", "LNU", "WDU", "HCA", "UHC", "UBC", "CMO", "CSD", "DMT", "DZP","WD1", "WD2", "WD5", "RMW", "NWD", "ZVA"]
        for shp in glob.glob(delta_source + "\\*.shp"):
            fc = os.path.basename(shp)
            fields = arcpy.ListFields(fc)
            for field in fields:
                with arcpy.da.UpdateCursor(shp, field.name) as cursor:
                    for row in cursor:
                        if (row[0] in ["", None]) and (not field.name in tip_int) and (not field.name in tip_real):
                            row[0] = "noInformation"
                        if (field.name in tip_int):
                            row[0] = -32767
                        if (field.name in tip_real):
                            row[0] = -32767.0
                        cursor.updateRow(row)


    print "---> Deleting unnecessary DELTA shapefiles fields. Please wait..."
    with open(os.path.join(ROOT_PATH, csv_out_path)) as file:
        reader = csv.reader(file)
        env.workspace = delta_source
        env.overwriteOutput = True

        fcs = arcpy.ListFeatureClasses()
        features = {}
        csv_dict = {}
        for fc in fcs:
            cursor = arcpy.da.SearchCursor(fc, "*")
            features[fc] = list(cursor.fields)

        fcodes = set()
        for cell in reader:
            shp_name = cell[0][-1] + cell[1] + ".shp"
            if not shp_name in csv_dict:
                csv_dict[shp_name] = {}
            csv_dict[shp_name][cell[2]] = ""
            for key in features.keys():
                if shp_name == key:
                    fcodes.add(shp_name)

        for key in features.keys():
            for fld in features[key]:
                if key in fcodes:
                    if not fld in ["FID", "Shape"]:
                        if not fld in csv_dict[key]:
                            arcpy.DeleteField_management(in_table=key, drop_field=[fld])

    print "---> Adding 'FCODE'  to DELTA shapefiles fields conf MUVD excel sheet. Please wait..."
    for file in glob.glob(delta_source + "\\*.shp"):
        fn = os.path.basename(file)
        arcpy.AddField_management(in_table=fn, field_name="FCODE", field_type="TEXT", field_length=6)
        expression = "!FCODE!.replace(!FCODE!,'" + str(fn[1:6]) + "')"
        arcpy.CalculateField_management(file, 'FCODE', expression, 'PYTHON_9.3')

def main():
    folder_shape()
    merge_shapefiles()
    add_fields()
    update_fields()
    delete_fields()
    fcode_field()
    delta_folder()
    excluded_ext = [".shp", ".prj", ".dbf", ".shx"]
    for file in glob.glob(main_source + "\\*"):
        if os.path.isfile(file):
            if not file[-4:] in excluded_ext:
                os.unlink(file)
    for file in glob.glob(delta_source + "\\*"):
        if os.path.isfile(file):
            if not file[-4:] in excluded_ext:
                os.unlink(file)


if __name__ == "__main__":
    main()
    time2 = time.time()
    total_time = time2 - time1
    print("Total time was: {:.2f} seconds".format(total_time))
    print("Script done!")
    exit = raw_input("Press ENTER <--| to exit...")
    print exit


####### Testing zone

