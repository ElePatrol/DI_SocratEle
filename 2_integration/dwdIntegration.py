import os
import sqlite3
from calendar import monthrange


def getVarLength(file, header_length):
    smallest_length = []
    smallest_value = []
    biggest_length = []
    biggest_value = []
    for x in range(header_length):
        smallest_length.append(999)
        smallest_value.append("")
        biggest_length.append(0)
        biggest_value.append("")
    with open(file, 'r') as db_content:
        # skip the header
        db_line_raw = db_content.readline()

        for db_line_raw in db_content:
            db_line = db_line_raw.split(";")
            for i, data in enumerate(db_line):
                if(i < header_length):
                    data = data.replace("\n", "")
                    if(len(data) < smallest_length[i]):
                        smallest_length[i] = len(data)
                        smallest_value[i] = data
                    elif(len(data) > biggest_length[i]):
                        biggest_length[i] = len(data)
                        biggest_value[i] = data
        db_content.close()

    print("Smallest lenth: ", smallest_length)
    for value in smallest_value:
        print(value)
    print("Biggest lenth: ", biggest_length)
    for value in biggest_value:
        print(value)


def writeAllDataInDatabase(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #
    #  Metadaten_Fehlwerte
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_fehlwerte = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Parameter": 3,
        "Gesamt_Fehlwerte": 1,
        "Beschreibung": 3
    }
    index_fehlwerte = {
        0: 0,
        1: 99,
        2: 3,
        3: 1,
        4: 2,
        5: 4,
        6: 5,
    }
    dataToDatabase("Metadaten_Fehlwerte", header_fehlwerte, index_fehlwerte,
                   "Metadaten_Fehlwerte_processed_combined.csv", cur)

    #
    #  Metadaten_Geographie_2
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_geographie_2 = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Stationshoehe": 2,
        "Geogr.Breite": 2,
        "Geogr.Laenge": 2
    }
    index_geographie_2 = {
        0: 0,
        1: 3,
        2: 4,
        3: 5,
        4: 1,
        5: 2,
        6: 99
    }
    dataToDatabase("Metadaten_Geographie_2", header_geographie_2, index_geographie_2,
                   "Metadaten_Geographie_processed_combined.csv", cur)

    #
    #  Metadaten_Geraete_Geberhoehe (Temporäre database)
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_geraete_geberhoehe = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Geberhoehe": 2
    }
    index_geraete_geberhoehe = {
        0: 0,
        1: 99,
        2: 99,
        3: 99,
        4: 99,
        5: 3,
        6: 1,
        7: 2,
        8: 99,
        9: 99,
    }
    dataToDatabase("Metadaten_Geraete_Geberhoehe", header_geraete_geberhoehe, index_geraete_geberhoehe,
                   "Metadaten_Geraete_processed_combined.csv", cur)

    #
    #  Metadaten_Geraete
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_geraete = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Geraetetyp_Name": 3,
        "Messverfahren": 3
    }
    index_geraete = {
        0: 0,
        1: 99,
        2: 99,
        3: 99,
        4: 99,
        5: 99,
        6: 1,
        7: 2,
        8: 3,
        9: 4,
    }
    dataToDatabase("Metadaten_Geraete", header_geraete, index_geraete,
                   "Metadaten_Geraete_processed_combined.csv", cur)

    #
    #  Metadaten_Parameter
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_parameter = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Parameter": 3,
        "Parameterbeschreibung": 3,
        "Einheit": 3,
        "Datenquelle": 3,
        "Zusatz-Info": 3,
        "Besonderheiten": 3,
        "Literaturhinweis": 3,
    }
    index_parameter = {
        0: 0,
        1: 1,
        2: 2,
        3: 99,
        4: 3,
        5: 4,
        6: 5,
        7: 6,
        8: 7,
        9: 8,
        10: 9
    }
    dataToDatabase("Metadaten_Parameter", header_parameter, index_parameter,
                   "Metadaten_Parameter_processed_combined.csv", cur)

    #
    #  Metadaten_Stationsname
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_stationname = {
        "Stations_ID": 1,
        "Von_Datum": 4,
        "Bis_Datum": 4,
        "Stationsname": 3
    }
    index_stationname = {
        0: 0,
        1: 3,
        2: 1,
        3: 2
    }
    dataToDatabase("Metadaten_Stationsname", header_stationname, index_stationname,
                   "Metadaten_Stationsname_processed_combined.csv", cur)

    #
    #  air_temperature
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_air_temperature = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_9": 1,
        "TT_TU": 2,
        "RF_TU": 2
    }
    index_air_temperature = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("air_temperature", header_air_temperature, index_air_temperature,
                   "air_temperature\produkt_processed.csv", cur)

    #
    #  cloud_type
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_cloud_type = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "V_N": 1,
        "V_N_I": 3,
        "V_S1_CS": 1,
        "V_S1_CSA": 1,
        "V_S1_HHS": 1,
        "V_S1_NS": 1,
        "V_S2_CS": 1,
        "V_S2_CSA": 1,
        "V_S2_HHS": 1,
        "V_S2_NS": 1,
        "V_S3_CS": 1,
        "V_S3_CSA": 1,
        "V_S3_HHS": 1,
        "V_S3_NS": 1,
        "V_S4_CS": 1,
        "V_S4_CSA": 1,
        "V_S4_HHS": 1,
        "V_S4_NS": 1
    }
    index_cloud_type = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 6,
        7: 7,
        8: 8,
        9: 9,
        10: 10,
        11: 11,
        12: 12,
        13: 13,
        14: 14,
        15: 15,
        16: 16,
        17: 17,
        18: 18,
        19: 19,
        20: 20
    }
    dataToDatabase("cloud_type", header_cloud_type, index_cloud_type,
                   "cloud_type\produkt_processed.csv", cur)

    #
    #  cloudiness
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_cloudiness = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "V_N_I": 3,
        "V_N": 1
    }
    index_cloudiness = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("cloudiness", header_cloudiness, index_cloudiness,
                   "cloudiness\produkt_processed.csv", cur)

    #
    #  dew_point
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_dew_point = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "TT": 2,
        "TD": 2
    }
    index_dew_point = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("dew_point", header_dew_point, index_dew_point,
                   "dew_point\produkt_processed.csv", cur)

    #
    #  extreme_wind
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_extreme_wind = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "FX_911": 2
    }
    index_extreme_wind = {
        0: 0,
        1: 1,
        2: 2,
        3: 3
    }
    dataToDatabase("extreme_wind", header_extreme_wind, index_extreme_wind,
                   "extreme_wind\produkt_processed.csv", cur)

    #
    #  moisture
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_moisture = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "ABSF_STD": 2,
        "VP_STD": 2,
        "TF_STD": 2,
        "P_STD": 2,
        "TT_STD": 2,
        "RF_STD": 2,
        "TD_STD": 2
    }
    index_moisture = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 6,
        7: 7,
        8: 8,
        9: 9,
    }
    dataToDatabase("moisture", header_moisture, index_moisture,
                   "moisture\produkt_processed.csv", cur)

    #
    #  precipitation
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_precipitation = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "R1": 2,
        "RS_IND": 1,
        "WRTR": 1
    }
    index_precipitation = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5
    }
    dataToDatabase("precipitation", header_precipitation, index_precipitation,
                   "precipitation\produkt_processed.csv", cur)

    #
    #  pressure
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_pressure = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "P": 1,
        "P0": 1
    }
    index_pressure = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("pressure", header_pressure, index_pressure,
                   "pressure\produkt_processed.csv", cur)

    #
    #  soil_temperature
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_soil_temperature = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_2": 1,
        "V_TE002": 1,
        "V_TE005": 2,
        "V_TE010": 2,
        "V_TE020": 2,
        "V_TE050": 2,
        "V_TE100": 2
    }
    index_soil_temperature = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 6,
        7: 7,
        8: 8,
    }
    dataToDatabase("soil_temperature", header_soil_temperature, index_soil_temperature,
                   "soil_temperature\produkt_processed.csv", cur)

    #
    #  solar
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_solar = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_592": 1,
        "ATMO_LBERG": 1,
        "FD_LBERG": 2,
        "FG_LBERG": 2,
        "SD_LBERG": 1,
        "ZENIT": 4,
        "MESS_DATUM_WOZ": 1
    }
    index_solar = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 5,
        6: 6,
        7: 7,
        8: 8,
    }
    dataToDatabase("solar", header_solar, index_solar,
                   "solar\produkt_processed.csv", cur)

    #
    #  sun
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_sun = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_7": 1,
        "SD_SO": 2
    }
    index_sun = {
        0: 0,
        1: 1,
        2: 2,
        3: 3
    }
    dataToDatabase("sun", header_sun, index_sun,
                   "sun\produkt_processed.csv", cur)

    #
    #  visibility
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_visibility = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "V_VV_I": 3,
        "V_VV": 1
    }
    index_visibility = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("visibility", header_visibility, index_visibility,
                   "visibility\produkt_processed.csv", cur)

    #
    #  weather_phenomena
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_weather_phenomena = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "WW": 1,
        "WW_Text": 3
    }
    index_weather_phenomena = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("weather_phenomena", header_weather_phenomena, index_weather_phenomena,
                   "weather_phenomena\produkt_processed.csv", cur)

    #
    #  wind
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_wind = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_3": 1,
        "F": 2,
        "D": 1
    }
    index_wind = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("wind", header_wind, index_wind,
                   "wind\produkt_processed.csv", cur)

    #
    #  wind_synop
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date
    header_wind_synop = {
        "Stations_ID": 1,
        "MESS_DATUM": 4,
        "QN_8": 1,
        "FF": 2,
        "DD": 1
    }
    index_wind_synop = {
        0: 0,
        1: 1,
        2: 2,
        3: 3,
        4: 4
    }
    dataToDatabase("wind_synop", header_wind_synop, index_wind_synop,
                   "wind_synop\produkt_processed.csv", cur)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


def dataToDatabase(db_name, header, index, file, db_cur):
    print("Start processing:", db_name)
    utmConverter = Proj(proj='utm', zone=32, ellps='WGS84', units='m',
                        preserve_units=False)
    lon = 0
    lon_indx = 0
    lat = 0
    create_table = "CREATE TABLE IF NOT EXISTS " + \
        db_name + " (ID INTEGER PRIMARY KEY AUTOINCREMENT, "

    first = True
    for key in header.keys():
        if(first):
            first = False
        else:
            create_table += ", "

        create_table += "\"" + key + "\" "

        column_type = header[key]

        if(column_type == 1):  # int
            create_table += "INTEGER"
        elif(column_type == 2):  # float
            create_table += "REAL"
        elif(column_type == 3):  # string
            create_table += "TEXT"
        elif(column_type == 4):  # date
            create_table += "DATETIME"
        elif(column_type == 5):  # coordinate
            create_table += "REAL"

    create_table += ");"

    db_cur.execute(create_table)

    insert_table = "INSERT INTO " + db_name + "("

    first = True
    for key in header.keys():
        if(first):
            first = False
        else:
            insert_table += ", "

        insert_table += "\"" + key + "\""

    insert_table += ") values ("

    first = True
    for key in header.keys():
        if(first):
            first = False
        else:
            insert_table += ", "

        insert_table += "?"

    insert_table += ")"

    with open(file, 'r') as db_content:
        # skip the header
        db_line_raw = db_content.readline()

        for db_line_raw in db_content:
            next_column = []
            for x in range(len(header)):
                # when we have no data there will be a "NULL"
                next_column.append("Null")
            db_line_raw = db_line_raw.replace("\n", "")
            if(len(db_line_raw) > 0):
                db_line = db_line_raw.split(";")
                for i, data in enumerate(db_line):
                    if(i < len(index)):
                        if(index[i] != 99):
                            # remove empty spaces at the start
                            while((len(data) > 0) and (data[0] == " ")):
                                data = data[1:]

                            # remove empty spaces at the start
                            while((len(data) > 0) and (data[-1] == " ")):
                                data = data[:-1]

                            if(len(data) > 0):
                                column_type = list(header.values())[index[i]]
                                converted_data = 0

                                if(column_type == 1):  # 1: int
                                    converted_data = data
                                elif(column_type == 2):  # 2: float
                                    converted_data = data
                                elif(column_type == 3):  # 3: text
                                    converted_data = data
                                elif(column_type == 4):  # 4: date
                                    if "." in data:  # "17.07.2021-01:00"
                                        if((len(data) > 10) and (data[10] == "-")):
                                            converted_data = data[6:10] + "-" + \
                                                data[3:5] + "-" + \
                                                data[0:2] + "T" + data[11:16]
                                        else:  # "30.10.2020"
                                            converted_data = data[6:10] + \
                                                "-" + data[3:5] + \
                                                "-" + data[0:2] + "T00:00"
                                    else:
                                        if(len(data) > 8):
                                            if ":" in data:  # "2016010101:00"
                                                converted_data = data[0:4] + \
                                                    "-" + data[4:6] + "-" + \
                                                    data[6:8] + "T" + \
                                                    data[8:10] + ":" + \
                                                    data[11:13]
                                            else:   # "1950040103"
                                                converted_data = data[0:4] + \
                                                    "-" + data[4:6] + "-" + \
                                                    data[6:8] + "T" + \
                                                    data[8:10] + ":00"
                                        else:  # "19500401"
                                            converted_data = data[0:4] + \
                                                "-" + data[4:6] + "-" + \
                                                data[6:8] + "T00:00"
                                elif(column_type == 5):  # 5: coordinate
                                    converted_data = data.replace(",", ".")

                                next_column[index[i]] = converted_data

                db_cur.execute(insert_table, next_column)

        db_content.close()


def removeDuplicatesInDatabase(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #
    #  Metadaten_Fehlwerte
    #
    remove_fehlwerte_duplicates = \
        "DELETE FROM Metadaten_Fehlwerte " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Fehlwerte " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Parameter, Gesamt_Fehlwerte, Beschreibung)"
    cur.execute(remove_fehlwerte_duplicates)

    #
    #  Metadaten_Geographie_2
    #
    remove_geographie_2_duplicates = \
        "DELETE FROM Metadaten_Geographie_2 " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Geographie " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Stationshoehe, \"Geogr.Breite\", \"Geogr.Laenge\")"
    cur.execute(remove_geographie_2_duplicates)

    #
    #  Metadaten_Geraete
    #
    remove_geraete_duplicates = \
        "DELETE FROM Metadaten_Geraete " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Geraete " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Geraetetyp_Name, Messverfahren)"
    cur.execute(remove_geraete_duplicates)

    #
    #  Metadaten_Geraete_Geberhoehe
    #
    remove_geraete_geberhoehe_duplicates = \
        "DELETE FROM Metadaten_Geraete_Geberhoehe " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Geraete_Geberhoehe " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Geberhoehe)"
    cur.execute(remove_geraete_geberhoehe_duplicates)

    #
    #  Metadaten_Parameter
    #
    remove_parameter_duplicates = \
        "DELETE FROM Metadaten_Parameter " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Parameter " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Parameter, Parameterbeschreibung, Einheit, Datenquelle, \"Zusatz-Info\", Besonderheiten, Literaturhinweis)"
    cur.execute(remove_parameter_duplicates)

    #
    #  Metadaten_Stationsname
    #
    remove_stationsname_duplicates = \
        "DELETE FROM Metadaten_Stationsname " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM Metadaten_Stationsname " + \
        "     GROUP BY Stations_ID, Von_Datum, Bis_Datum, Stationsname)"
    cur.execute(remove_stationsname_duplicates)

    #
    #  air_temperature
    #
    remove_air_temperature_duplicates = \
        "DELETE FROM air_temperature " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM air_temperature " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_9, TT_TU, RF_TU)"
    cur.execute(remove_air_temperature_duplicates)

    #
    #  cloud_type
    #
    remove_cloud_type_duplicates = \
        "DELETE FROM cloud_type " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM cloud_type " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, V_N, V_N_I, V_S1_CS, V_S1_CSA, V_S1_HHS, V_S1_NS, V_S2_CS, V_S2_CSA, V_S2_HHS, V_S2_NS, V_S3_CS, V_S3_CSA, V_S3_HHS, V_S3_NS, V_S4_CS, V_S4_CSA, V_S4_HHS, V_S4_NS)"
    cur.execute(remove_cloud_type_duplicates)

    #
    #  cloudiness
    #
    remove_cloudiness_duplicates = \
        "DELETE FROM cloudiness " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM cloudiness " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, V_N_I, V_N)"
    cur.execute(remove_cloudiness_duplicates)

    #
    #  dew_point
    #
    remove_dew_point_duplicates = \
        "DELETE FROM dew_point " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM dew_point " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, TT, TD)"
    cur.execute(remove_dew_point_duplicates)

    #
    #  extreme_wind
    #
    remove_extreme_wind_duplicates = \
        "DELETE FROM extreme_wind " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM extreme_wind " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, FX_911)"
    cur.execute(remove_extreme_wind_duplicates)

    #
    #  moisture
    #
    remove_moisture_duplicates = \
        "DELETE FROM moisture " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM moisture " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, ABSF_STD, VP_STD, TF_STD, P_STD, TT_STD, RF_STD, TD_STD)"
    cur.execute(remove_moisture_duplicates)

    #
    #  precipitation
    #
    remove_precipitation_duplicates = \
        "DELETE FROM precipitation " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM precipitation " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, R1, RS_IND, WRTR)"
    cur.execute(remove_precipitation_duplicates)

    #
    #  pressure
    #
    remove_pressure_duplicates = \
        "DELETE FROM pressure " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM pressure " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, P, P0)"
    cur.execute(remove_pressure_duplicates)

    #
    #  soil_temperature
    #
    remove_soil_temperature_duplicates = \
        "DELETE FROM soil_temperature " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM soil_temperature " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_2, V_TE002, V_TE005, V_TE010, V_TE020, V_TE050, V_TE100)"
    cur.execute(remove_soil_temperature_duplicates)

    #
    #  solar
    #
    remove_solar_duplicates = \
        "DELETE FROM solar " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM solar " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_592, ATMO_LBERG, FD_LBERG, FG_LBERG, SD_LBERG, ZENIT, MESS_DATUM_WOZ)"
    cur.execute(remove_solar_duplicates)

    #
    #  sun
    #
    remove_sun_duplicates = \
        "DELETE FROM sun " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM sun " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_7, SD_SO)"
    cur.execute(remove_sun_duplicates)

    #
    #  visibility
    #
    remove_visibility_duplicates = \
        "DELETE FROM visibility " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM visibility " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, V_VV_I, V_VV)"
    cur.execute(remove_visibility_duplicates)

    #
    #  weather_phenomena
    #
    remove_weather_phenomena_duplicates = \
        "DELETE FROM weather_phenomena " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM weather_phenomena " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, WW, WW_Text)"
    cur.execute(remove_weather_phenomena_duplicates)

    #
    #  wind
    #
    remove_wind_duplicates = \
        "DELETE FROM wind " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM wind " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_3, F, D)"
    cur.execute(remove_wind_duplicates)

    #
    #  wind_synop
    #
    remove_wind_synop_duplicates = \
        "DELETE FROM wind_synop " + \
        "WHERE ID NOT IN " + \
        "    (SELECT ID " + \
        "     FROM wind_synop " + \
        "     GROUP BY Stations_ID, MESS_DATUM, QN_8, FF, DD)"
    cur.execute(remove_wind_synop_duplicates)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


def combineDuplicatesInDatabase(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #
    #  Metadaten_Geographie
    #

    header_geographie_2 = [
        "Stationshoehe",
        "Geogr.Breite",
        "Geogr.Laenge"
    ]
    combineDuplicates("Metadaten_Geographie_2", header_geographie_2, cur)

    #
    #  Metadaten_Geraete
    #

    header_geraete = [
        "Geraetetyp_Name",
        "Messverfahren"
    ]
    combineDuplicates("Metadaten_Geraete", header_geraete, cur)

    #
    #  Metadaten_Geraete_Geberhoehe
    #

    header_geraete_geberhoehe = [
        "Geberhoehe)"
    ]
    combineDuplicates("Metadaten_Geraete_Geberhoehe",
                      header_geraete_geberhoehe, cur)

    #
    #  Metadaten_Parameter
    #

    header_parameter = [
        "Parameter",
        "Parameterbeschreibung",
        "Einheit",
        "Datenquelle",
        "Zusatz-Info",
        "Besonderheiten",
        "Literaturhinweis",
    ]
    combineDuplicates("Metadaten_Parameter", header_parameter, cur)

    #
    #  Metadaten_Stationsname
    #

    header_stationname = [
        "Stationsname"
    ]
    combineDuplicates("Metadaten_Stationsname", header_stationname, cur)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


def combineDuplicates(db_name, header, db_cur):
    print("Start processing:", db_name)
    duplicates = []

    # get duplicates requst
    select_duplicates = "SELECT a.* " + \
        "FROM " + db_name + " AS a " + \
        "INNER JOIN (SELECT * " + \
        "FROM " + db_name + " " + \
        "GROUP BY Stations_ID"

    # add all columns (except the dates)
    for column_name in header:
        select_duplicates += ", \"" + column_name + "\" "

    select_duplicates += "HAVING Count(*) > 1) AS b " + \
        "ON a.Stations_ID = b.Stations_ID "

    for column_name in header:
        select_duplicates += "AND a.\"" + column_name + \
            "\" = b." + "\"" + column_name + "\" "

    select_duplicates += "ORDER BY Stations_ID, Von_Datum ASC, Bis_Datum DESC"

    print(select_duplicates)

    duplicates = db_cur.execute(select_duplicates)

    insert_table = "INSERT INTO " + db_name + " " + \
        "(Stations_ID, Von_Datum, Bis_Datum"

    for column_name in header:
        insert_table += ", \"" + column_name + "\""

    insert_table += ") values (?, ?, ?"

    for column_name in header:
        insert_table += ", ?"

    insert_table += ")"

    delete_row = "DELETE FROM " + db_name + " WHERE ID=?"

    new_row = []
    current_id = ""
    von_datum = ""
    bis_datum = ""
    data = []
    first = True
    for row in duplicates:
        # check if the id is the same
        if current_id == str(row[1]):
            # check if the data is the same
            same = True
            for index, var in enumerate(data):
                print(var, row[index + 4])
                if var != row[index + 4]:
                    same = False
                    break

            if same:
                if bis_datum != "Null":
                    bis_datum = row[3]
            else:
                # save the new_row data
                new_row.append(current_id)
                new_row.append(von_datum)
                new_row.append(bis_datum)
                new_row.extend(data)
                db_cur.execute(insert_table, new_row)

                von_datum = row[2]
                bis_datum = row[3]
                data = row[4:]
        else:
            if not first:
                # save the new_row data, but only when its not first
                new_row.append(current_id)
                new_row.append(von_datum)
                new_row.append(bis_datum)
                new_row.extend(data)
                db_cur.execute(insert_table, new_row)
            else:
                first = False

            current_id = str(row[1])
            von_datum = row[2]
            bis_datum = row[3]
            data = row[4:]

        db_cur.execute(delete_row, (str(row[0]),))

    # save the last one, because it will not recognize any change, when the found data ends
    new_row.append(current_id)
    new_row.append(von_datum)
    new_row.append(bis_datum)
    new_row.extend(data)
    db_cur.execute(insert_table, new_row)


def createMetadaten_Geographie(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #
    #  Metadaten_Geographie_2
    #

    cur.execute("CREATE TABLE Metadaten_Geographie ( " +
                "ID             INTEGER  PRIMARY KEY AUTOINCREMENT, " +
                "Stations_ID    INTEGER, " +
                "Von_Datum      DATETIME, " +
                "Bis_Datum      DATETIME, " +
                "Stationshoehe  REAL, " +
                "Geberhoehe     REAL, " +
                "[Geogr.Breite] REAL, " +
                "[Geogr.Laenge] REAL " +
                ");")

    cur.execute("INSERT INTO Metadaten_Geographie SELECT ID, " +
                "Stations_ID, " +
                "Von_Datum, " +
                "Bis_Datum, " +
                "Stationshoehe, " +
                "Geberhoehe, " +
                "\"Geogr.Breite\", " +
                "\"Geogr.Laenge\" " +
                "FROM ( " +
                "SELECT a.*, " +
                "b.*, " +
                "(b.Bis_Datum - b.Von_Datum) AS test " +
                "FROM Metadaten_Geographie_2 AS a " +
                "INNER JOIN " +
                "Metadaten_Geraete_Geberhoehe AS b ON a.Stations_ID = b.Stations_ID AND  " +
                "(CASE WHEN b.Bis_Datum NOT NULL THEN (CASE WHEN a.Bis_Datum NOT NULL THEN ( (a.von_datum BETWEEN b.von_datum AND b.bis_datum) OR " +
                "(a.bis_datum BETWEEN b.von_datum AND b.bis_datum) OR " +
                "(a.von_datum < a.von_datum AND " +
                "a.bis_datum > b.bis_datum) OR " +
                "(a.von_datum > a.von_datum AND " +
                "a.bis_datum < b.bis_datum) ) ELSE b.bis_datum > a.von_datum END) ELSE (CASE WHEN a.Bis_Datum NOT NULL THEN a.bis_datum > b.von_datum ELSE TRUE END) END) " +
                "ORDER BY a.Stations_ID, " +
                "b.Von_Datum ASC, " +
                "b.Bis_Datum DESC " +
                ") " +
                "GROUP BY ID;")

    cur.execute("DROP TABLE IF EXISTS Metadaten_Geographie_2;")
    cur.execute("DROP TABLE IF EXISTS Metadaten_Geraete_Geberhoehe;")

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


def writeUnfalldatenInDatabase(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #
    #  unfall_data_2016
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date, 5: Coordinate
    header_unfall_data_2016 = {
        "UGEMEINDE": 1,
        "UJAHR": 1,
        "UMONAT": 1,
        "USTUNDE": 1,
        "UWOCHENTAG": 1,
        "UKATEGORIE": 1,
        "UART": 1,
        "UTYP1": 1,
        "ULICHTVERH": 1,
        "IstRad": 1,
        "IstPKW": 1,
        "IstFuss": 1,
        "IstKrad": 1,
        "IstGkfz": 1,
        "IstSonstige": 1,
        "LINREFX": 5,
        "LINREFY": 5,
        "XGCSWGS84": 5,
        "YGCSWGS84": 5,
        "STRZUSTAND": 1
    }
    index_unfall_data_2016 = {
        0: 99,
        1: 99,
        2: 99,
        3: 99,
        4: 99,
        5: 0,
        6: 1,
        7: 2,
        8: 3,
        9: 4,
        10: 5,
        11: 6,
        12: 7,
        13: 8,
        14: 19,
        15: 9,
        16: 10,
        17: 11,
        18: 12,
        19: 13,
        20: 14,
        21: 15,
        22: 16,
        23: 17,
        24: 18
    }
    dataToDatabase("unfall_data", header_unfall_data_2016, index_unfall_data_2016,
                   "Unfallorte_2016_LinRef.csv", cur)

    #
    #  unfall_data_2017
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date, 5: Coordinate
    header_unfall_data_2017 = {
        "UGEMEINDE": 1,
        "UJAHR": 1,
        "UMONAT": 1,
        "USTUNDE": 1,
        "UWOCHENTAG": 1,
        "UKATEGORIE": 1,
        "UART": 1,
        "UTYP1": 1,
        "ULICHTVERH": 1,
        "IstRad": 1,
        "IstPKW": 1,
        "IstFuss": 1,
        "IstKrad": 1,
        "IstGkfz": 1,
        "IstSonstige": 1,
        "LINREFX": 5,
        "LINREFY": 5,
        "XGCSWGS84": 5,
        "YGCSWGS84": 5,
        "STRZUSTAND": 1
    }
    index_unfall_data_2017 = {
        0: 99,
        1: 99,
        2: 99,
        3: 99,
        4: 99,
        5: 0,
        6: 1,
        7: 2,
        8: 3,
        9: 4,
        10: 5,
        11: 6,
        12: 7,
        13: 9,
        14: 10,
        15: 11,
        16: 12,
        17: 14,
        18: 8,
        19: 19,
        20: 15,
        21: 16,
        22: 17,
        23: 18
    }
    dataToDatabase("unfall_data", header_unfall_data_2017, index_unfall_data_2017,
                   "Unfallorte2017_LinRef.csv", cur)

    #
    #  unfall_data_2018
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date, 5: Coordinate
    header_unfall_data_2018 = {
        "UGEMEINDE": 1,
        "UJAHR": 1,
        "UMONAT": 1,
        "USTUNDE": 1,
        "UWOCHENTAG": 1,
        "UKATEGORIE": 1,
        "UART": 1,
        "UTYP1": 1,
        "ULICHTVERH": 1,
        "IstRad": 1,
        "IstPKW": 1,
        "IstFuss": 1,
        "IstKrad": 1,
        "IstGkfz": 1,
        "IstSonstige": 1,
        "LINREFX": 5,
        "LINREFY": 5,
        "XGCSWGS84": 5,
        "YGCSWGS84": 5,
        "STRZUSTAND": 1
    }
    index_unfall_data_2018 = {
        0: 99,
        1: 99,
        2: 99,
        3: 99,
        4: 0,
        5: 1,
        6: 2,
        7: 3,
        8: 4,
        9: 5,
        10: 6,
        11: 7,
        12: 8,
        13: 9,
        14: 10,
        15: 11,
        16: 12,
        17: 13,
        18: 14,
        19: 19,
        20: 15,
        21: 16,
        22: 17,
        23: 18
    }
    dataToDatabase("unfall_data", header_unfall_data_2018, index_unfall_data_2018,
                   "Unfallorte2018_LinRef.csv", cur)

    #
    #  unfall_data_2019
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date, 5: Coordinate
    header_unfall_data_2019 = {
        "UGEMEINDE": 1,
        "UJAHR": 1,
        "UMONAT": 1,
        "USTUNDE": 1,
        "UWOCHENTAG": 1,
        "UKATEGORIE": 1,
        "UART": 1,
        "UTYP1": 1,
        "ULICHTVERH": 1,
        "IstRad": 1,
        "IstPKW": 1,
        "IstFuss": 1,
        "IstKrad": 1,
        "IstGkfz": 1,
        "IstSonstige": 1,
        "LINREFX": 5,
        "LINREFY": 5,
        "XGCSWGS84": 5,
        "YGCSWGS84": 5,
        "STRZUSTAND": 1
    }
    index_unfall_data_2019 = {
        0: 99,
        1: 99,
        2: 99,
        3: 99,
        4: 0,
        5: 1,
        6: 2,
        7: 3,
        8: 4,
        9: 5,
        10: 6,
        11: 7,
        12: 8,
        13: 9,
        14: 10,
        15: 11,
        16: 12,
        17: 13,
        18: 14,
        19: 15,
        20: 16,
        21: 17,
        22: 18,
        23: 19
    }
    dataToDatabase("unfall_data", header_unfall_data_2019, index_unfall_data_2019,
                   "Unfallorte2019_LinRef.csv", cur)

    #
    #  unfall_data_2020
    #

    # 1: Integer, 2: Float, 3: Text, 4: Date, 5: Coordinate
    header_unfall_data_2020 = {
        "UGEMEINDE": 1,
        "UJAHR": 1,
        "UMONAT": 1,
        "USTUNDE": 1,
        "UWOCHENTAG": 1,
        "UKATEGORIE": 1,
        "UART": 1,
        "UTYP1": 1,
        "ULICHTVERH": 1,
        "IstRad": 1,
        "IstPKW": 1,
        "IstFuss": 1,
        "IstKrad": 1,
        "IstGkfz": 1,
        "IstSonstige": 1,
        "LINREFX": 5,
        "LINREFY": 5,
        "XGCSWGS84": 5,
        "YGCSWGS84": 5,
        "STRZUSTAND": 1
    }
    index_unfall_data_2020 = {
        0: 99,
        1: 99,
        2: 99,
        3: 99,
        4: 99,
        5: 0,
        6: 1,
        7: 2,
        8: 3,
        9: 4,
        10: 5,
        11: 6,
        12: 7,
        13: 8,
        14: 9,
        15: 10,
        16: 11,
        17: 12,
        18: 13,
        19: 14,
        20: 15,
        21: 16,
        22: 17,
        23: 18,
        24: 19
    }
    dataToDatabase("unfall_data", header_unfall_data_2020, index_unfall_data_2020,
                   "Unfallorte2020_LinRef.csv", cur)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


def fixDateOfUnfalldaten(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # create new table
    cur.execute("CREATE TABLE IF NOT EXISTS unfall_Geographie_data_2 (" +
                "ID          INTEGER PRIMARY KEY AUTOINCREMENT," +
                "UGEMEINDE   INTEGER," +
                "UJAHR       INTEGER," +
                "UMONAT      INTEGER," +
                "USTUNDE     INTEGER," +
                "UTAG  INTEGER," +
                "UKATEGORIE  INTEGER," +
                "UART        INTEGER," +
                "UTYP1       INTEGER," +
                "ULICHTVERH  INTEGER," +
                "IstRad      INTEGER," +
                "IstPKW      INTEGER," +
                "IstFuss     INTEGER," +
                "IstKrad     INTEGER," +
                "IstGkfz     INTEGER," +
                "IstSonstige INTEGER," +
                "LINREFX     REAL," +
                "LINREFY     REAL," +
                "XGCSWGS84   REAL," +
                "YGCSWGS84   REAL," +
                "STRZUSTAND  INTEGER," +
                "Stations_ID INTEGER" +
                ");")

    insert_table = "INSERT INTO unfall_Geographie_data_2 ( UGEMEINDE, UJAHR, UMONAT, USTUNDE, UTAG, UKATEGORIE, UART, UTYP1, ULICHTVERH, IstRad, IstPKW, IstFuss, IstKrad, IstGkfz, IstSonstige, LINREFX, LINREFY, XGCSWGS84, YGCSWGS84, STRZUSTAND, Stations_ID, Distance ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"

    # load unfall data
    cur.execute("SELECT * FROM unfall_Geographie_data")

    rows = cur.fetchall()

    for row in rows:
        month_days = monthrange(row[2], row[3])
        counter = 0
        days = []

        # calculate what to add
        if(month_days[0] > row[5]):
            counter = (7 - (month_days[0] + 1)) + (row[5] + 1) + 1
            while(counter <= month_days[1]):
                days.append(counter)
                counter += 7
        elif(month_days[0] == row[5]):
            counter = 1
            while(counter <= month_days[1]):
                days.append(counter)
                counter += 7
        else:
            counter = (row[5] - month_days[0]) + 1
            while(counter <= month_days[1]):
                days.append(counter)
                counter += 7

        for day in days:
            next_row = []
            for i, data in enumerate(row):
                if(i == 5):
                    next_row.append(day)
                elif(i != 0):
                    next_row.append(data)

            cur.execute(insert_table, next_row)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


if __name__ == "__main__":
    # example link
    # 0                                            1
    # 0            1  2     3        4        5
    # stundenwerte_SD_00003_19510101_20110331_hist.zip

    # Download all needed files
    # downloadSource(
    #    "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/")

    # merge the databases of the same type together (data and meta)
    # mergeDatabasesAllFolder()

    # select all data between 2016 and 2020 and remove multiple header (also combine databases with different names, but same content)
    # processDatabasesAllFolder()

    # combine all selected data from the metadaten-databases in big ones
    # combineProcessedDatabases("Metadaten_Fehlwerte_processed")
    # combineProcessedDatabases("Metadaten_Geographie_processed")
    # combineProcessedDatabases("Metadaten_Geraete_processed")
    # combineProcessedDatabases("Metadaten_Parameter_processed")
    # combineProcessedDatabases("Metadaten_Stationsname_processed")

    # get smallest and biggest value for each file (don't use processed files, to get better values, ignore all parameter, taht are not integer if we expect one integr parameter)
    # getVarLength("Metadaten_Fehlwerte_processed_combined.csv", 7)
    # getVarLength("Metadaten_Geographie_processed_combined.csv", 7)
    # getVarLength("Metadaten_Geraete_processed_combined.csv", 10)
    # getVarLength("Metadaten_Parameter_processed_combined.csv", 11)
    # getVarLength("Metadaten_Stationsname_processed_combined.csv", 4)

    # create sqlite db and put data in there
    # writeAllDataInDatabase("weather_data.db")

    # remove double data (we have a lot of it)
    # removeDuplicatesInDatabase("weather_data.db")

    # combine same data with different start and end time
    # combineDuplicatesInDatabase("weather_data.db")

    # combine tables to create the Metadaten_Geographie table
    # createMetadaten_Geographie()

    # Add Unfalldaten
    # writeUnfalldatenInDatabase("weather_data.db")
	
	
    # Test1
    fixDateOfUnfalldaten("weather_data.db")