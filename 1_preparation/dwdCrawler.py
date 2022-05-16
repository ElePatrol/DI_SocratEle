import requests
from bs4 import BeautifulSoup
import os
import zipfile


def downloadSource(url):
    # GET-Request ausführen
    response = requests.get(url)

    # BeautifulSoup HTML-Dokument aus dem Quelltext parsen
    html = BeautifulSoup(response.text, 'html.parser')

    # Alle Links aus dem HTML-Dokument extrahieren
    main_links_html = []
    for a in html.find_all('a', href=True):
        if ".." not in a['href']:
            main_links_html.append(a)

    for a in main_links_html:
        # create folder for it
        try:
            os.mkdir(a.contents[0])
        except OSError:
            print("Creation of the directory %s failed" % a.contents[0])

        base_url = url + a['href']

        # check if there is recent and historical data
        response_temppage = requests.get(base_url)
        if(len(response_temppage.find_all('a', href=True)) <= 4):
            # open new url HISTORICAL
            downloadZip(a.contents[0], base_url + "historical/")

            # open new url HISTORICAL
            downloadZip(a.contents[0], base_url + "recent/")
        else:
            downloadZip(a.contents[0], base_url)


def downloadZip(folder, base_url):
    # GET-Request ausführen
    response_subpage = requests.get(base_url)

    # BeautifulSoup HTML-Dokument aus dem Quelltext parsen
    subpage = BeautifulSoup(response_subpage.text, 'html.parser')

    # Alle Links aus dem HTML-Dokument extrahieren
    for a_sub in subpage.find_all('a', href=True):
        if ".." not in a_sub['href']:
            zipFile = requests.get(base_url + a_sub['href'])
            open(folder + a_sub['href'],
                 "wb").write(zipFile.content)


def mergeDatabasesAllFolder():
    for folder in os.walk("."):
        mergeDatabasesFolder(folder[0])


def mergeDatabasesFolder(folder):
    header = {}
    fileErrors = []
    base_folder = folder + "\\"
    for file in os.listdir(folder):
        if ".zip" in file:
            with zipfile.ZipFile(base_folder + file, 'r') as zipObj:
                # Get list of files names in zip
                listOfiles = zipObj.namelist()
                for database in listOfiles:
                    if ".html" not in database:
                        old_db_name_list = database.split("_")
                        offset = 0
                        for part in reversed(old_db_name_list):
                            if(any(char.isdigit() for char in part)):
                                offset -= 1
                        db_name_list = old_db_name_list[0:offset]
                        db_name = base_folder + \
                            "_".join(db_name_list) + ".txt"

                        db_data_write = []
                        with zipObj.open(database, 'r') as db_content:
                            db_line_raw = db_content.readline()
                            db_line = db_line_raw.decode('latin_1')
                            if db_name not in header.keys():
                                header[db_name] = db_line
                                pos = db_line.rfind("eor")
                                if(pos == -1):
                                    db_data_write.append(db_line[:-1])
                                else:
                                    db_data_write.append(db_line[:pos])
                            elif(header[db_name] != db_line):
                                fileErrors.append(db_name)
                                db_content.close()
                                break

                            for db_line_raw in db_content:
                                # decode and remove \n
                                db_line = (db_line_raw.decode(
                                    'latin_1')).replace("\n", "")
                                if ';' in db_line:
                                    pos = db_line.rfind("eor")
                                    if(pos == -1):
                                        db_data_write.append(
                                            "\n" + db_line)
                                    else:
                                        db_data_write.append(
                                            "\n" + db_line[:pos])
                            db_content.close()

                        if(len(db_data_write) != 0):
                            with open(db_name, "a+") as db_file:
                                db_file.writelines(db_data_write)
                                db_file.close()

                zipObj.close()

    if(len(fileErrors) > 0):
        print(fileErrors)


def processDatabasesAllFolder():
    for folder in os.walk("."):
        print("Entering folder: ", folder[0])
        processDatabasesFehlerwerte(folder[0])
        processDatabasesGeographie(folder[0])
        processDatabasesGeraete(folder[0])
        processDatabasesParameter(folder[0])
        processDatabasesStationsname(folder[0])
        processDatabasesProdukt(folder[0])


def processDatabasesFehlerwerte(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("Metadaten_Fehl"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, "Stations_", [3, 4], [6, 10]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "Metadaten_Fehlwerte_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesGeographie(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("Metadaten_Geographie"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, "Stations_", [4, 5], [0, 4]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "Metadaten_Geographie_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesGeraete(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("Metadaten_Geraete"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, "Stations_", [6, 7], [0, 4]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "Metadaten_Geraete_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesParameter(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("Metadaten_Parameter"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, tuple({"Stations_", "Legende"}), [1, 2], [0, 4]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "Metadaten_Parameter_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesStationsname(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("Metadaten_Stationsname"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, "Stations_", [2, 3], [0, 4]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "Metadaten_Stationsname_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesProdukt(folder):
    base_folder = folder + "\\"
    db_data_write = []

    add_header = True
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            if file.startswith("produkt_"):
                print("Found file: ", file)
                db_data_write.extend(processDatabasesAll(base_folder + file,
                                                         add_header, "STATIONS_", [1, 1], [0, 4]))
                add_header = False

    if(len(db_data_write) > 0):
        with open(base_folder + "produkt_processed.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def processDatabasesAll(file, add_header, header_prefix, column_index, year_pos):
    db_data_write = []
    with open(file, 'r') as db_content:
        if(add_header):
            # get header and add it to the file
            db_line_raw = db_content.readline()
            db_data_write.append(db_line_raw)

        # Stations_ID
        for db_line_raw in db_content:
            if(len(db_line_raw) > 2):
                if not db_line_raw.startswith(header_prefix):
                    # check if it is in our time interval
                    columns = db_line_raw.split(";")

                    if(len(columns) < 4):
                        db_data_write.append(db_line_raw)
                    else:
                        if(len(columns[column_index[0]]) < year_pos[1]) or columns[column_index[0]].startswith(" "):
                            db_data_write.append(db_line_raw)
                        elif (len(columns[column_index[1]]) < year_pos[1]) or columns[column_index[1]].startswith(" "):
                            db_data_write.append(db_line_raw)
                        else:
                            von = int(columns[column_index[0]]
                                      [year_pos[0]:year_pos[1]])
                            bis = int(columns[column_index[1]]
                                      [year_pos[0]:year_pos[1]])
                            if((von >= 2016 and von <= 2020) or (bis >= 2016 and bis <= 2020) or (von < 2016 and bis > 2020)):
                                db_data_write.append(db_line_raw)

    if(len(db_data_write) > 0):
        db_data_write[-1] += "\n"

    return db_data_write


def combineProcessedDatabases(filename):
    add_header = True
    db_data_write = []

    for folder in os.walk("."):
        base_folder = folder[0] + "\\"
        print("Entering folder: ", folder[0])
        for file in os.listdir(folder[0]):
            if file.endswith(".csv"):
                if file.startswith(filename):
                    db_data_write.extend(
                        readProcessedDatabase(base_folder + filename + ".csv", add_header))
                    add_header = False
                    break

    if(len(db_data_write) > 0):
        with open(filename + "_combined.csv", "a+") as db_file:
            db_file.writelines(db_data_write)
            db_file.close()


def readProcessedDatabase(file, add_header):
    db_data_write = []
    with open(file, 'r') as db_content:
        db_line_raw = db_content.readline()

        if(add_header):
            db_data_write.append(db_line_raw)

        for db_line_raw in db_content:
            db_data_write.append(db_line_raw)

    if(len(db_data_write) > 0):
        db_data_write[-1] += "\n"

    return db_data_write


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
