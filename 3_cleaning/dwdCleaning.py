import sqlite3


def removeDuplicates(db_name):
    # open and (if not exists) create database file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # create new table
    cur.execute("CREATE TABLE IF NOT EXISTS unfall_Geographie_data_4 (" +
                "UJAHR INTEGER," +
                "UMONAT INTEGER," +
                "USTUNDE INTEGER," +
                "UTAG INTEGER," +
                "UKATEGORIE INTEGER," +
                "UART INTEGER," +
                "UTYP1 INTEGER," +
                "ULICHTVERH INTEGER," +
                "STRZUSTAND INTEGER," +
                "YGCSWGS84 REAL," +
                "XGCSWGS84 REAL," +
                "TT_TU REAL," +
                "RF_TU INTEGER," +
                "V_N_CTYPE INTEGER," +
                "V_N_I_CTYPE TEXT," +
                "V_S1_CS INTEGER," +
                "V_S1_CSA INTEGER," +
                "V_S1_HHS INTEGER," +
                "V_S1_NS INTEGER," +
                "V_S2_CS INTEGER," +
                "V_S2_CSA INTEGER," +
                "V_S2_HHS INTEGER," +
                "V_S2_NS INTEGER," +
                "V_S3_CS INTEGER," +
                "V_S3_CSA INTEGER," +
                "V_S3_HHS INTEGER," +
                "V_S3_NS INTEGER," +
                "V_S4_CS INTEGER," +
                "V_S4_CSA INTEGER," +
                "V_S4_HHS INTEGER," +
                "V_S4_NS INTEGER," +
                "V_N_I_CNESS TEXT," +
                "V_N_CNESS INTEGER," +
                "TT REAL," +
                "TD REAL," +
                "FX_911 REAL," +
                "ABSF_STD REAL," +
                "VP_STD REAL," +
                "TF_STD REAL," +
                "P_STD INTEGER," +
                "TT_STD REAL," +
                "RF_STD REAL," +
                "TD_STD REAL," +
                "R1 REAL," +
                "RS_IND INTEGER," +
                "WRTR INTEGER," +
                "P INTEGER," +
                "P0 INTEGER," +
                "V_TE002 INTEGER," +
                "V_TE005 REAL," +
                "V_TE010 REAL," +
                "V_TE020 REAL," +
                "V_TE050 REAL," +
                "V_TE100 REAL," +
                "ATMO_LBERG INTEGER," +
                "FD_LBERG INTEGER," +
                "FG_LBERG INTEGER," +
                "SD_LBERG INTEGER," +
                "ZENIT DATE," +
                "MESS_DATUM_WOZ DATE," +
                "SD_SO INTEGER," +
                "V_VV_I TEXT," +
                "V_VV INTEGER," +
                "WW INTEGER," +
                "WW_Text TEXT," +
                "F REAL," +
                "D INTEGER," +
                "FF REAL," +
                "DD INTEGER" +
                ");")

    insert_table = "INSERT INTO unfall_Geographie_data_4 ( UJAHR, UMONAT,  USTUNDE, UTAG, UKATEGORIE, UART, UTYP1, ULICHTVERH, STRZUSTAND, YGCSWGS84, XGCSWGS84, TT_TU, RF_TU, V_N_CTYPE, V_N_I_CTYPE, V_S1_CS, V_S1_CSA, V_S1_HHS, V_S1_NS, V_S2_CS, V_S2_CSA, V_S2_HHS, V_S2_NS, V_S3_CS, V_S3_CSA, V_S3_HHS, V_S3_NS, V_S4_CS, V_S4_CSA, V_S4_HHS, V_S4_NS, V_N_I_CNESS, V_N_CNESS, TT, TD, FX_911, ABSF_STD, VP_STD, TF_STD, P_STD, TT_STD, RF_STD, TD_STD, R1, RS_IND, WRTR, P,  P0, V_TE002, V_TE005, V_TE010, V_TE020, V_TE050, V_TE100, ATMO_LBERG, FD_LBERG, FG_LBERG, SD_LBERG, ZENIT, MESS_DATUM_WOZ, SD_SO, V_VV_I, V_VV, WW, WW_Text, F, D, FF, DD ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"

    # load unfall data
    cur.execute("SELECT * FROM unfall_Geographie_data_3")

    rows = cur.fetchall()

    current_row = []
    row_duplicates = []

    for row in rows:
        if len(current_row) == 0:
            for i in range(0, 11):
                current_row.append(row[i])
        else:
            new_row = False
            for i in range(0, 11):
                if i != 3:
                    if row[i] != current_row[i]:
                        new_row = True
                        break

            if new_row:
                index = 0
                if current_row[8] == 0:
                    # Strasse trocken
                    minHumidity = 100
                    for i, row_duplicate in enumerate(row_duplicates):
                        if int(row_duplicate[12] or 0) < minHumidity:
                            minHumidity = int(row_duplicate[12] or 0)
                            index = i
                elif current_row[8] == 1:
                    # Strasse nass/feucht/schlüpfrig
                    maxHumidity = 0
                    for i, row_duplicate in enumerate(row_duplicates):
                        if int(row_duplicate[12] or 0) > maxHumidity:
                            maxHumidity = int(row_duplicate[12] or 0)
                            index = i
                elif current_row[8] == 2:
                    # Strasse winterglatt
                    minTemperature = 100
                    for i, row_duplicate in enumerate(row_duplicates):
                        if float(row_duplicate[11] or 0) < minTemperature:
                            minTemperature = float(row_duplicate[11] or 0)
                            index = i

                cur.execute(insert_table, row_duplicates[index])

                row_duplicates.clear()

                for i in range(0, 11):
                    current_row[i] = row[i]

        row_duplicates.append(row)

    # Save (commit) the changes
    con.commit()

    # close the database connection
    con.close()


if __name__ == "__main__":
    # Remove duplicates regarding the posible days
    removeDuplicates("weather_data.db")
