import csv
import pymssql
import os

# map crime types to ids
crime_types_dict = {
    "Violence and sexual offences": 0,
    "Anti-social behaviour": 1,
    "Criminal damage and arson": 2,
    "Public order": 3,
    "Vehicle crime": 4,
    "Other theft": 5,
    "Shoplifting": 6,
    "Other crime": 7,
    "Burglary": 8,
    "Drugs": 9,
    "Possession of weapons": 10,
    "Robbery": 11,
    "Bicycle theft": 12,
    "Theft from the person": 13
}

last_outcomes_dict = {
    "Investigation complete; no suspect identified": 0,
    "": 1, #unk outcome
    "Under investigation": 2,
    "Unable to prosecute suspect": 3,
    "Formal action is not in the public interest": 4,
    "Awaiting court outcome": 5,
    "Offender given a caution": 6,
    "Further action is not in the public interest": 7,
    "Further investigation is not in the public interest": 8,
    "Local resolution": 9,
    "Suspect charged as part of another case": 10,
    "Action to be taken by another organisation": 11,
    "Status update unavailable": 12,
    "Offender sent to prison": 13,
    "Offender fined": 14,
    "Court result unavailable": 15,
    "Defendant found not guilty": 16,
    "Offender otherwise dealt with": 17,
    "Offender given community sentence": 18,
    "Offender given suspended prison sentence": 19,
    "Offender given a drugs possession warning": 20,
    "Court case unable to proceed": 21,
    "Offender ordered to pay compensation": 22,
    "Offender given conditional discharge": 23,
    "Offender given penalty notice": 24,
    "Offender given absolute discharge": 25,
    "Offender deprived of property": 26,
    "Defendant sent to Crown Court": 27,
}

# open up pymssql db conn
server = "127.0.0.1:1444"
user = "sa"
password = "Palmer101!"
db_name = "data"

conn = pymssql.connect(server, user, password, db_name)

def import_content(file_name):
    parts = file_name.split('-')
    file_name = 'data/{0}-{1}/{2}'.format(parts[0], parts[1], file_name)

    with open(file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader) # skip headers

        data = (list(csv_reader))
        all_row_count = len(data)
        count = 0

        for row in data:
            crime_id = row[0]
            date = row[1]
            reported = row[2] # loc
            falls_within = row[3] # loc
            long = row[4]
            lat = row[5]
            location = row[6]
            lsoa_code = row[7]
            lsoa_name = row[8]
            crime_type = row[9]
            last_outcome = row[10]
            context = row[11]

            crime_type_id = crime_types_dict[crime_type]
            last_outcome_id = last_outcomes_dict[last_outcome]

            count += 1

            print ("{3} - rows done {0}/{1} ({2:.2f}%)".format(count, all_row_count, (count / all_row_count) * 100, file_name))

            dateSplit = date.split("-")
            year = dateSplit[0]
            month = dateSplit[1]

            # create a new cursor
            cursor = conn.cursor()
            cursor.callproc('InsertData', (
                crime_id,
                year,
                month,
                reported,
                falls_within,
                long,
                lat,
                location,
                lsoa_code,
                crime_type_id,
                last_outcome_id,
                context
            ))

            cursor.close()

        conn.commit()
        print ("Total of {0} rows added successfully".format(count))

# Change this root dir
rootdir = r"C:\Users\Jimmy\Desktop\_python\police_data_parse\data"

for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        # print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if ('street' in file):
            import_content(file)

