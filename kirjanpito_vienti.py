"""
primary keyt sqlite_sequence:
'document', int: tositteen nro
'entry', int: viennin nro

sqlite> .schema document
CREATE TABLE document (id integer PRIMARY KEY AUTOINCREMENT NOT NULL,number integer NOT NULL,period_id integer NOT NULL,date date NOT NULL,FOREIGN KEY (period_id) REFERENCES period (id));

CREATE TABLE entry (id integer PRIMARY KEY AUTOINCREMENT NOT NULL,document_id integer NOT NULL,account_id integer NOT NULL,debit bool NOT NULL,amount numeric(10, 2) NOT NULL,description varchar(100) NOT NULL,row_number integer NOT NULL,flags integer NOT NULL,FOREIGN KEY (document_id) REFERENCES document (id),FOREIGN KEY (account_id) REFERENCES account (id));

INSERT INTO "document" VALUES(3,2,1,1483221600000);
(primary key, tositteen nro, period id??, date)

> INSERT INTO "entry" VALUES(3,3,174,0,80,'Kangasmerkit',1,0);
> INSERT INTO "entry" VALUES(4,3,36,1,80,'Kangasmerkit',2,0);

(primary key, viennin nro, tilin nro, debit (1/0), eurot (kok,desim), kuvaus, rivinro, 0)
"""
import sqlite3
from datetime import datetime
import time
import calendar
import csv
import sys

DB_FILE = '/home/makinen/tss/2018_tilitin.sqlite'

DEBIT = 1
KREDIT = 0

MONTHS = ['Tammikuu', 'Helmikuu', 'Maaliskuu', 'Huhtikuu', 'Toukokuu', 'Kesäkuu', 'Heinäkuu',\
'Elokuu', 'Syyskuu', 'Lokakuu', 'Marraskuu', 'Joulukuu']

def create_document(cursor, document_date):

    # Gets the id of the latest document
    cursor.execute('SELECT max(seq) FROM sqlite_sequence WHERE name="document"')
    max_document_number = (cursor.fetchone()[0], )

    # Gets the number of the newest document and increase it for the next document
    cursor.execute('SELECT number FROM document WHERE id=?', max_document_number)
    max_number = cursor.fetchone()[0]+1
    #print("max number %d" % max_number)

    # Gets the latest period number
    cursor.execute('SELECT max(seq) FROM sqlite_sequence WHERE name="period"')
    period = cursor.fetchone()[0]

    # Creates a new document
    unixtime = time.mktime(document_date.timetuple())*1000
    document = (max_number, period, unixtime)
    cursor.execute('INSERT INTO document (number, period_id, date) VALUES(?,?,?)', document)

    # Return the id of the new document
    cursor.execute('SELECT max(seq) FROM sqlite_sequence WHERE name="document"')
    document_id = cursor.fetchone()[0]
    #print("document id %d" % document_id)

    return document_id

def insert_row(cursor, document_id, amount, desc):
    # 1910 = pankkitili
    # 2310 = lainat rahoituslaitoksilta
    row = 1
    cursor.execute('SELECT id FROM account WHERE number="1910"')
    a_1910 = cursor.fetchone()[0]
    cursor.execute('SELECT id FROM account WHERE number="2310"')
    a_2310 = cursor.fetchone()[0]

    # 4190 = pankkikulut
    if desc.startswith('Palvelumaksut'):
        cursor.execute('SELECT id FROM account WHERE number="4190"')
        a_2310 = cursor.fetchone()[0]

    # 4170 = ATK-kulut
    if desc.startswith('Web-hotellin maksu'):
        cursor.execute('SELECT id FROM account WHERE number="4170"')
        a_2310 = cursor.fetchone()[0]

    if amount > 0:
        debit_account_id = a_1910
        kredit_account_id = a_2310
    else:
        debit_account_id = a_2310
        kredit_account_id = a_1910

    amount = abs(amount)

    entry = (document_id, debit_account_id, DEBIT, amount, desc, row, 0)
    cursor.execute("INSERT INTO entry (document_id, account_id, debit, amount, description, row_number, flags) VALUES (?,?,?,?,?,?,?)", entry)

    #(primary key, tositenro, tilin nro, debit (1/0), eurot (kok,desim), kuvaus, rivinro, 0)
    entry = (document_id, kredit_account_id, KREDIT, amount, desc, row+1, 0)
    cursor.execute("INSERT INTO entry (document_id, account_id, debit, amount, description, row_number, flags) VALUES (?,?,?,?,?,?,?)", entry)

def parse_csv(csv_file):

    reader = csv.reader(csv_file, delimiter=';', skipinitialspace=True)
    rows = [] # muut
    rows2 = [] # kurssimaksut
    rows3 = [] # wcs-workshop
    rows4 = [] # wcs-workshop2
    for row in reader:
        palvelumaksu = False
        if row[3] == '730':
            palvelumaksu = True

        reference_number = ''
        if row[7]:
            reference_number = row[7].replace(' ', '')

        amount = float(row[2].replace(',', '.'))
        row = {'date': row[0], 'amount': amount, 'desc': row[8], 'reference': reference_number, 'name': row[5]}

        if palvelumaksu:
            row['desc'] = 'Palvelumaksut'

        if row['name'].startswith('Wepardi'):
            row['desc'] = 'Web-hotellin maksu'

        try:
            # 2PI
            # 11 11110 - 11 11700
            if reference_number and int(reference_number) >= 1111110 and int(reference_number) < 1111700:
                rows3.append(row)
                continue

            # MAY WE SWING
            # 55500 - 57500
            if reference_number and int(reference_number) >= 55500 and int(reference_number) < 57500:
                rows4.append(row)
                continue

            # kurssimaksut: 4000 - 8000
            if reference_number and int(reference_number) > 4000 and int(reference_number) < 8000:
                rows2.append(row)
            else:
                rows.append(row)
        except ValueError:
            row['desc'] = '%s %s' % (reference_number, row['desc'])
            rows.append(row)

    #print ('viitteita wcs:')
    #print (len(rows3))
    return (rows, rows2, rows3, rows4)

def main():

    if len(sys.argv) < 2:
        print("Usage: ./kirjanpito_vienti csv_file")
        sys.exit(1)

    csv_file = open(sys.argv[1], encoding="latin-1")
    #print(csv_file.readlines()[:1][0].split('\t'))
    #sys.exit(0)
    entries, entries_coursefees, entries_wcs, entries_wcs2 = parse_csv(csv_file.readlines()[1:])

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for entry in entries:
        #print("{} {}/{} .".format(e['amount'], e['desc'], e['name']))
        document_date = datetime.strptime(entry['date'], "%d.%m.%Y")
        #print(document_date)
        document_id = create_document(cursor, document_date)
        insert_row(cursor, document_id, float(entry['amount']), "{} / {}".format(entry['desc'], entry['name']))

    wcs_amount = 0.0
    for entry in entries_wcs:
        wcs += entry['amount']

    # Calculate the sum of workshop fees received this month and add it as one entry.
    if wcs_amount:
        # use the last day of month as the document date
        last_course_fee = entries_wcs[len(entries_wcs)-1:]
        document_date = datetime.strptime(last_course_fee[0]['date'], "%d.%m.%Y")
        last_day = calendar.monthrange(document_date.year, document_date.month)[1]
        last_day_date = datetime(document_date.year, document_date.month, last_day)

        month = MONTHS[int(entries_wcs[0]['date'].split('.')[1])-1]

        document_id = create_document(cursor, last_day_date)
        insert_row(cursor, document_id, wcs_amount, '2PI-workshop / {}'.format(month))

    wcs2_amount = 0.0
    for entry in entries_wcs2:
        wcs2_amount += entry['amount']

    # Calculate the sum of workshop fees received this month and add it as one entry.
    if wcs2_amount:
        # use the last day of month as the document date
        last_fee = entries_wcs2[len(entries_wcs2)-1:]
        document_date = datetime.strptime(last_fee[0]['date'], "%d.%m.%Y")
        last_day = calendar.monthrange(document_date.year, document_date.month)[1]
        last_day_date = datetime(document_date.year, document_date.month, last_day)

        month = MONTHS[int(entries_wcs2[0]['date'].split('.')[1])-1]

        document_id = create_document(cursor, last_day_date)
        insert_row(cursor, document_id, wcs2_amount, 'May We Swing / {}'.format(month))

    # Calculate the sum of course fees received this month and add it as one entry.
    course_fees = 0.0
    for entry in entries_coursefees:
        course_fees += entry['amount']

    if course_fees:
        # use the last day of the month as the document date
        last_course_fee = entries_coursefees[len(entries_coursefees)-1:]
        document_date = datetime.strptime(last_course_fee[0]['date'], "%d.%m.%Y")
        last_day = calendar.monthrange(document_date.year, document_date.month)[1]
        last_day_date = datetime(document_date.year, document_date.month, last_day)

        month = MONTHS[int(entries[0]['date'].split('.')[1])-1]

        document_id = create_document(cursor, last_day_date)
        insert_row(cursor, document_id, course_fees, 'Kurssimaksut / {}'.format(month))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
