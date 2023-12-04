import uuid
import email
from datetime import datetime
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector
from bs4 import BeautifulSoup
import imaplib as imp

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456789pB@",
    "database": "email_extract",
}


def extracting_mail(imapUserEmail, imapPassword, label):
    imapHostServer = 'imap.gmail.com'
    imapVar = imp.IMAP4_SSL( imapHostServer )
    imapVar.login( imapUserEmail, imapPassword )
    labels = label
    imapVar.select( labels )
    unread_emails = []
    tmp, data = imapVar.search( None, 'UNSEEN' )
    for n in data[0].split():
        typ, msg_data = imapVar.fetch( n, '(RFC822)' )
        msg = email.message_from_bytes( msg_data[0][1] )

        subject = msg["Subject"]
        sender = email.utils.parseaddr( msg["From"] )[1]
        email_text = ""
        for part in msg.walk():
            if part.get_payload( decode=True ) is not None:
                body = part.get_payload( decode=True ).decode( "UTF-8", errors="ignore" )
                email_text += body
        date_received = parsedate_to_datetime( msg["Date"] )

        email_data = {
            "subject": subject,
            "sender": sender,
            "body": email_text,
            "date_received": date_received
        }

        unread_emails.append( email_data )
    print( unread_emails )
    return unread_emails


class genesis_client:

    def insert_data_into_mysql(self, result_dat):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            cursor = conn.cursor()

            for result_data in result_dat:

                cursor.execute(
                    "SELECT COUNT(*) FROM extracted_data_db WHERE client_jobid = %s",
                    (result_data['client_jobid'],)
                )
                record_count = cursor.fetchone()[0]

                if record_count == 0:
                    insert_query = """
                        INSERT INTO extracted_data_db (
                            client_jobid,
                            job_title,
                            job_start_date, 
                            job_end_date, 
                            location,
                            job_status,
                            client_id,
                            job_id

                        ) VALUES (
                            %(client_jobid)s,
                            %(job_title)s,
                            %(job_start_date)s,
                            %(job_end_date)s,
                            %(location)s,                          
                            %(job_status)s,
                            %(client_id)s,
                            %(job_id)s
                        )
                    """

                    data_to_insert = {
                        "client_jobid": result_data['client_jobid'],
                        'job_title':result_data['job_title'],
                        "job_start_date": result_data['job_start_date'],
                        "job_end_date": result_data['job_end_date'],
                        "location": result_data['location'],
                        "job_status": result_data['job_status'],
                        "client_id": result_data['client_id'],
                        "job_id": str( uuid.uuid4() )
                    }
                    cursor.execute( "SET FOREIGN_KEY_CHECKS = 0" )
                    # Execute the insert query
                    cursor.execute( insert_query, data_to_insert )

            conn.commit()

            return cursor.fetchall()
        except mysql.connector.Error as error:
            print( f"Error inserting data into MySQL: {error}" )

        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def extract_client_details(self):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT client_id,client,imapUserEmail,imapPassword FROM client_db_email_extract WHERE client ='genesis'" )
                result = cursor.fetchone()

                if result:
                    return result
                else:
                    # Handle the case where no result was found
                    return None

        except mysql.connector.Error as error:
            print( f"Error connecting to MySQL: {error}" )

        finally:
            if conn.is_connected():
                conn.close()

    def genesis_extract_information(self):
        global email_subject, result
        labels = "Genesis"
        client_data = self.extract_client_details()
        unread_emails = extracting_mail( client_data[2], client_data[3], labels )
        # print(unread_emails)
        extracted_list = []
        result_data = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            soup = BeautifulSoup( email_body, 'html.parser' )
            table = soup.find_all( 'table' )
            # print(email_body)
            if 'added' in email_subject:
                if table:

                    for t in table:
                        table_data = []
                        for row in t.find_all( 'tr' ):
                            row_data = [cell.get_text( strip=True ) for cell in row.find_all( 'td' )]
                            table_data.append( row_data )
                            value_data = table_data[-1:][0]
                            html_convertor = html2text.HTML2Text()
                            htmltxt = html_convertor.handle( email_body )
                            # print(table_data)
                            a = ''
                            data = htmltxt.split( '\n' )
                            for i in data:
                                if '#|' in i:
                                    a += i

                                if i.startswith( 'City' ):
                                    a += i
                            column = ["".join( item.split() ) for item in a[3:].split( '|' )]
                            zip_var = dict( zip( column, value_data ) )
                            # print( zip_var )
                            for column, value_data in zip_var.items():
                                if column == 'ShiftDate':
                                    if '-' in value_data:
                                        start, end = [date.strip() for date in value_data.split( '-' )]
                                        start = start.strip()
                                        week_start_date = datetime.strptime( start, '%m/%d/%Y' )
                                        week_date = datetime.strftime( week_start_date, '%Y-%m-%d' )
                                        zip_var['Start'] = week_date

                                        end = end.strip()
                                        week_end_date = datetime.strptime( end, '%m/%d/%Y' )
                                        week_date = datetime.strftime( week_end_date, '%Y-%m-%d' )
                                        zip_var['End'] = week_date
                            zip_var['State'] = zip_var.get( 'Area', '' ) + "," + zip_var.get( 'City',
                                                                                              '' ) + "," + zip_var.get(
                                "State", '' )
                            zip_var["job_id"] = str( uuid.uuid4() )
                            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
                            zip_var['date_received'] = date_received
                            zip_var['job_status'] = 'open'
                            zip_var['job_title']=None
                            result_data = {
                                "client_jobid": zip_var.get( 'ID', '' ),
                                "job_title":zip_var.get('job_title',''),
                                "job_start_date": zip_var.get( 'Start', '' ),
                                "job_end_date": zip_var.get( 'End', '' ),
                                "location": zip_var.get( 'State', '' ),
                                "job_status": zip_var.get( 'job_status', '' ),
                                "job_id": zip_var.get( 'job_id', '' ),
                                'client_id': client_data[0],
                                'client': client_data[1],
                                'date_received': zip_var.get( 'date_received', '' )
                            }
        extracted_list.append( result_data )
        print( extracted_list )
        result = self.insert_data_into_mysql( extracted_list )
        return result


results = genesis_client()
results.genesis_extract_information()
