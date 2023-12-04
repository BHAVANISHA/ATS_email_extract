import uuid
import email
import imaplib
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456789pB@",
    "database": "email_extract",
}


def extracting_mail(imapUserEmail, imapPassword, label):
    imap_server = "imap.gmail.com"
    imap_port = 993

    imap_conn = imaplib.IMAP4_SSL( imap_server, imap_port )

    imap_conn.login( imapUserEmail, imapPassword )

    labels = label
    imap_conn.select( labels )

    _, message_ids = imap_conn.search( None, "(UNSEEN)" )

    unread_emails = []

    for message_id in message_ids[0].split():
        try:
            _, data = imap_conn.fetch( message_id, "(RFC822)" )
            raw_email = data[0][1]
            email_message = email.message_from_bytes( raw_email )

            subject = email_message["Subject"]
            sender = email.utils.parseaddr( email_message["From"] )[1]
            body1 = ""
            date_received = parsedate_to_datetime( email_message["Date"] )

            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_type() == "text/plain":
                        body1 = part.get_payload( decode=True ).decode()
                    elif part.get_content_type() == "text/html":
                        body1 = html2text.html2text( part.get_payload( decode=True ).decode() )
            else:
                body1 = email_message.get_payload( decode=True ).decode()

            email_data = {
                "subject": subject,
                "sender": sender,
                "body": body1,
                "date_received": date_received
            }

            unread_emails.append( email_data )
        except Exception as e:
            print( f"Error processing email: {str( e )}" )

            imap_conn.store( message_id, '-FLAGS', '(\Seen)' )

    imap_conn.close()
    print( unread_emails )
    return unread_emails


class omnia_client_update:
    def insert_data_into_mysql(self, result_data):
        global update_query
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
            )

            cursor = conn.cursor()
            for result_data_item in result_data:
                cursor.execute(
                    "SELECT COUNT(*) FROM extracted_data_db WHERE client_jobid = %s",
                    (result_data_item['client_jobid'],)
                )
                record_count = cursor.fetchone()[0]

                if record_count == 0:
                    update_query = """
                    INSERT INTO extracted_data_db(
                       job_title,
                       client_jobid,
                       location,
                       comments,
                       job_description,
                       client_id,
                       job_id,
                       job_status
                    ) VALUES (
                       %(job_title)s,
                       %(client_jobid)s,
                       %(location)s,
                       %(comments)s,
                       %(job_description)s,
                       %(client_id)s,
                       %(job_id)s,
                       %(job_status)s
                   )                    """

                    data_to_insert = {

                    "job_title": result_data_item['job_title'],
                    "client_jobid": result_data_item['client_jobid'],
                    "location": result_data_item['location'],
                    "comments": result_data_item['comments'],
                    "job_description": result_data_item['job_description'],
                    "client_id": result_data_item['client_id'],
                    "job_id": str( uuid.uuid4() ),
                    "job_status": result_data_item['job_status']

                    }
                    cursor.execute( "SET FOREIGN_KEY_CHECKS = 0" )
                    cursor.execute( update_query, data_to_insert )

            conn.commit()
            return "Data inserted/updated successfully"
        except mysql.connector.Error as error:
            print( f"Error inserting/updating data into MySQL: {error}" )

        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def update_result_data(self, result_dat):
        try:

            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            cursor = conn.cursor()

            for result_data in result_dat:
                update_query = """UPDATE extracted_data_db SET job_status = %(job_status)s, comments = %(comments)s
                WHERE client_jobid = %(client_jobid)s"""

                data_to_update = {
                    "job_status": result_data["job_status"],
                    "client_jobid": result_data['client_jobid'],
                    "comments": result_data['comments']
                }

                cursor.execute( "SET FOREIGN_KEY_CHECKS = 0" )

                cursor.execute( update_query, data_to_update )

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
                cursor.execute( "SELECT client_id,imapUserEmail,imapPassword,client "
                                "FROM client_db_email_extract WHERE client = 'omnia'" )
                result = cursor.fetchone()

                if result:
                    return result
                else:
                    return None

        except mysql.connector.Error as error:
            print( f"Error connecting to MySQL: {error}" )

        finally:
            if conn.is_connected():
                conn.close()

    def extract_update_logic(self, unread_emails):

        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject=email_data['subject']

            extracted_data = {
                'client_jobid': None,
                'job_title': None,
                'location': None,
                'job_description': None,
                'job_status': 'open',
                'comments': None,
            }
            if 'closed' in email_subject:
                lines = email_body.split( '\n' )
                for i, line in enumerate( lines ):
                    if line.startswith( "Requisition#" ):
                        extracted_data['client_jobid'] = line.split( ":" )[1].strip()
                    elif line.startswith( "Job Category" ):
                        extracted_data['job_title'] = line.split( ":" )[1].strip()
                    elif line.startswith( "Location" ):
                        extracted_data['location'] = line.split( ":" )[1].strip()

                    elif 'closed' in email_data['subject']:
                        extracted_data['job_status'] = 'closed'
                        extracted_data['comments'] = ' the requisition has been closed due to all positions being filled.'

                    result_data_item = {
                        'client_jobid': extracted_data['client_jobid'],
                        'job_title': extracted_data['job_title'],
                        'location': extracted_data['location'],
                        'comments': extracted_data['comments'],
                        'job_description': extracted_data['job_description'],
                        # 'job_id': job_id,
                        'job_status': extracted_data['job_status'],
                    }
        extracted_list.append( result_data_item )
        result = self.update_result_data( extracted_list )
        # print( extracted_list )
        return result

    # =================================================================================================================
    def omnia_extract_details(self):
        global email_subject, result
        labels = 'omnia'
        client_data = self.extract_client_details()

        unread_emails = extracting_mail( client_data[1], client_data[2], labels )
        result = None
        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
            if ' Open' in email_subject:
                client_jobid = None
                if "Requisition #:" in email_body:
                    requisition_index = email_body.find( "Requisition #:" ) + len( "Requisition #:" )
                    end_index = email_body.find( "\n", requisition_index )
                    if end_index != -1:
                        client_jobid = email_body[requisition_index:end_index].strip()

                # Extract job category (Job Category: Bilingual Specialist Social Services Publications)
                job_title = None
                if "Job Category:" in email_body:
                    category_index = email_body.find( "Job Category:" ) + len( "Job Category:" )
                    end_index = email_body.find( "\n", category_index )
                    if end_index != -1:
                        job_title = email_body[category_index:end_index].strip()

                # Extract work location (Contractor's Work Location: Maricopa County)
                location = None
                if "Contractor's Work Location:" in email_body:
                    location_index = email_body.find( "Contractor's Work Location:" ) + len( "Contractor's Work Location:" )
                    end_index = email_body.find( "\n", location_index )
                    if end_index != -1:
                        location = email_body[location_index:end_index].strip()

                job_status=None
                comments = None
                if ' Open Position' in email_subject:
                    job_status = 'open'
                if "Broadcast Comments :" in email_body:
                    comments = email_body.split( "Broadcast Comments :",1 )[1].replace('\n','').split('Position Description :',1)[0].strip(' ')
                job_description = None
                if "Position Description :" in email_body:
                    job_description = email_body.split( "Position Description :",1 )[1].replace('\n','').split('PLEASE DO NOT REPLY TO THIS MESSAGE',1)[0]

                job_id = str( uuid.uuid4() )

                result_data_item = {
                    'client_jobid': client_jobid,
                    'job_title': job_title,
                    'location': location,
                    'comments': comments,
                    'job_description': job_description,
                    'job_id': job_id,
                    'job_status': job_status,
                    'date_received': date_received,
                    'client_id': client_data[0],
                    'client': client_data[3],
                }
        extracted_list.append( result_data_item )
        print( extracted_list )
        if 'Open' in email_subject:
            result = self.insert_data_into_mysql( extracted_list )
        elif 'closed' in email_subject:
            result = self.extract_update_logic( unread_emails )

        return result


insance = omnia_client_update()
insertion_result = insance.omnia_extract_details()
