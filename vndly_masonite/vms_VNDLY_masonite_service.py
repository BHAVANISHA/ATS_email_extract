import uuid
import email
import imaplib
from datetime import datetime
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


class vndly_masonite_client_update:
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

                       client_jobid,
                       job_title,
                       no_of_positions,
                       location,
                       job_start_date,
                       job_end_date,
                       comments,
                       job_description,
                       client_id,
                       job_id,
                       job_status
                    ) VALUES (
                       %(client_jobid)s,
                       %(job_title)s,
                       %(no_of_positions)s,
                       %(location)s,
                       %(job_start_date)s,
                       %(job_end_date)s,
                       %(comments)s,
                       %(job_description)s,
                       %(client_id)s,
                       %(job_id)s,
                       %(job_status)s
                   )                    """

                data_to_insert = {
                    "client_jobid": result_data_item['client_jobid'],
                    "job_title": result_data_item['job_title'],
                    "no_of_positions": result_data_item['no_of_positions'],
                    "location": result_data_item['location'],
                    "job_start_date": result_data_item['job_start_date'],
                    "job_end_date": result_data_item['job_end_date'],
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
                update_query = """UPDATE extracted_data_db SET job_status = %(job_status)s, comments = %(comments)s,no_of_positions=%(no_of_positions)s
                WHERE client_jobid = %(client_jobid)s"""

                data_to_update = {
                    "job_status": result_data["job_status"],
                    "client_jobid": result_data['client_jobid'],
                    "comments": result_data['comments'],
                    "no_of_positions": result_data['no_of_positions'],
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
                                "FROM client_db_email_extract WHERE client = 'vndly_masonite'" )
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

        global comments
        extracted_data = {
            'client_jobid': None,
            'job_title': None,
            'num_positions': None,
            'location': None,
            'job_start_date': None,
            'job_end_date': None,
            'job_status': None,
            'comments': None,
            'job_description': None,
        }

        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
            if 'Job Posting Ended' in email_subject:
                client_jobid = None
                if "Job Posting Ended: " in email_subject:
                    client_jobid = email_subject.split( 'Job Posting Ended: ' )[1].strip().split( '-', 1 )[0].strip()
                job_title = None
                if "Job Title - **" in email_body:
                    job_title = email_body.split( 'Job Title - **', 1 )[1].strip().split( '**', 1 )[0].strip()
                no_of_positions = None
                if "Number of Positions - **" in email_body:
                    no_of_positions = email_body.split( 'Number of Positions - **', 1 )[1].strip().split( '**', 1 )[
                        0].strip()
                location = None
                if "Job Location - **" in email_body:
                    location = email_body.split( "Job Location - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                job_start_date = None
                if "Job Start Date - **" in email_body:
                    job_start_date = email_body.split( "Job Start Date - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                    date_obj = datetime.strptime( job_start_date, '%m/%d/%y' )
                    job_start_date = date_obj.strftime( '%Y-%m-%d' )
                job_end_date = None
                if "Job End Date - **" in email_body:
                    job_end_date = email_body.split( "Job End Date - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                    date_obj = datetime.strptime( job_end_date, '%m/%d/%y' )
                    job_end_date = date_obj.strftime( '%Y-%m-%d' )
                if 'Job Posting Ended' in email_subject:
                    job_status = 'closed'
                else:
                    job_status = 'open'
                comments = None
                if job_status == 'closed':
                    comments = email_body.split( 'Job End Reason - ****', 1 )[1].strip().split( '.\n\n  \n\nThank', 1 )[
                        0].strip()

                job_description = None
                if "Job Description - ****\n\n**" in email_body:
                    job_description = \
                    email_body.split( "Job Description - ****\n\n**", 1 )[1].strip( '*' ).split( '.\n\n  \n\nThank you',
                                                                                                 1 )[0].strip( '\n' )

                result_data_item = {
                    'client_jobid': client_jobid,
                    'job_title': job_title,
                    'no_of_positions': no_of_positions,
                    'location': location,
                    'job_start_date': job_start_date,
                    'job_end_date': job_end_date,
                    'job_status': job_status,
                    'comments': comments,
                    'job_description': job_description,
                }
        extracted_list.append( result_data_item )
        result = self.update_result_data( extracted_list )
        print( extracted_list )
        return result

    # =================================================================================================================
    def vndly_masonite_extract_details(self):
        global email_subject, result, comments
        labels = 'vndly_masonite'
        client_data = self.extract_client_details()

        unread_emails = extracting_mail( client_data[1], client_data[2], labels )
        comments = None
        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )

            if 'New Job Published' in email_subject:

                client_jobid = None
                if "New Job Published:" in email_subject:
                    client_jobid = email_subject.split( 'New Job Published:' )[1].strip().split( ':', 1 )[0].strip()
                job_title = None
                if "Job Title - **" in email_body:
                    job_title = email_body.split( 'Job Title - **', 1 )[1].strip().split( '**', 1 )[0].strip()
                no_of_positions = None
                if "Number of Positions - **" in email_body:
                    no_of_positions = email_body.split( 'Number of Positions - **', 1 )[1].strip().split( '**', 1 )[
                        0].strip()
                location = None
                if "Job Location - **" in email_body:
                    location = email_body.split( "Job Location - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                job_start_date = None
                if "Job Start Date - **" in email_body:
                    job_start_date = email_body.split( "Job Start Date - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                    date_obj = datetime.strptime( job_start_date, '%m/%d/%y' )
                    job_start_date = date_obj.strftime( '%Y-%m-%d' )
                job_end_date = None
                if "Job End Date - **" in email_body:
                    job_end_date = email_body.split( "Job End Date - **", 1 )[1].strip().split( '**', 1 )[0].strip()
                    date_obj = datetime.strptime( job_end_date, '%m/%d/%y' )
                    job_end_date = date_obj.strftime( '%Y-%m-%d' )
                if 'job has been posted' in email_body:
                    job_status = 'open'
                    comments = None
                else:
                    job_status = None

                job_description = None
                if "Job Description - " in email_body:
                    job_description = \
                    email_body.split( "Job Description - ", 1 )[1].replace('\n','').strip( '*' ).split( '.\n\n  \n\nThank you',
                                                                                                 1 )[0].strip( '\n' )

                job_id = str( uuid.uuid4() )

                result_data_item = {
                    'client_jobid': client_jobid,
                    'job_title': job_title,
                    'no_of_positions': no_of_positions,
                    'location': location,
                    'job_start_date': job_start_date,
                    'job_end_date': job_end_date,
                    'job_status': job_status,
                    'comments': comments,
                    'job_description': job_description,
                    'job_id': job_id,
                    'date_received': date_received,
                    'client_id': client_data[0],
                    'client': client_data[3],
                }
        extracted_list.append( result_data_item )
        print( extracted_list )
        if 'Job Published' in email_subject:
            result = self.insert_data_into_mysql( extracted_list )
        if 'Job Posting Ended' in email_subject:
            result = self.extract_update_logic( unread_emails )

        return result


insance = vndly_masonite_client_update()
insertion_result = insance.vndly_masonite_extract_details()
