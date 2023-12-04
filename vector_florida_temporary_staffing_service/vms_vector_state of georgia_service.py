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
    return unread_emails


class florida_client:

    def insert_data_into_mysql(self, result_data):
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
                    insert_query2 = "INSERT INTO extracted_data_db (job_title,job_description, client_jobid,job_status, client_id, job_id) VALUES (%s,%s,%s, %s, %s, %s)"
                    data_to_insert = (
                        result_data_item['job_title'],
                        result_data_item['job_description'],
                        result_data_item['client_jobid'],
                        result_data_item['job_status'],
                        result_data_item['client_id'],
                        result_data_item['job_id'],
                    )
                    cursor.execute( "SET FOREIGN_KEY_CHECKS = 0" )
                    cursor.execute( insert_query2, data_to_insert )

            conn.commit()
            return cursor.fetchall()
        except mysql.connector.Error as error:
            print( f"Error inserting data into MySQL: {error}" )
        finally:
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()

    def extract_client_details(self):

        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
            )

            cursor = conn.cursor()
            cursor.execute( "SELECT client_id, imapUserEmail, imapPassword, client "
                            "FROM client_db_email_extract WHERE client = 'florida_temporary_staffing_service'" )
            result = cursor.fetchone()

            if result:
                return result
            else:
                return None
        except mysql.connector.Error as error:
            print( f"Error connecting to MySQL: {error}" )
        finally:
            if cursor:
                cursor.fetchall()  # Consume any unread results
                cursor.close()
            if conn and conn.is_connected():
                conn.close()

    def update_result_data(self, extracted_list):
        try:

            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            cursor = conn.cursor()

            for result_data in extracted_list:
                update_query = """UPDATE extracted_data_db SET job_status = %(job_status)s
            WHERE client_jobid = %(client_jobid)s"""


                data_to_update = {
                    "job_status": result_data["job_status"],
                    "client_jobid": result_data['client_jobid'],

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

    def extract_update_logic(self, unread_emails):
        result_data_item = {}
        extracted_list = []
        job_description=[]
        job_status = None

        for email_data in unread_emails:
            email_subject=email_data['subject']
            if 'Filled' in email_subject:

                if 'Been Filled' in email_data['subject']:
                    job_status = 'closed'
                if "Req." in email_data["subject"]:
                    client_jobid = email_data["subject"].split("Req.",1)[1].split("Has",1)[0].strip(" ")
                    result_data_item["client_jobid"]=client_jobid
                # Extract client from the subject
                client_start = email_data['subject'].find( 'VectorVMS Msg:' )
                if client_start != -1:
                    client_end = email_data['subject'].find( ' Contract:', client_start )
                    if client_end != -1:
                        client = email_data['subject'][client_start + len( 'VectorVMS Msg:' ):client_end]
                    else:
                        client = None
                else:
                    client = None
                job_description.append('client_name:'+client)

                title_start = email_data['body'].find( 'Role/Title:' )
                if title_start != -1:
                    title_end = email_data['body'].find( 'System Requisition ID:', title_start )
                    if title_end != -1:
                        title_data = email_data['body'][title_start + len( 'Role/Title:' ):title_end].strip()
                        title = title_data.replace( '\n', ',' ).strip( "|-," )
                    else:
                        title = None
                else:
                    title = None
                job_description1=' '.join(job_description)
                result_data_item['job_description']=job_description1
                result_data_item['job_title'] = title
                result_data_item['job_id'] = str( uuid.uuid4() )
                result_data_item['job_status'] = job_status
        extracted_list.append( result_data_item )
        result = self.update_result_data( extracted_list )
        print( extracted_list )
        return result

    def florida_extract_details(self):
        global email_subject, result, job_status
        labels = 'florida_temporary_staffing_service'
        client_data = self.extract_client_details()
        job_description=[]
        unread_emails = extracting_mail( client_data[1], client_data[2], labels )
        result_data_item = {}
        extract_list = []
        job_status = None
        for email_data in unread_emails:
            email_subject = email_data['subject']
            if 'Open' in email_subject:
                if 'Now Open' in email_data['subject']:
                    job_status = 'open'
                subject_parts = email_data['subject'].split( 'Req. ' )
                if len( subject_parts ) > 1:
                    requisition_id = subject_parts[1].split( ' Now Open' )[0]
                else:
                    requisition_id = None
                result_data_item['client_jobid'] = requisition_id

                client_start = email_data['subject'].find( 'VectorVMS Msg:' )
                if client_start != -1:
                    client_end = email_data['subject'].find( ' Contract:', client_start )
                    if client_end != -1:
                        client = email_data['subject'][client_start + len( 'VectorVMS Msg:' ):client_end]
                    else:
                        client = None
                else:
                    client = None
                job_description.append('client_name:'+client)

                # Extract title from the body
                title_start = email_data['body'].find( 'Role/Title:' )
                if title_start != -1:
                    title_end = email_data['body'].find( 'System Requisition ID:', title_start )
                    if title_end != -1:
                        title_data = email_data['body'][title_start + len( 'Role/Title:' ):title_end].strip()
                        title = title_data.replace( '\n', ',' ).strip( "|-," )
                    else:
                        title = None
                else:
                    title = None
                job_description1=' '.join(job_description)
                result_data_item['job_description']=job_description1
                result_data_item['job_title'] = title
                result_data_item['job_id'] = str( uuid.uuid4() )
                result_data_item['job_status'] = job_status
                result_data_item['client_id'] = client_data[0]
                result_data_item['client'] = client_data[3]
        extract_list.append(result_data_item)
        print(result_data_item)
        if 'Open' in email_subject:
            result = self.insert_data_into_mysql( extract_list )
            extract_list.append( result_data_item )

        elif 'Filled' in email_subject:
            result = self.extract_update_logic( unread_emails )
        return result


insance = florida_client()
insertion_result = insance.florida_extract_details()
