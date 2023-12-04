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

def get_unread_emails(imapUserEmail, imapPassword, label):
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

class fieldglass_kraft_vms_job_extraction:
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
                    "SELECT COUNT(*) FROM extracted_data_db WHERE clientjobid = %s",
                    (result_data_item['clientjobid'],)
                )
                record_count = cursor.fetchone()[0]

                if record_count == 0:
                    insert_query2 = "INSERT INTO extracted_data_db (job_title, clientjobid, location, comments, job_description, client_id, client, job_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    data_to_insert = {
                        "job_title": result_data_item['job_title'],
                        "clientjobid": result_data_item['clientjobid'],
                        "location": result_data_item['location'],
                        "comments": result_data_item['comments'],
                        "job_description": result_data_item['job_description'],
                        "client_id": result_data_item['client_id'],
                        "client": result_data_item['client'],
                        "job_id": str( uuid.uuid4() )
                    }
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
                            "FROM client_db_email_extract WHERE client = 'omnia'" )
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

    def extract_data_details(self):
        labels = 'omnia'
        vms_data = self.extract_client_details()

        unread_emails = get_unread_emails(vms_data[1], vms_data[2], labels)

        extracted_list = []
        for email_data in unread_emails:
            email_body = email_data['body']
            # Extract requisition number (Requisition #: 2161)
            clientjobid = None
            if "Requisition #:" in email_body:
                requisition_index = email_body.find("Requisition #:") + len("Requisition #:")
                end_index = email_body.find("\n", requisition_index)
                if end_index != -1:
                    clientjobid = email_body[requisition_index:end_index].strip()

            # Extract job category (Job Category: Bilingual Specialist Social Services Publications)
            job_title = None
            if "Job Category:" in email_body:
                category_index = email_body.find("Job Category:") + len("Job Category:")
                end_index = email_body.find("\n", category_index)
                if end_index != -1:
                    job_title = email_body[category_index:end_index].strip()

            # Extract work location (Contractor's Work Location: Maricopa County)
            location = None
            if "Contractor's Work Location:" in email_body:
                location_index = email_body.find("Contractor's Work Location:") + len("Contractor's Work Location:")
                end_index = email_body.find("\n", location_index)
                if end_index != -1:
                    location = email_body[location_index:end_index].strip()

            # Extract Broadcast Comments
            comments = None
            if "Broadcast Comments :" in email_body:
                comments_index = email_body.find("Broadcast Comments :") + len("Broadcast Comments :")
                end_index = email_body.find("\n", comments_index)
                if end_index != -1:
                    comments = email_body[comments_index:end_index].strip()

            # Extract Position Description
            job_description = None
            if "Position Description :" in email_body:
                description_index = email_body.find("Position Description :") + len("Position Description :")
                end_index = email_body.find("\n", description_index)
                if end_index != -1:
                    job_description = email_body[description_index:end_index].strip()

            job_id = str(uuid.uuid4())

            # Create a dictionary for the extracted data
            result_data_item = {
                'clientjobid': clientjobid,
                'job_title': job_title,
                'location': location,
                'comments': comments,
                'job_description': job_description,
                'job_id': job_id,
                'client_id': vms_data[0],
                'client': vms_data[3],
            }
            extracted_list.append(result_data_item)

        result = self.insert_data_into_mysql(extracted_list)
        print(extracted_list)
        return result

insance = fieldglass_kraft_vms_job_extraction()
insertion_result = insance.extract_data_details()
# print(insertion_result)
