from datetime import datetime
import uuid
import email
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector
import imaplib

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


class acceleration_client:

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
                            job_description,
                            client_id,
                            job_id

                        ) VALUES (
                            %(client_jobid)s,
                            %(job_title)s,
                            %(job_start_date)s,
                            %(job_end_date)s,
                            %(location)s, 
                            %(job_description)s,                         
                            %(client_id)s,
                            %(job_id)s
                        )
                    """

                    data_to_insert = {
                        "client_jobid": result_data['client_jobid'],
                        "job_title": result_data['job_title'],
                        "job_start_date": result_data['job_start_date'],
                        "job_end_date": result_data['job_end_date'],
                        "location": result_data['location'],
                        "job_description": result_data['job_description'],
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
                    "SELECT client_id,client,imapUserEmail,imapPassword FROM client_db_email_extract WHERE client ='acceleration'" )
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

    def acceleration_extract_information(self):
        global email_subject, result
        labels = "acceleration"
        client_data = self.extract_client_details()
        unread_emails = extracting_mail( client_data[2], client_data[3], labels )
        # print(unread_emails)
        extracted_list = []
        result_data = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            formatted_result = {
                'client_jobid': None,
                'location': None,
                'job_title': None,
                "job_start_date": None,
                'job_end_date': None,
                'job_description': None

            }
            if 'New Requisition' in email_subject:
                lines = email_body.split( '\n' )
                # print(lines)
                for line in lines:
                    parts = line.split( ":", 1 )
                    if len( parts ) == 2:
                        keyword, value = parts[0].strip(), parts[1].strip()
                        # print(keyword)
                        if keyword == '**Job Information** | Job ID':
                            formatted_result["client_jobid"] = value.strip( '|' ).strip( ' ' )
                        if keyword == 'Job Location':
                            formatted_result["location"] = value.strip( '|' )
                        if keyword == 'Job Title':
                            formatted_result["job_title"] = value.strip( '|' )
                        if keyword == 'Start Date':
                            start_date = \
                            email_body.split( 'Start Date: |', 1 )[1].replace('\n','').split( 'End Date: |', 1 )[
                                0].strip()
                            start_date = start_date.strip()
                            week_start_date = datetime.strptime( start_date, '%m/%d/%Y' )
                            week_date = datetime.strftime( week_start_date, '%Y-%m-%d' )
                            formatted_result['job_start_date'] = week_date

                        if keyword == 'End Date':
                            end_date = \
                                email_body.split( 'End Date: |', 1 )[1].replace( '\n', '' ).split( "Report To Manager's Office Address: | ", 1 )[
                                    0].strip()
                            end_date = end_date.strip()
                            week_end_date = datetime.strptime( end_date, '%m/%d/%Y' )
                            week_date = datetime.strftime( week_end_date, '%Y-%m-%d' )
                            formatted_result['job_end_date'] = week_date
                        if keyword == "Report To Manager's Office Address":
                            formatted_result['job_description'] = 'report_location:'+value.strip( '|' ).replace( ',', '' )

                        formatted_result["job_id"] = str( uuid.uuid4() )
                        formatted_result['date_received'] = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )

                        result_data = {
                            "client_jobid": formatted_result.get( 'client_jobid', '' ),  # Use get() with a default value
                            "job_start_date": formatted_result.get( 'job_start_date', '' ),
                            "job_end_date": formatted_result.get( 'job_end_date', '' ),
                            "location": formatted_result.get( 'location', '' ),
                            "job_title": formatted_result.get( 'job_title', '' ),
                            "job_description": formatted_result.get( 'job_description', '' ),
                            "job_id": formatted_result.get( 'job_id', '' ),
                            'client_id': client_data[0],
                            'client': client_data[1],
                            'date_received': formatted_result.get( 'date_received', '' )
                        }
        extracted_list.append( result_data )
        print( result_data )
        result = self.insert_data_into_mysql( extracted_list )
        return result


results = acceleration_client()
results.acceleration_extract_information()
