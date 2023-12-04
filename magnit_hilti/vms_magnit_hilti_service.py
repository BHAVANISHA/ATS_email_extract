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


class magnit_hilti_update:
    def insert_data_into_mysql(self, result_data):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
            )

            cursor = conn.cursor()
            update_query = None  # Initialize update_query with None

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
                       job_start_date,
                       job_end_date,
                       no_of_positions,
                       comments,
                       job_description,
                       client_id,
                       job_id,
                       job_status
                    ) VALUES (
                       %(job_title)s,
                       %(client_jobid)s,
                       %(location)s,
                       %(job_start_date)s,
                       %(job_end_date)s,
                       %(no_of_positions)s,
                       %(comments)s,
                       %(job_description)s,
                       %(client_id)s,
                       %(job_id)s,
                       %(job_status)s
                    )"""

                data_to_insert = {
                    "job_title": result_data_item['job_title'],
                    "client_jobid": result_data_item['client_jobid'],
                    "location": result_data_item['location'],
                    "job_start_date": result_data_item['job_start_date'],
                    "job_end_date": result_data_item['job_end_date'],
                    "no_of_positions": result_data_item['no_of_positions'],
                    "comments": result_data_item['comments'],
                    "job_description": result_data_item['job_description'],
                    "client_id": result_data_item['client_id'],
                    "job_id": str( uuid.uuid4() ),
                    "job_status": result_data_item['job_status'],
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
                                "FROM client_db_email_extract WHERE client = 'magnit_hilti'" )
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

        job_description = []
        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
            if 'Requisition Closed Notification for Requisition' in email_subject:
                comments = None
                client_jobid = None
                if "Requisition Closed Notification for Requisition" in email_subject:
                    client_jobid = \
                    email_subject.split( "Requisition Closed Notification for Requisition", 1 )[1].strip( ' ' ).split( ',',
                                                                                                                       1 )[
                        0].strip()

                job_status = None
                if "Closed" in email_subject:
                    job_status = 'closed'
                    comments = email_body.split( 'Thank you', 1 )[0].replace( '\n', '' ).strip(' ')

                job_description = ' '.join( job_description )
                job_id = str( uuid.uuid4() )

                result_data_item = {
                    'client_jobid': client_jobid,
                    'comments': comments,
                    'job_description': job_description,
                    'job_id': job_id,
                    'job_status': job_status,
                    'date_received': date_received,

                }
        extracted_list.append( result_data_item )
        result = self.update_result_data( extracted_list )
        print( extracted_list )
        return result

    # =================================================================================================================
    def magnit_hilti_extract_details(self):
        global email_subject, result, comments
        labels = 'magnit_hilti'
        client_data = self.extract_client_details()

        unread_emails = extracting_mail( client_data[1], client_data[2], labels )
        result = None
        job_description = []
        extracted_list = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
            if 'Resume Request Notification' in email_subject:
                client_jobid = None
                if "Req#: " in email_body:
                    client_jobid = email_body.split( "Req#: ", 1 )[1].strip( ' ' ).split( 'Name:', 1 )[0].replace( ' ',
                                                                                                                  '' ).replace(
                        '\n', '' )

                if "Name:" in email_body:
                    name = email_body.split( 'Name:', 1 )[1].strip().split( 'Type:', 1 )[0].strip()
                    job_description.append( 'name:' + name )
                if "Type:" in email_body:
                    type = email_body.split( 'Type:', 1 )[1].strip().split( 'Status:', 1 )[0].strip()
                    job_description.append( 'type:' + type )
                job_status = None
                if "Status:" in email_body:
                    job_status = email_body.split( 'Status:', 1 )[1].strip().split( 'Client:', 1 )[0].strip()
                comments = None
                client_name = None
                if "Client:" in email_body:
                    client_name = email_body.split( 'Client:', 1 )[1].strip().split( 'Start: ', 1 )[0].strip()
                    job_description.append('client_name'+client_name)
                job_start_date = None
                if "Start:" in email_body:
                    # job_start_date = email_body.split( 'Start:', 1 )[1].strip().split( 'End:', 1 )[0].strip()
                    start_date = \
                        email_body.split( 'Start:', 1 )[1].replace( '\n', '' ).split( 'End: ', 1 )[
                            0].strip()
                    start_date = start_date.strip()
                    week_start_date = datetime.strptime( start_date, '%m/%d/%Y' )
                    week_date = datetime.strftime( week_start_date, '%Y-%m-%d' )
                    job_start_date = week_date
                job_end_date = None
                if "End:" in email_body:
                    end_date = \
                        email_body.split( 'End: ', 1 )[1].replace( '\n', '' ).split( 'Description: ', 1 )[
                            0].strip()
                    end_date = end_date.strip()
                    week_end_date = datetime.strptime( end_date, '%m/%d/%Y' )
                    week_date = datetime.strftime( week_end_date, '%Y-%m-%d' )
                    job_end_date = week_date
                if "Description: " in email_body:
                    job_description1 = email_body.split( 'Reason:', 1 )[1].strip().split( 'Department:', 1 )[0].strip()
                    job_description.append( 'department:' + job_description1 )
                if "Department:" in email_body:
                    department = email_body.split( 'Department:', 1 )[1].strip().split( 'Job Category:', 1 )[0].strip()
                    job_description.append( 'department:' + department )
                if "Job Category: " in email_body:
                    job_category = email_body.split( 'Job Category: ', 1 )[1].strip().split( 'Job Title: ', 1 )[0].strip()
                    job_description.append( 'job_category:' + job_category )
                job_title = None
                if "Job Title:" in email_body:
                    job_title = email_body.split( 'Job Title:', 1 )[1].strip().split( 'Duties:', 1 )[0].strip()
                if "Duties: " in email_body:
                    duties = email_body.split( 'Duties: ', 1 )[1].strip().split( '# of Positions:', 1 )[0].strip()
                    job_description.append( 'duties:' + duties )
                no_of_positions = None
                if "# of Positions:" in email_body:
                    no_of_positions = email_body.split( '# of Positions:', 1 )[1].strip().split( 'Location:', 1 )[0].strip()
                location = None
                if "Location:" in email_body:
                    location = email_body.split( 'Location:', 1 )[1].strip().split( 'Schedule: ', 1 )[0].strip()
                if "Hours Per Week:" in email_body:
                    h_p_w = email_body.split( 'Hours Per Week:', 1 )[1].strip().split( 'Hours Per Day:', 1 )[0].strip()
                    job_description.append( 'h_p_w:' + h_p_w )
                if "Hours Per Day:" in email_body:
                    h_p_d = email_body.split( 'Hours Per Day:', 1 )[1].strip().split( 'Interview Notes:', 1 )[0].strip()
                    job_description.append( 'h_p_d:' + h_p_d )
                if "Interview Notes:" in email_body:
                    interview_notes = email_body.split( 'Interview Notes:', 1 )[1].strip().split( 'Thank you,', 1 )[
                        0].strip()
                    job_description.append( 'interview_notes:' + interview_notes )

                job_description = ' '.join( job_description )
                job_id = str( uuid.uuid4() )

                result_data_item = {
                    'client_jobid': client_jobid,
                    'job_title': job_title,
                    'location': location,
                    'job_start_date': job_start_date,
                    'job_end_date': job_end_date,
                    'no_of_positions': no_of_positions,
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
        if 'Resume Request Notification' in email_subject:
            result = self.insert_data_into_mysql( extracted_list )
        if 'Closed' in email_subject:
            result = self.extract_update_logic( unread_emails )

        return result


insance = magnit_hilti_update()
insertion_result = insance.magnit_hilti_extract_details()
