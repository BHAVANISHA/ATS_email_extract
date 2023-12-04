import uuid
import email
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector
import imaplib
from datetime import datetime

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


class vms_people_fluent_client:

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
                            location,
                            job_description,
                            job_start_date,
                            job_end_date,
                            job_bill_rate,
                            client_id,
                            job_id

                        ) VALUES (
                            %(client_jobid)s,
                            %(job_title)s,
                            %(location)s,
                            %(job_description)s,                          
                            %(job_start_date)s,
                            %(job_end_date)s,
                            %(job_bill_rate)s,
                            %(client_id)s,
                            %(job_id)s
                        )
                    """

                    data_to_insert = {
                        "client_jobid": result_data['client_jobid'],
                        "job_title": result_data['job_title'],
                        "location": result_data['location'],
                        "job_description": result_data['job_description'],
                        "job_start_date": result_data['job_start_date'],
                        "job_end_date": result_data['job_end_date'],
                        "job_bill_rate": result_data['job_bill_rate'],
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
                    "SELECT client_id,client,imapUserEmail,imapPassword FROM client_db_email_extract WHERE client ='vms_people_fluent'" )
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

    def vms_extract_information(self):
        global email_subject, result, result_data
        labels = "VMS"
        client_data = self.extract_client_details()
        unread_emails = extracting_mail( client_data[2], client_data[3], labels )
        # print(unread_emails)
        extracted_list=[]
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject=email_data['subject']
            sections = email_body.split( '**Additional Job Information:**' )
            labels = [
                "Bill Rate:",
                "Start Date:",
                "End Date:",
                "Location:",
                "Description:",
                "**Job Status",
                "Job ID:",
                "Title",
            ]
            extracted_data = {}
            for section in sections:
                section = section.strip()
                for label in labels:
                    if label in section:
                        data = section.split( label, 1 )[1].strip()
                        extracted_data[label] = data
            for label, data in extracted_data.items():
                lines = email_body.split( "\n" )
                email_text = email_body.replace( "\\n", ", " )
                job_description=[]
                info = {}
                if 'New job assignment' in email_subject:
                    for line in lines:
                        if "Bill Rate:" in line:
                            info["job_bill_rate"] = line.split( ":" )[1].strip( '**' ).replace(',','')
                        elif "**Start Date:**" in line:
                            start_date = \
                                email_body.split( '**Start Date:**', 1 )[1].replace( '\n', '' ).split( '**End Date:**', 1 )[
                                    0].replace("*",'')
                            start_date = start_date.strip()
                            week_start_date = datetime.strptime( start_date, '%m/%d/%y' )
                            week_date = datetime.strftime( week_start_date, '%y-%m-%d' )
                            info['job_start_date'] = week_date
                        elif "**End Date:**" in line:
                            end_date = \
                                email_body.split( '**End Date:**', 1 )[1].replace( '\n', '' ).split( '# of Openings: **', 1 )[
                                    0].replace('**','')
                            end_date = end_date.strip()
                            week_end_date = datetime.strptime( end_date, '%m/%d/%y' )
                            week_date = datetime.strftime( week_end_date, '%y-%m-%d' )
                            info['job_end_date'] = week_date
                        elif "**Description" in line:
                            description_lines = email_body.split( line )[1:]
                            description = "\n".join( [line.strip() for line in description_lines] ).strip()
                            info["job_description"] = description
                        elif "Location:" in line:
                            location = line.split( ":" )[1].strip( "*" ).replace( '\\n', ',' )
                            info["location"] = location
                        elif 'Title' in line:
                            info['job_title'] = line.split( ":" )[1].strip( "[*" )
                        elif '**Job Status' in line:
                            info['job_status'] = line.split( ":" )[1].strip( "**" )
                        elif "Job ID:" in line:
                            job_id = line.split( ":" )[1].strip( "*" )
                            info["client_jobid"] = job_id
                        elif "**Client:" in line:
                            client = line.split( ":" )[1].strip( "*" )
                            job_description.append('client:'+client)
                        info['date_received'] = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
                        info['job_description']=' '.join(job_description)
                        info['job_id'] = str( uuid.uuid4() )

                        result_data = {
                            "client_jobid": info.get( 'client_jobid', '' ),  # Use get() with a default value
                            "job_title": info.get( 'job_title', '' ),
                            "location": info.get( 'location', '' ),
                            "job_description": info.get( 'job_description', '' ),
                            "job_start_date": info.get( 'job_start_date', '' ),
                            "job_end_date": info.get( 'job_end_date', '' ),
                            "job_bill_rate": info.get( 'job_bill_rate', '' ),
                            "job_id": info.get( 'job_id', '' ),
                            'client_id': client_data[0],
                            'client': client_data[1],
                            'date_received': info.get( 'date_received', '' )
                        }
        extracted_list.append( result_data )
        print( result_data )
        result = self.insert_data_into_mysql( extracted_list )
        return result


results = vms_people_fluent_client()
results.vms_extract_information()
