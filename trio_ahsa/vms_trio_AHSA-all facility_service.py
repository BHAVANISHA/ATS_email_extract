import uuid
import email
from datetime import datetime
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


class ahsa_client:

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
                            job_status,
                            no_of_positions,
                            job_description,
                            job_start_date,
                            client_id,
                            job_id

                        ) VALUES (
                            %(client_jobid)s,
                            %(job_title)s,
                            %(job_status)s,
                            %(no_of_positions)s,
                            %(job_description)s,                          
                            %(job_start_date)s,
                            %(client_id)s,
                            %(job_id)s
                        )
                    """

                    data_to_insert = {
                        "client_jobid": result_data['client_jobid'],
                        "job_title":result_data['job_title'],
                        "job_status": result_data['job_status'],
                        "no_of_positions":result_data['no_of_positions'],
                        "job_description": result_data['job_description'],
                        "job_start_date":result_data['job_start_date'],
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
                    "SELECT client_id,client,imapUserEmail,imapPassword FROM client_db_email_extract WHERE client ='ahsa'" )
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
                update_query = """UPDATE extracted_data_db SET job_status = %(job_status)s,no_of_positions=%(no_of_positions)s
                WHERE client_jobid = %(client_jobid)s"""

                data_to_update = {
                    "job_status": result_data["job_status"],
                    "client_jobid": result_data['client_jobid'],
                    "no_of_positions": result_data['no_of_positions']
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

        global comments
        extracted_list = []
        job_description1 = []
        result_data_item = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            if 'Job Listing' in email_subject:
                lines = email_body.split( '\n' )

                for line in lines:
                    client_jobid = None
                    if "Job Number: | [" in email_body:
                        client_jobid = email_body.split( 'Job Number: | [' )[1].strip().split( ']', 1 )[0].strip()
                        result_data_item['client_jobid'] = client_jobid
                    job_title = None
                    if 'Job Title: | ' in email_body:
                        job_title = email_body.split( 'Job Title: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['job_title'] = job_title

                    if "Modified | " in email_body:
                        modified = email_body.split( 'Modified | ',1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['modified'] = modified
                        job_description1.append( 'modified:' + modified )
                    if "Unit: | " in email_body:
                        unit = email_body.split( 'Unit: | ',1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['unit'] = unit
                        job_description1.append( 'unit:' + unit )
                    if "Rate Information: |" in email_body:
                        rate_info = email_body.split( 'Rate Information: |',1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['unit'] = rate_info
                        job_description1.append( 'modified:' + rate_info )
                    job_start_date = None
                    if "Start Date: | " in email_body:
                        job_start_date_str = email_body.split( 'Start Date: | ' )[1].strip().split( '\n', 1 )[0].strip()
                        job_start_date_obj = datetime.strptime( job_start_date_str, '%m/%d/%Y' )
                        job_start_date = job_start_date_obj.strftime( '%Y-%m-%d' )
                        result_data_item['job_start_date'] = job_start_date
                    if "Duration: | " in email_body:
                        duration = email_body.split( 'Duration: | ',1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['duration'] = duration
                        job_description1.append( 'duration:' + duration )
                    no_of_positions = None
                    if "# of Open Positions: | " in email_body:
                        no_of_positions = email_body.split( '# of Open Positions: | ',1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['no_of_positions'] = no_of_positions
                    if '**Position Status:** Closed' in email_body:
                        job_status = 'CLOSED'

                    else:
                        job_status = 'open'

                    if "Shift: | Days:" in email_body:
                        shift_days = email_body.split( 'Shift: | Days:', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['shift_days'] = shift_days
                        job_description1.append( 'shift_days:' + shift_days )
                    if "Shift Notes: | " in email_body:
                        shift_notes = email_body.split( 'Shift Notes: | ', 1 )[1].strip().split( '\nOn', 1 )[0].strip()
                        result_data_item['shift_notes'] = shift_notes
                        job_description1.append( 'shift_notes:' + shift_notes )
                    if "On Call Requirements: | " in email_body:
                        on_call_requirements = email_body.split( 'On Call Requirements: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['on_call_requirements'] = on_call_requirements
                        job_description1.append( 'on_call_requirements:' + on_call_requirements )
                    if "Weekend Requirements: | " in email_body:
                        weekend_requirements = email_body.split( 'Weekend Requirements: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['weekend_requirements'] = weekend_requirements
                        job_description1.append( 'weekend_requirements:' + weekend_requirements )
                    if "MANDATED Job Requirements: |" in email_body:
                        mandated_requirements = email_body.split( 'MANDATED Job Requirements: |', 1 )[1].strip( '*' ).split(
                            'Registration/Certification Requirements: |', 1 )[0].strip( '\n' )
                        result_data_item['mandated_requirements'] = mandated_requirements
                        job_description1.append( 'mandated_requirements:' + mandated_requirements )
                    if 'Registration/Certification Requirements: |' in email_body:
                        certification = email_body.split( 'Registration/Certification Requirements: |', 1 )[1].strip().split(
                            'State License(s): |', 1 )[0].strip( '*' )
                        result_data_item['certification'] = certification
                        job_description1.append( 'certification:' + certification )
                    if 'State License(s): | ' in email_body:
                        slicense = email_body.split( 'State License(s): | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data_item['license'] = slicense
                        job_description1.append( 'license:' + slicense )
                    if 'Unit Information: |' in email_body:
                        unit_info = email_body.split( 'Unit Information: |', 1 )[1].strip().split( 'Job Description: | | **_', 1 )[
                            0].strip()
                        result_data_item['unit_info'] = unit_info
                        job_description1.append( 'unit_info:' + unit_info )
                    if 'Job Description: | | **_' in email_body:
                        jobdescription = email_body.split( 'Job Description: | | **_', 1 )[1].strip().split( 'Submission Requirements: |',
                                                                                            1 )[0].strip()
                        result_data_item['jobdescription'] = jobdescription
                        job_description1.append( 'jobdescription:' + jobdescription )
                    if 'Submission Requirements: |' in email_body:
                        sub_requirements = email_body.split( 'Submission Requirements: |', 1 )[1].strip().split( 'Please **', 1 )[0].strip()
                        result_data_item['sub_requirements'] = sub_requirements
                        job_description1.append( 'sub_requirements:' + sub_requirements )
                    job_id = str( uuid.uuid4() )
                    date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
                    # result_data_item['job_description'] = ' '.join( job_description1 )
                    result_data_item = {
                        'client_jobid': client_jobid,
                        'job_title': job_title,
                        'no_of_positions': no_of_positions,
                        'job_start_date': job_start_date,
                        'job_status': job_status,
                        'job_description': " ".join(job_description1),
                        'job_id': job_id,
                        'date_received': date_received,

                    }
        extracted_list.append( result_data_item )
        result = self.update_result_data( extracted_list )
        # print( extracted_list )
        return result

    def ahsa_extract_information(self):
        global email_subject, result
        labels = "trio_ahsa"
        client_data = self.extract_client_details()
        unread_emails = extracting_mail( client_data[2], client_data[3], labels )
        extracted_list = []
        job_description = []
        result_data = {}
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject = email_data['subject']
            lines = email_body.split( '\n' )
            if 'Job Listing' in email_subject:
                for line in lines:
                    client_jobid = None
                    if "Job Number: | [" in email_body:
                        client_jobid = email_body.split( 'Job Number: | [' )[1].strip().split( ']', 1 )[0].strip()
                        result_data['client_jobid'] = client_jobid
                    job_title = None
                    if 'Job Title: | ' in email_body:
                        job_title = email_body.split( 'Job Title: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['job_title'] = job_title

                    if "Modified | " in email_body:
                        modified = email_body.split( 'Modified | ' )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['modified'] = modified
                        job_description.append( 'modified:' + modified )
                    if "Unit: | " in email_body:
                        unit = email_body.split( 'Unit: | ' )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['unit'] = unit
                        job_description.append( 'unit:' + unit )
                    if "Rate Information: |" in email_body:
                        rate_info = email_body.split( 'Rate Information: |' )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['unit'] = rate_info
                        job_description.append( 'modified:' + rate_info )
                    job_start_date = None
                    if "Start Date: | " in email_body:
                        job_start_date_str = email_body.split( 'Start Date: | ' )[1].strip().split( '\n', 1 )[0].strip()
                        job_start_date_obj = datetime.strptime( job_start_date_str, '%m/%d/%Y' )
                        job_start_date = job_start_date_obj.strftime( '%Y-%m-%d' )
                        result_data['job_start_date']=job_start_date

                    if "Duration: | " in email_body:
                        duration = email_body.split( 'Duration: | ' )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['duration'] = duration
                        job_description.append( 'duration:' + duration )
                    no_of_positions = None
                    if "# of Open Positions: | " in email_body:
                        no_of_positions = email_body.split( '# of Open Positions: | ' )[1].strip().split( '\n', 1 )[
                            0].strip()
                        result_data['no_of_positions'] = no_of_positions
                    if '**Position Status:** Open' in email_body:
                        job_status = 'open'

                    else:
                        job_status = None

                    if "Shift: | Days:" in email_body:
                        shift_days = email_body.split( 'Shift: | Days:', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['shift_days'] = shift_days
                        job_description.append( 'shift_days:' + shift_days )
                    if "Shift Notes: | " in email_body:
                        shift_notes = email_body.split( 'Shift Notes: | ', 1 )[1].strip().split( '\nOn', 1 )[0].strip()
                        result_data['shift_notes'] = shift_notes
                        job_description.append( 'shift_notes:' + shift_notes )
                    if "On Call Requirements: | " in email_body:
                        on_call_requirements = \
                        email_body.split( 'On Call Requirements: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['on_call_requirements'] = on_call_requirements
                        job_description.append( 'on_call_requirements:' + on_call_requirements )
                    if "Weekend Requirements: | " in email_body:
                        weekend_requirements = \
                        email_body.split( 'Weekend Requirements: | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['weekend_requirements'] = weekend_requirements
                        job_description.append( 'weekend_requirements:' + weekend_requirements )
                    if "MANDATED Job Requirements: |" in email_body:
                        mandated_requirements = email_body.split( 'MANDATED Job Requirements: |', 1 )[1].strip( '*' ).split(
                            'Registration/Certification Requirements: |', 1 )[0].strip( '\n' )
                        result_data['mandated_requirements'] = mandated_requirements
                        job_description.append( 'mandated_requirements:' + mandated_requirements )
                    if 'Registration/Certification Requirements: |' in email_body:
                        certification = \
                        email_body.split( 'Registration/Certification Requirements: |', 1 )[1].strip().split(
                            'State License(s): |', 1 )[0].strip( '*' )
                        result_data['certification'] = certification
                        job_description.append( 'certification:' + certification )
                    if 'State License(s): | ' in email_body:
                        slicense = email_body.split( 'State License(s): | ', 1 )[1].strip().split( '\n', 1 )[0].strip()
                        result_data['license'] = slicense
                        job_description.append( 'license:' + slicense )
                    if 'Unit Information: |' in email_body:
                        unit_info = \
                        email_body.split( 'Unit Information: |', 1 )[1].strip().split( 'Job Description: | | **_', 1 )[
                            0].strip()
                        result_data['unit_info'] = unit_info
                        job_description.append( 'unit_info:' + unit_info )
                    if 'Job Description: | | **_' in email_body:
                        jobdescription = \
                        email_body.split( 'Job Description: | | **_', 1 )[1].strip().split( 'Submission Requirements: |',
                                                                                            1 )[0].strip()
                        result_data['jobdescription'] = jobdescription
                        job_description.append( 'jobdescription:' + jobdescription )
                    if 'Submission Requirements: |' in email_body:
                        sub_requirements = \
                        email_body.split( 'Submission Requirements: |', 1 )[1].strip().split( 'Please **', 1 )[0].strip()
                        result_data['sub_requirements'] = sub_requirements
                        job_description.append( 'sub_requirements:' + sub_requirements )
                    job_id = str( uuid.uuid4() )
                    date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )
                    result_data['job_description'] = ' '.join( job_description )
                    result_data = {
                        'client_jobid': client_jobid,
                        'job_title': job_title,
                        'no_of_positions': no_of_positions,
                        'job_start_date': job_start_date,
                        'job_status': job_status,
                        'job_description': ' '.join( job_description ),  # Convert the list to a string
                        'job_id': job_id,
                        'date_received': date_received,
                        'client_id': client_data[0],
                        'client': client_data[3],
                    }

        extracted_list.append( result_data )
        print( extracted_list )
        if 'Job Listing' in email_subject:
            result = self.insert_data_into_mysql( extracted_list )
        if 'Job Listing' in email_subject:
            result = self.extract_update_logic( unread_emails )

        return result


results = ahsa_client()
results.ahsa_extract_information()
