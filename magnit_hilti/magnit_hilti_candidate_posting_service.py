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
    "database": "dump_email_extract",
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


class magnit_hilti_interviewing:

    def get_job_id_by_client_job_id(self, extracted_list):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
            )

            cursor = conn.cursor()

            for result in extracted_list:
                client_jobid = result.get( 'client_jobid', None )
                first_name = result.get( "first_name", None )
                comments = result.get( 'comments', None )

                cursor.execute( "SELECT job_id FROM jobpostings WHERE client_jobid = %s", (client_jobid,) )
                job_id = cursor.fetchone()

                if job_id:
                    job_id = job_id[0]  # Access the first element of the tuple
                    result["job_id"] = job_id
                    print( "job_id:", job_id )

                cursor.execute( "SELECT applicants_id FROM applicants WHERE first_name = %s", (first_name,) )
                applicants_id = cursor.fetchone()

                if applicants_id:
                    applicants_id = applicants_id[0]  # Access the first element of the tuple
                    result["applicant_id"] = applicants_id
                    print( "applicants_id:", applicants_id )

                cursor.execute( "SELECT * FROM applicant_progress_status WHERE job_id = %s AND applicants_id = %s",
                                (job_id, applicants_id) )
                existing_record = cursor.fetchone()

                if existing_record:
                    update_query = """
                                            UPDATE applicant_progress_status
                                            SET comments = %s,
                                                candidates_status = JSON_SET(
                                                    candidates_status,
                                                    '$.interviewing',
                                                    CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(candidates_status, '$.rejected')) = 'false' THEN 'true' ELSE 'false' END
                                                )
                                            WHERE job_id = %s AND applicants_id = %s;
                                        """
                    data_to_update = (comments, job_id, applicants_id)
                    cursor.execute( update_query, data_to_update )
                    conn.commit()
                    result["comments"] = comments
                    print( f"Updated comments for job_id={job_id}" )
                else:
                    print( f"Job_id {job_id} not found in the database." )

        except mysql.connector.Error as error:
            print( f"Error getting job_id from table1: {error}" )
        finally:
            cursor.close()
            conn.close()

    def magnit_hilti_extract_information(self):
        # global email_subject, result
        labels = "magnit_hilti"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )
        result_data_item = {}
        extracted_list = []
        comments = []
        for email_data in unread_emails:
            email_body = email_data['body']

            if 'Date:' in email_body:
                meet_date = \
                    email_body.split( 'Date:', 1 )[1].strip( ' \n' ).split( 'Time: ',
                                                                                                      1 )[
                        0].replace('\n','').strip()
                comments.append( 'meet_date:' + meet_date )

            if 'Location:' in email_body:
                location = \
                    email_body.split( 'Location:', 1 )[1].split( 'Type:', 1 )[
                        0].strip()
                comments.append('location:'+location)
            if 'Technician - 'in email_data['subject']:
                first_name=email_data['subject'].split('Technician - ',1)[1].strip().split(')',1)[0].strip()
                result_data_item['first_name']=first_name
            if 'Type:' in email_body:
                interview_type = email_body.split( 'Type:', 1 )[1].strip().split(
                    'Interviewers:', 1 )[
                    0].strip()

                comments.append('interview_type:'+interview_type)

            if 'Time: ' in email_body:
                meet_time = email_body.split( 'Time: ', 1 )[1].split(
                    'Duration:', 1 )[
                    0].strip()
                comments.append('meet_time:'+meet_time)

            if 'Duration:' in email_body:
                meet_duration = \
                    email_body.split( 'Duration:', 1 )[1].strip( ' \n' ).split(
                        'Mode:',
                        1 )[0].strip()
                comments.append( 'meet_duration:' + meet_duration )
            if 'Interviewers:' in email_body:
                interviewers = email_body.split( 'Interviewers:', 1 )[1].strip( ' \n' ).split(
                    '\n', 1 )[0].strip()
                comments.append( 'interviewers:' + interviewers )
            if 'Mode:' in email_body:
                interview_mode = email_body.split( 'Mode:', 1 )[1].strip( ' \n' ).split( 'Location: ', 1 )[0].strip()
                comments.append( 'interview_mode:' + interview_mode )
            if '### Object Name' in email_body:
                first_name = email_body.split( '### Object Name', 1 )[1].strip( ' \n' ).split( '- USA ', 1 )[
                    0].strip()
                result_data_item['first_name'] = first_name.rstrip( ' \n' ).strip()

            if '#24216779 for the' in email_body:
                job_title = email_body.split( '#24216779 for the', 1 )[1].strip().split( '.', 1 )[0].strip()
                result_data_item['job_title'] = job_title

            if ' Req #' in email_data['subject']:
                client_jobid = email_data['subject'].split( 'Req #', 1 )[1].strip().split( '(', 1 )[
                    0].strip()
                result_data_item['client_jobid'] = client_jobid

            if 'Notes: ' in email_body:
                notes = email_body.split( 'Notes: ', 1 )[1].strip( ' \n' ).split( 'Regards,', 1 )[
                    0].strip()
                comments.append('notes:'+notes)

            result_data_item['comments'] = ' '.join( comments )
            extracted_list.append( result_data_item )
            print(extracted_list)
            self.get_job_id_by_client_job_id( extracted_list )

            return extracted_list


results = magnit_hilti_interviewing()
results.magnit_hilti_extract_information()
