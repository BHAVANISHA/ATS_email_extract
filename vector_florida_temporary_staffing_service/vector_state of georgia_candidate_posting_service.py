
import email
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


class vector_florida_update:

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
                    job_id = job_id[0]
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
                                                    '$.ready_to_onboard',
                                                    true
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

    def vector_extract_information(self):
        global extracted_list
        labels = "vector_florida_update"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )

        result_data_item = {}
        extracted_list = []
        comments=[]
        for email_data in unread_emails:
            email_subject = email_data['subject']
            date_received = email_data['date_received'].strftime( "%Y-%m-%d %H:%M:%S" )


            subject_parts = email_data['subject'].split( 'Req. ' )
            if len( subject_parts ) > 1:
                requisition_id = subject_parts[1].split( ' Now Open' )[0]
            else:
                requisition_id = None


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
            comments.append( "job_title:" + title )
            result_data_item['comments'] = " ".join( comments )
            result_data_item['first_name']=client
            result_data_item['client_jobid'] = requisition_id
            extracted_list.append(result_data_item)
            self.get_job_id_by_client_job_id( extracted_list )
            print( extracted_list )
            return extracted_list


results = vector_florida_update()
results.vector_extract_information()
