
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


class onboard_client1_update:

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

    def onboard_extract_information(self):
        global extracted_list, extracted_data, comments
        labels = "vndly_masonite"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )

        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject=email_data['subject']
            extracted_list = []
            extracted_data={}
            lines = email_body.split( '\n' )
            comments = []
            for line in lines:
                if "Onboarding Notification: " in email_subject:
                    client_jobid = email_subject.split( 'Onboarding Notification: ' )[1].strip().split( '-', 1 )[0].strip()
                    extracted_data['client_jobid'] = client_jobid
                    # comments.append( 'client_jobid:' + client_jobid )
                if 'Hi,' in line:

                    first_name = email_body.split( 'Hi,',1 )[1].split( 'has been onboarded' )[0].strip( '*' ).strip().replace( '****', '' ).replace( '\n', '' ).strip()
                    extracted_data['first_name'] = first_name
                    # comments.append( 'first_name:' + first_name )

                if 'Job Title - **' in line:
                    job_title=line.split('Job Title - **',1)[1].replace('\n','').strip('*').split('Job Location',1)[0].strip()
                    extracted_data['job_title']=job_title
                    comments.append('job_title:'+job_title)
                if 'Job Location - **' in line:
                    job_location=line.split('Job Location - **',1)[1].strip('|').replace('\n','').strip('*').split('Job Start Date',1)[0].strip('*')
                    extracted_data['job_location']=job_location
                    comments.append('job_location:'+job_location)
                if 'Job Start Date - **' in line:
                    job_start_date=line.split('Job Start Date - **',1)[1].replace('\n','').strip('*').split('Please log in to [Workday VNDLY]',1)[0].strip()
                    extracted_data['job_start_date']=job_start_date
                    comments.append("job_start_date:"+job_start_date)

            extracted_data['comments']= " ".join( comments )
            extracted_list.append( extracted_data )
            self.get_job_id_by_client_job_id( extracted_list )
            print( extracted_list )
            return extracted_list


results = onboard_client1_update()
results.onboard_extract_information()
