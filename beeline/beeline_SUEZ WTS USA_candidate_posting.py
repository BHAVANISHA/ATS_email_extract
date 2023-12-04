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


class beeline_client3_update:

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

    def beeline3_extract_information(self):
        # global email_subject, result
        labels = "beeline"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )
        comments = []
        extracted_list = []
        formatted_result = {}
        for entry in unread_emails:

            lines = entry['body'].split( '\n' )

            for line in lines:

                if 'Candidate Name:' in line:
                    formatted_result['first_name'] = line.split( 'Candidate Name:' )[1].strip( "*" ).strip( " " )

                if 'Request Number:' in line:
                    job_number = line.split( 'Request Number:' )[1].strip( "**" )
                    formatted_result['client_jobid'] = job_number.split( '-' )[0].strip( " " )
                elif 'Title' in line:
                    job_title = line.split( 'Title' )[1].strip( ':**' )
                    comments.append( 'Job Title:' + job_title )
                elif 'Proposed Interview Times' in line:
                    Proposed_Interview_Times = line.split( 'Proposed Interview Times' )[1].strip( ":**" )
                    comments.append( 'Proposed Interview Times:' + Proposed_Interview_Times )
                elif 'Contact Name' in line:
                    Contact_Name = line.split( 'Contact Name' )[1].strip( ":**" )
                    comments.append( "Contact Name:" + Contact_Name )
                elif '**Location:**' in line:
                    location = line.split( '**Location:**', 1 )[1].strip()
                    if location:
                        comments.append( "Location:" + location )
                    else:
                        comments.append( 'Location:' + 'none' )
                elif '**Interview Comments:**' in line:
                    interview_comments = line.split( '**Interview Comments:**', 1 )[1].strip()

                    if interview_comments:
                        comments.append( "interview_comments:" + interview_comments )
                    else:
                        comments.append( 'interview_comments:' + 'none' )
                elif '**Date Submitted:**'in line:
                    date_submitted = line.split( '**Date Submitted:**' )[1].strip( ":**" )
                    comments.append( "date_submitted:" + date_submitted )
                elif '**Proposed Interview Times:**' in line:
                    proposed_interview_times = line.split( '**Proposed Interview Times:**' )[1].strip( ":**" )
                    comments.append( "proposed_interview_times:" + proposed_interview_times )
                elif '**Submitted By:**' in line:
                    submitted_by = line.split( '**Submitted By:**' )[1].strip( ":**" )
                    comments.append( "submitted_by:" + submitted_by )
                elif '**Selected Interview Time:**' in line:
                    interview_time = line.split( '**Selected Interview Time:**', 1 )[1].strip()

                    if interview_time:
                        comments.append( "interview_time:" + interview_time )
                    else:
                        comments.append( 'interview_time:' + 'none' )

            for email in unread_emails:
                body = email['body']
                start_index = body.find( '*Candidate Interview Type:*' ) + len( '*Candidate Interview Type:*' )
                end_index = body.find( ')', start_index ) + 1
                interview_type = body[start_index:end_index].strip( '*' )
                comments.append( "interview_type:" + interview_type )
                if "**Modification Details" in body:
                    modification_details = body.split("**Modification Details:",1)[1].split("---",1)[0].strip("**").strip(" ")
                    comments.append( "modification_details:" + modification_details )

        formatted_result['comments'] = " ".join( comments )
        print( formatted_result )
        extracted_list.append( formatted_result )

        self.get_job_id_by_client_job_id( extracted_list )
        # print( extracted_list )
        return extracted_list


results = beeline_client3_update()
results.beeline3_extract_information()
