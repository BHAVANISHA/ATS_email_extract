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


class beeline_cargill:

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

                candidate_name = result.get( "candidate_name", None )
                week_start = result.get( "week_start", None )
                week_end = result.get( "week_end", None )
                comments = result.get( "comments", None )

                cursor.execute( "SELECT placement_id FROM placement_details WHERE candidate_name = %s",
                                (candidate_name,) )

                placement_id = cursor.fetchone()

                if placement_id:
                    placement_id = placement_id[0]
                    result["placement_id"] = placement_id
                    print( "placement_id:", placement_id )

                cursor.execute( "SELECT * FROM time_sheet WHERE placement_id = %s ", (placement_id,) )
                existing_record = cursor.fetchone()

                if existing_record:
                    update_query = """
                                            UPDATE time_sheet
                                            SET comments = %s,
                                            week_start=%s,
                                            week_end=%s

                                            WHERE placement_id = %s """
                    data_to_update = (comments, week_start, week_end, placement_id)
                    cursor.execute( update_query, data_to_update )
                    conn.commit()
                    result["comments"] = comments
                    print( f"Updated comments for placement_id={placement_id}" )
                else:
                    print( f"placement_id {placement_id} not found in the database." )

        except mysql.connector.Error as error:
            print( f"Error getting placement_id from table1: {error}" )
        finally:
            cursor.close()
            conn.close()

    def beeline_cargill_extract_information(self):
        global extracted_list, week_day
        labels = "beeline_cargil"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )

        result_data_item = {}
        extracted_list = []
        comments = []
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject=email_data['subject']
            lines = email_body.split( '\n' )
            if ' Timesheet Approval' in email_subject:
                if 'Timesheet for' in email_body:
                    candidate_name = email_body.split( 'Timesheet for', 1 )[1].strip( ' \n' ).split( 'was set to', 1 )[
                        0].strip()
                    result_data_item['candidate_name'] = candidate_name.rstrip( '- ' ).strip()
                    comments.append( 'candidate_name:' + candidate_name )

                if 'APPROVED by' in email_body:
                    approved_by = email_body.split( 'APPROVED by', 1 )[1].strip().split( '\n', 1 )[0]
                    result_data_item['approved_by'] = approved_by
                    comments.append( 'approved_by:' + approved_by )

                for line in lines:
                    if line.startswith( "Time Period:" ):

                        week_start = line.split( 'Time Period:', 1 )[1].strip().split( ' - ', 1 )[0].strip( " " )
                        if line.startswith( "Time Period:" ):
                            week_day = line.split( ",", 1 )[1].split( "\n ", 1 )[0].strip( ' ' )

                        result_data_item['week_start'] = week_start + "," + week_day

                    if line.startswith( "Time Period:" ):
                        if '-' in line:
                            week_end = line.split( '-', 1 )[1].strip().split( '\n', 1 )[0]
                            result_data_item['week_end'] = week_end

                result_data_item['comments'] = ' '.join( comments )
                extracted_list.append( result_data_item )
                self.get_job_id_by_client_job_id( extracted_list )
                print( extracted_list )
                return extracted_list


results = beeline_cargill()
results.beeline_cargill_extract_information()
