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


class field_glass_baxter:

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
                time_sheet_id = result.get( "time_sheet_id", None )

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
                                            week_end=%s,
                                            time_sheet_id=%s                                            
                                            WHERE placement_id = %s """
                    data_to_update = (comments, week_start, week_end, time_sheet_id, placement_id)
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

    def field_glass_baxter_extract_information(self):
        global extracted_list
        labels = "fieldglass_baxter"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )

        result_data_item = {}
        extracted_list = []
        comments = []
        for email_data in unread_emails:
            email_body = email_data['body']
            email_subject=email_data['subject']
            if 'Time Sheet completely approved' in email_subject:
                if '### Worker Name' in email_body:
                    candidate_name = \
                    email_body.split( '### Worker Name', 1 )[1].split( '### Time Sheet Total Quantity', 1 )[0].strip()
                    result_data_item['candidate_name'] = candidate_name
                    comments.append( 'candidate_name:' + candidate_name )
                if '### Time Sheet ID' in email_body:
                    time_sheet_id = email_body.split( '### Time Sheet ID', 1 )[1].split( '### Time Sheet Start Date', 1 )[
                        0].strip()
                    result_data_item['time_sheet_id'] = time_sheet_id
                    comments.append( 'time_sheet_id:' + time_sheet_id )

                if '### Time Sheet Start Date' in email_body:
                    start_date = \
                    email_body.split( '### Time Sheet Start Date', 1 )[1].split( '### Time Sheet End Date', 1 )[0].strip()
                    start_date = start_date.strip()
                    week_start_date = datetime.strptime( start_date, '%Y-%m-%d' )
                    week_date = datetime.strftime( week_start_date, '%Y-%m-%d' )
                    result_data_item['week_start'] = week_date
                if '### Time Sheet End Date' in email_body:
                    end_date = \
                    email_body.split( '### Time Sheet End Date', 1 )[1].split( '### Time Sheet Billable Hours', 1 )[
                        0].strip()
                    end_date = end_date.strip()
                    week_end_date = datetime.strptime( end_date, '%Y-%m-%d' )
                    week_date = datetime.strftime( week_end_date, '%Y-%m-%d' )
                    result_data_item['week_end'] = week_date
                if '### Main Document ID' in email_body:
                    document_id = email_body.split( '### Main Document ID', 1 )[1].split( '### Site', 1 )[0].strip()
                    result_data_item['document_id'] = document_id
                    comments.append( 'document_id:' + document_id )

                if '### Worker ID' in email_body:
                    worker_id = email_body.split( '### Worker ID', 1 )[1].split( '### Worker Name', 1 )[0].strip()
                    result_data_item['worker_id'] = worker_id
                    comments.append( 'worker_id:' + worker_id )

                if '### Time Sheet Billable Hours' in email_body:
                    billable_hours = \
                    email_body.split( '### Time Sheet Billable Hours', 1 )[1].split( '### Main Document ID', 1 )[0].strip()
                    result_data_item['billable_hours'] = billable_hours
                    comments.append( 'billable_hours:' + billable_hours )
                if '### Site' in email_body:
                    site = email_body.split( '### Site', 1 )[1].split( '### Worker ID', 1 )[0].strip()
                    result_data_item['site'] = site
                    comments.append( 'site:' + site )
                if '### Time Sheet Total Quantity' in email_body:
                    total_quantity = email_body.split( '### Time Sheet Total Quantity', 1 )[1].replace('\n---','').replace(' ','').split( '![Details]',1 )[0].replace('\n','')
                    result_data_item['total_quantity'] = total_quantity
                    comments.append( 'total_quantity:' + total_quantity )

            result_data_item['comments'] = ' '.join( comments )
            extracted_list.append( result_data_item )
            self.get_job_id_by_client_job_id( extracted_list )
            print( extracted_list )
            return extracted_list


results = field_glass_baxter()
results.field_glass_baxter_extract_information()
