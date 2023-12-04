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


class simplify_stewart_update:

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
                ot_hours = result.get( "ot_hours", None )
                total_bill_amt = result.get( "total_bill_amt", None )

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
                                            ot_hours=%s,
                                            total_bill_amt=%s                                             
                                            WHERE placement_id = %s """
                    data_to_update = (comments, week_start, week_end, ot_hours, total_bill_amt, placement_id)
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

    def simplify_extract_information(self):
        global extracted_list
        labels = "simplify_steward"
        unread_emails = extracting_mail( 'bhavanisha@vrdella.com', 'dexq ylbk akqq dwyt', labels )

        result_data_item = {}
        extracted_list = []
        comments = []
        for email_data in unread_emails:
            email_subject = email_data['subject']
            for entry in unread_emails:

                lines = entry['body'].split( '\n' )

                for line in lines:
                    if 'Timesheet Approved' in email_subject:
                        if 'Contractor' in line:
                            candidate_name = line.split( 'Contractor' )[1].strip( ":" ).strip( " " )
                            result_data_item['candidate_name'] = candidate_name
                            comments.append( 'candidate_name:' + candidate_name )
                        if 'Job ID#:' in line:
                            job_id = line.split( 'Job ID#:' )[1].strip( " " )

                            comments.append( 'job_id:' + job_id )

                        if 'Location:' in line:
                            Location = line.split( 'Location:' )[1].strip( " " )

                            comments.append( 'Location:' + Location )
                        if 'Job Name:' in line:
                            job_title = line.split( 'Job Name:' )[1].strip( " " )

                            comments.append( 'job_title:' + job_title )

                        if 'Approved By:' in line:
                            Approved_By = line.split( 'Approved By:' )[1].strip( " " )

                            comments.append( 'Approved_By:' + Approved_By )

                        if 'Timesheet Dates:' in line:
                            start_date = line.split( 'Timesheet Dates:' )[1].strip( " " ).split( '-', 1 )[0]
                            start_date = start_date.strip()
                            week_start_date = datetime.strptime( start_date, '%m/%d/%Y' )
                            week_date = datetime.strftime( week_start_date, '%Y-%m-%d' )
                            result_data_item['week_start'] = week_date

                        if 'Timesheet Dates:' in line:
                            end_date = line.split( '2023 - ' )[1].strip( " " ).split( ' ', 1 )[0]
                            end_date = end_date.strip()
                            week_end_date = datetime.strptime( end_date, '%m/%d/%Y' )
                            week_date = datetime.strftime( week_end_date, '%Y-%m-%d' )
                            result_data_item['week_end'] = week_date
                        if 'Regular Time Hours:' in line:
                            regular_time_hours = line.split( 'Regular Time Hours:' )[1].strip( " " ).replace(',','')
                            comments.append( 'regular_time_hours:' + regular_time_hours )
                        if 'Over Time Hours:' in line:
                            ot_hours = line.split( 'Over Time Hours:' )[1].strip( " " ).replace(',','')
                            result_data_item['ot_hours'] = ot_hours
                        if 'Regular Bill Rate:' in line:
                            regular_bill_pay = line.split( 'Regular Bill Rate:' )[1].strip( " " ).strip( '$' ).replace(',','')
                            comments.append( 'regular_bill_pay:' + regular_bill_pay )
                        if 'Over Time Bill Rate:' in line:
                            over_time_bill_rate = line.split( 'Over Time Bill Rate:' )[1].strip( " " ).strip( '$' ).replace(',','')
                            comments.append( 'over_time_bill_rate:' + over_time_bill_rate )
                        if 'Total Bill Rate:' in line:
                            total_bill_amt = \
                                line.split( 'Total Bill Rate:' )[1].strip( " " ).strip( '$' ).split( 'Thank you', 1 )[
                                    0].strip(
                                    ' ' ).replace(',','')
                            result_data_item['total_bill_amt'] = total_bill_amt
                        result_data_item['comments'] = ' '.join( comments )

            extracted_list.append( result_data_item )
            self.get_job_id_by_client_job_id( extracted_list )
            print( extracted_list )
            return extracted_list


results = simplify_stewart_update()
results.simplify_extract_information()
