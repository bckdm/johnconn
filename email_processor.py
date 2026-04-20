import imaplib
import email
import email.header
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

class EmailProcessor:
    def __init__(self, db):
        self.db = db
        self.imap_server = ''
        self.imap_port = 993
        self.imap_user = ''
        self.imap_password = ''
        self.smtp_server = ''
        self.smtp_port = 465
        self.smtp_user = ''
        self.smtp_password = ''
        self.load_settings()
    
    def load_settings(self):
        settings = self.db.get_settings()
        self.imap_server = settings.get('imap_server', '')
        self.imap_port = int(settings.get('imap_port', 993))
        self.imap_user = settings.get('imap_user', '')
        self.imap_password = settings.get('imap_password', '')
        self.smtp_server = settings.get('auto_smtp_server', settings.get('smtp_server', ''))
        self.smtp_port = int(settings.get('auto_smtp_port', settings.get('smtp_port', 465)))
        self.smtp_user = settings.get('auto_smtp_user', settings.get('smtp_user', ''))
        self.smtp_password = settings.get('auto_smtp_password', settings.get('smtp_password', ''))
    
    def send_reply(self, to_email, subject, template_name, client):
        from email_sender import BulkEmailSender
        sender = BulkEmailSender(self.db)
        sender.smtp_server = self.smtp_server
        sender.smtp_port = self.smtp_port
        sender.smtp_user = self.smtp_user
        sender.smtp_password = self.smtp_password
        
        btc_rate = sender.get_btc_rate() if 'Auszahlung' in template_name else None
        html = sender.render_template(template_name, client, btc_rate)
        return sender.send_email(to_email, subject, html)
    
    def decode_subject(self, subject):
        if not subject:
            return ''
        decoded_parts = []
        for part, encoding in email.header.decode_header(subject):
            if isinstance(part, bytes):
                try:
                    decoded_parts.append(part.decode(encoding or 'utf-8', errors='replace'))
                except:
                    decoded_parts.append(part.decode('utf-8', errors='replace'))
            else:
                decoded_parts.append(part)
        return ' '.join(decoded_parts)
    
    def get_email_body(self, msg):
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        break
                    except:
                        continue
        else:
            try:
                body = msg.get_payload(decode=True).decode('utf-8', errors='replace')
            except:
                body = str(msg.get_payload())
        return body.strip()
    
    def process_incoming_emails(self):
        if not self.imap_server or not self.imap_user:
            logger.warning("IMAP not configured. Please configure IMAP settings.")
            return
        
        try:
            mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            mail.login(self.imap_user, self.imap_password)
            mail.select('INBOX')
            
            status, messages = mail.search(None, 'UNSEEN')
            if status != 'OK' or not messages[0]:
                mail.close()
                mail.logout()
                return
            
            email_ids = messages[0].split()
            logger.info(f"Found {len(email_ids)} unread emails")
            
            for msg_id in email_ids:
                try:
                    status, data = mail.fetch(msg_id, '(RFC822)')
                    if status != 'OK':
                        continue
                    
                    msg = email.message_from_bytes(data[0][1])
                    sender = email.utils.parseaddr(msg['From'])[1]
                    subject = self.decode_subject(msg['Subject'])
                    body = self.get_email_body(msg)
                    
                    logger.info(f"Processing email from {sender}: {subject}")
                    
                    client = self.db.get_client_by_email(sender)
                    
                    if not client:
                        logger.warning(f"Client not found for {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                        continue
                    
                    # Step 2: Rückerstattung confirmation
                    if 'Rückerstattung-bestätigt' in subject or 'Ruckerstattung-bestatigt' in subject:
                        self.db.update_client_status(client['id'], 'ruckerstattung_confirmed', 1)
                        self.db.update_client_status(client['id'], 'ruckerstattung_confirmed_date', datetime.now())
                        self.db.update_client_status(client['id'], 'status', 'confirmed')
                        self.db.log_email(client['id'], sender, 'confirmation_received', 'delivered')
                        self.send_reply(sender, 'Ihre Rückerstattung - Auszahlungsinformationen', '2-Auszahlung.html', client)
                        logger.info(f"Sent 2-Auszahlung.html to {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    
                    # Step 3: Bank request
                    elif 'Bankkonto Auszahlungsantrag' in subject:
                        self.db.update_client_status(client['id'], 'auszahlungsantrag_type', 'bank')
                        self.db.update_client_status(client['id'], 'status', 'payout_pending')
                        self.db.log_email(client['id'], sender, 'bank_request', 'delivered')
                        self.send_reply(sender, 'Ihr Auszahlungsantrag wurde autorisiert', '3-Auszahlung autorisiert - Bankkonto.html', client)
                        logger.info(f"Sent bank authorization to {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    
                    # Step 3: Wallet request
                    elif 'Wallet Auszahlungsantrag' in subject:
                        self.db.update_client_status(client['id'], 'auszahlungsantrag_type', 'wallet')
                        self.db.update_client_status(client['id'], 'status', 'payout_pending')
                        self.db.log_email(client['id'], sender, 'wallet_request', 'delivered')
                        self.send_reply(sender, 'Ihr Auszahlungsantrag wurde autorisiert', '3-Auszahlung autorisiert - Wallet.html', client)
                        logger.info(f"Sent wallet authorization to {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    
                    # Step 4: Bank amount with IBAN
                    elif 'Bankkonto-Auszahlungsbetrag' in subject:
                        iban_pattern = r'[A-Z]{2}\d{2}[A-Z0-9]{11,30}'
                        match = re.search(iban_pattern, body.upper())
                        if match:
                            iban = match.group(0)
                            self.db.update_client(client['id'], {'iban': iban})
                            self.db.log_email(client['id'], sender, 'bank_payout_confirmed', 'delivered')
                            self.send_reply(sender, 'Ihre Auszahlung wird veranlasst', '4-Auszahlung per Bankuberweisung.html', client)
                            self.db.update_client_status(client['id'], 'status', 'completed')
                            logger.info(f"Bank payout confirmed for {sender}: {iban}")
                        else:
                            logger.warning(f"No IBAN found in email from {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    
                    # Step 4: Wallet amount with address
                    elif 'Wallet-Auszahlungsbetrag' in subject:
                        wallet_pattern = r'[13][a-km-zA-HJ-NP-Z1-9]{25,34}'
                        match = re.search(wallet_pattern, body)
                        if match:
                            wallet = match.group(0)
                            self.db.update_client(client['id'], {'wallet_address': wallet})
                            self.db.log_email(client['id'], sender, 'wallet_payout_confirmed', 'delivered')
                            self.send_reply(sender, 'Ihr Freigabecode für die Auszahlung', '4-Freigabecode.html', client)
                            self.db.update_client_status(client['id'], 'status', 'completed')
                            logger.info(f"Wallet payout confirmed for {sender}: {wallet}")
                        else:
                            logger.warning(f"No wallet address found in email from {sender}")
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    
                    else:
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                        
                except Exception as e:
                    logger.error(f"Error processing email {msg_id}: {e}")
                    try:
                        mail.store(msg_id, '+FLAGS', '\\Seen')
                    except:
                        pass
            
            mail.close()
            mail.logout()
            
        except Exception as e:
            logger.error(f"IMAP error: {e}")