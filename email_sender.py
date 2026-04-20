import smtplib
import logging
import requests
import time
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BulkEmailSender:
    def __init__(self, db):
        self.db = db
        self.smtp_server = ''
        self.smtp_port = 465
        self.smtp_user = ''
        self.smtp_password = ''
        self.warmup_week = 1
        self.warmup_enabled = True
        self.daily_limit = 50
        self.sent_today = 0
        self.last_reset = datetime.now()
        self.load_settings()
    
    def load_settings(self):
        settings = self.db.get_settings()
        self.smtp_server = settings.get('smtp_server', '')
        self.smtp_port = int(settings.get('smtp_port', 465))
        self.smtp_user = settings.get('smtp_user', '')
        self.smtp_password = settings.get('smtp_password', '')
        self.warmup_enabled = settings.get('warmup_enabled', 'true') == 'true'
        self.warmup_week = int(settings.get('warmup_week', 1))
        
        # Warmup schedule
        schedule = [50, 100, 200, 300, 500, 700, 1000]
        week_index = min(self.warmup_week - 1, len(schedule) - 1)
        self.daily_limit = schedule[week_index] if self.warmup_enabled else int(settings.get('daily_email_limit', 1000))
    
    def get_warmup_schedule(self):
        return [
            {'week': 1, 'limit': 50, 'description': 'Week 1 - Initial warmup'},
            {'week': 2, 'limit': 100, 'description': 'Week 2 - Gradual increase'},
            {'week': 3, 'limit': 200, 'description': 'Week 3 - Building reputation'},
            {'week': 4, 'limit': 300, 'description': 'Week 4 - Increased volume'},
            {'week': 5, 'limit': 500, 'description': 'Week 5 - High volume'},
            {'week': 6, 'limit': 700, 'description': 'Week 6 - Maximum warmup'},
            {'week': 7, 'limit': 1000, 'description': 'Week 7 - Full capacity'}
        ]
    
    def check_rate_limit(self):
        now = datetime.now()
        if now.date() > self.last_reset.date():
            self.sent_today = 0
            self.last_reset = now
        
        if self.sent_today >= self.daily_limit:
            tomorrow = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            wait_seconds = (tomorrow - now).total_seconds()
            logger.warning(f"Daily limit ({self.daily_limit}) reached. Waiting {wait_seconds/3600:.1f} hours")
            time.sleep(wait_seconds)
            self.sent_today = 0
            self.last_reset = datetime.now()
    
    def render_template(self, template_name, client, btc_rate=None):
        template_path = os.path.join('templates/email_templates', template_name)
        
        if not os.path.exists(template_path):
            logger.error(f"Template not found: {template_path}")
            return f"<html><body><h1>Template {template_name} not found</h1></body></html>"
        
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()
        
        settings = self.db.get_settings()
        reply_email = settings.get('reply_email', 'auszahlung@as-blockchain.com')
        
        data = {
            'first_name': client.get('first_name', ''),
            'last_name': client.get('last_name', ''),
            'btc_sum': client.get('btc_sum', '0'),
            'btc_fee': client.get('btc_fee', '0'),
            'btc_address': client.get('btc_address', ''),
            'btc_url': client.get('btc_url', '#'),
            'qr_code': client.get('qr_code', ''),
            'iban': client.get('iban', ''),
            'wallet_address': client.get('wallet_address', ''),
            'tx_id': client.get('tx_id', ''),
            'tx_href': client.get('tx_href', '#'),
            'btc_eur_rate': btc_rate if btc_rate else 'N/A',
            'reply_email': reply_email,
            'date': datetime.now().strftime('%d.%m.%Y')
        }
        
        for key, value in data.items():
            html = html.replace(f'{{{{{key}}}}}', str(value) if value else '')
        
        return html
    
    def send_email(self, to_email, subject, html_content):
        try:
            self.check_rate_limit()
            
            if not self.smtp_server or not self.smtp_user:
                raise Exception("SMTP not configured. Please configure SMTP settings.")
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = to_email
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            if self.smtp_port == 465:
                with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=30) as server:
                    server.login(self.smtp_user, self.smtp_password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30) as server:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                    server.send_message(msg)
            
            self.sent_today += 1
            logger.info(f"Email sent to {to_email}")
            return True, None
            
        except Exception as e:
            logger.error(f"Failed to send to {to_email}: {e}")
            return False, str(e)
    
    def get_btc_rate(self):
        try:
            response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur', timeout=10)
            data = response.json()
            return data['bitcoin']['eur']
        except Exception as e:
            logger.error(f"Failed to get BTC rate: {e}")
            return None
    
    def process_queue(self):
        try:
            self.load_settings()
            
            pending_emails = self.db.get_pending_emails(limit=10)
            
            if not pending_emails:
                return
            
            logger.info(f"Processing {len(pending_emails)} pending emails")
            
            for email in pending_emails:
                self.db.update_queue_status(email['id'], 'sending')
                
                client = self.db.get_client_by_id(email['client_id'])
                if not client:
                    self.db.update_queue_status(email['id'], 'failed', 'Client not found')
                    continue
                
                btc_rate = None
                if 'Ruckerstattung' in email['template_name'] or 'Auszahlung' in email['template_name']:
                    btc_rate = self.get_btc_rate()
                
                html_content = self.render_template(email['template_name'], client, btc_rate)
                
                success, error = self.send_email(
                    email['recipient_email'],
                    email['subject'],
                    html_content
                )
                
                if success:
                    self.db.update_queue_status(email['id'], 'sent')
                    self.db.log_email(
                        email['client_id'],
                        email['recipient_email'],
                        email['template_name'].replace('.html', ''),
                        'delivered'
                    )
                    
                    if email['template_name'] == '1-Ruckerstattung.html':
                        self.db.update_client_status(email['client_id'], 'email_sent', 1)
                        self.db.update_client_status(email['client_id'], 'email_sent_date', datetime.now())
                        self.db.update_client_status(email['client_id'], 'status', 'email_sent')
                else:
                    self.db.update_queue_status(email['id'], 'failed', error)
                    self.db.log_email(
                        email['client_id'],
                        email['recipient_email'],
                        email['template_name'].replace('.html', ''),
                        'failed',
                        error
                    )
                
                time.sleep(2)
                
        except Exception as e:
            logger.error(f"Error processing queue: {e}")