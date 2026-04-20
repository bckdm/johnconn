import sqlite3
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = sqlite3.connect('email_auto.db', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init_tables()
    
    def init_tables(self):
        cursor = self.conn.cursor()
        
        # Clients table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT UNIQUE,
                phone TEXT,
                btc_sum REAL,
                btc_fee REAL,
                btc_address TEXT,
                btc_url TEXT,
                qr_code TEXT,
                iban TEXT,
                wallet_address TEXT,
                tx_id TEXT,
                tx_href TEXT,
                email_sent INTEGER DEFAULT 0,
                email_sent_date TIMESTAMP,
                ruckerstattung_confirmed INTEGER DEFAULT 0,
                ruckerstattung_confirmed_date TIMESTAMP,
                auszahlungsantrag_type TEXT,
                status TEXT DEFAULT 'pending'
            )
        ''')
        
        # Email queue table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                recipient_email TEXT,
                template_name TEXT,
                subject TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                attempts INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_at TIMESTAMP
            )
        ''')
        
        # Email logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                recipient TEXT,
                email_type TEXT,
                status TEXT,
                error_message TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Campaigns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_name TEXT,
                template_name TEXT,
                total_recipients INTEGER DEFAULT 0,
                sent_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Default settings
        default_settings = [
            ('warmup_enabled', 'true'),
            ('warmup_week', '1'),
            ('daily_email_limit', '50'),
            ('reply_email', 'auszahlung@as-blockchain.com'),
            ('smtp_server', ''),
            ('smtp_port', '465'),
            ('smtp_user', ''),
            ('smtp_password', ''),
            ('imap_server', ''),
            ('imap_port', '993'),
            ('imap_user', ''),
            ('imap_password', ''),
            ('auto_smtp_server', ''),
            ('auto_smtp_port', '465'),
            ('auto_smtp_user', ''),
            ('auto_smtp_password', '')
        ]
        
        for key, value in default_settings:
            cursor.execute("INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)", (key, value))
        
        self.conn.commit()
    
    def get_cursor(self):
        return self.conn.cursor()
    
    def commit(self):
        self.conn.commit()
    
    def upsert_client(self, data):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM clients WHERE email = ?", (data.get('email'),))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE clients SET 
                    first_name = ?, last_name = ?, phone = ?,
                    btc_sum = ?, btc_fee = ?, btc_address = ?,
                    btc_url = ?, qr_code = ?, iban = ?,
                    wallet_address = ?, tx_id = ?, tx_href = ?
                WHERE email = ?
            ''', (
                data.get('first_name'), data.get('last_name'), data.get('phone'),
                data.get('btc_sum'), data.get('btc_fee'), data.get('btc_address'),
                data.get('btc_url'), data.get('qr_code'), data.get('iban'),
                data.get('wallet_address'), data.get('tx_id'), data.get('tx_href'),
                data.get('email')
            ))
        else:
            cursor.execute('''
                INSERT INTO clients (
                    id, first_name, last_name, email, phone,
                    btc_sum, btc_fee, btc_address, btc_url, qr_code,
                    iban, wallet_address, tx_id, tx_href
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('id'), data.get('first_name'), data.get('last_name'),
                data.get('email'), data.get('phone'), data.get('btc_sum'),
                data.get('btc_fee'), data.get('btc_address'), data.get('btc_url'),
                data.get('qr_code'), data.get('iban'), data.get('wallet_address'),
                data.get('tx_id'), data.get('tx_href')
            ))
        self.conn.commit()
    
    def get_client_by_email(self, email):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM clients WHERE email = ?", (email,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_client_by_id(self, client_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_clients_paginated(self, page, per_page, search=''):
        offset = (page - 1) * per_page
        cursor = self.conn.cursor()
        if search:
            cursor.execute('''
                SELECT * FROM clients 
                WHERE email LIKE ? OR first_name LIKE ? OR last_name LIKE ?
                LIMIT ? OFFSET ?
            ''', (f'%{search}%', f'%{search}%', f'%{search}%', per_page, offset))
        else:
            cursor.execute("SELECT * FROM clients LIMIT ? OFFSET ?", (per_page, offset))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_clients_count(self, search=''):
        cursor = self.conn.cursor()
        if search:
            cursor.execute('''
                SELECT COUNT(*) as count FROM clients 
                WHERE email LIKE ? OR first_name LIKE ? OR last_name LIKE ?
            ''', (f'%{search}%', f'%{search}%', f'%{search}%'))
        else:
            cursor.execute("SELECT COUNT(*) as count FROM clients")
        row = cursor.fetchone()
        return row['count'] if row else 0
    
    def get_clients_for_campaign(self, filters=None):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM clients WHERE email_sent = 0 OR email_sent IS NULL")
        return [dict(row) for row in cursor.fetchall()]
    
    def add_to_email_queue(self, client_id, recipient_email, template_name, subject):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO email_queue (client_id, recipient_email, template_name, subject, status)
            VALUES (?, ?, ?, ?, 'pending')
        ''', (client_id, recipient_email, template_name, subject))
        self.conn.commit()
    
    def get_pending_emails(self, limit=10):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM email_queue 
            WHERE status = 'pending'
            ORDER BY created_at LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    
    def update_queue_status(self, queue_id, status, error_message=None):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE email_queue 
            SET status = ?, error_message = ?, attempts = attempts + 1,
                sent_at = CASE WHEN ? = 'sent' THEN CURRENT_TIMESTAMP ELSE sent_at END
            WHERE id = ?
        ''', (status, error_message, status, queue_id))
        self.conn.commit()
    
    def log_email(self, client_id, recipient, email_type, status, error_message=None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO email_logs (client_id, recipient, email_type, status, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (client_id, recipient, email_type, status, error_message))
        self.conn.commit()
    
    def update_client_status(self, client_id, field, value):
        cursor = self.conn.cursor()
        cursor.execute(f"UPDATE clients SET {field} = ? WHERE id = ?", (value, client_id))
        self.conn.commit()
    
    def update_client(self, client_id, data):
        cursor = self.conn.cursor()
        for key, value in data.items():
            cursor.execute(f"UPDATE clients SET {key} = ? WHERE id = ?", (value, client_id))
        self.conn.commit()
        return True
    
    def get_queue_count(self, status):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM email_queue WHERE status = ?", (status,))
        row = cursor.fetchone()
        return row['count'] if row else 0
    
    def get_campaigns(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM email_campaigns ORDER BY start_date DESC")
        return [dict(row) for row in cursor.fetchall()]
    
    def create_campaign(self, data):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO email_campaigns (campaign_name, template_name, total_recipients, status)
            VALUES (?, ?, ?, 'running')
        ''', (data['name'], data['template_name'], data['total']))
        self.conn.commit()
        return cursor.lastrowid
    
    def stop_campaign(self, campaign_id):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE email_campaigns SET status = 'stopped' WHERE id = ?", (campaign_id,))
        self.conn.commit()
    
    def get_settings(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT key, value FROM system_settings")
        rows = cursor.fetchall()
        return {row['key']: row['value'] for row in rows}
    
    def update_settings(self, settings):
        cursor = self.conn.cursor()
        for key, value in settings.items():
            cursor.execute('''
                INSERT OR REPLACE INTO system_settings (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (key, str(value)))
        self.conn.commit()
    
    def get_autoreply_stats(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM email_logs WHERE email_type = 'confirmation_received'")
        confirmations = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM email_logs WHERE email_type = 'bank_request'")
        bank_requests = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM email_logs WHERE email_type = 'wallet_request'")
        wallet_requests = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM email_logs WHERE email_type IN ('bank_payout_confirmed', 'wallet_payout_confirmed')")
        completed_payouts = cursor.fetchone()['count']
        return {
            'confirmations_received': confirmations,
            'bank_requests': bank_requests,
            'wallet_requests': wallet_requests,
            'completed_payouts': completed_payouts
        }
    
    def get_recent_auto_replies(self, limit=30):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT l.*, c.first_name, c.last_name, c.email
            FROM email_logs l
            LEFT JOIN clients c ON l.client_id = c.id
            WHERE l.email_type IN ('confirmation_received', 'bank_request', 'wallet_request', 
                                   'bank_payout_confirmed', 'wallet_payout_confirmed')
            ORDER BY l.sent_at DESC LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_autoreply_logs_paginated(self, page, per_page):
        offset = (page - 1) * per_page
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT l.*, c.first_name, c.last_name, c.email
            FROM email_logs l
            LEFT JOIN clients c ON l.client_id = c.id
            ORDER BY l.sent_at DESC LIMIT ? OFFSET ?
        ''', (per_page, offset))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_autoreply_logs_count(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM email_logs")
        return cursor.fetchone()['count']
    
    def get_autoreply_settings(self):
        return self.get_settings()
    
    def update_autoreply_settings(self, settings):
        self.update_settings(settings)