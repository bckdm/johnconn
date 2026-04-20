import os
import logging
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_cors import CORS
import pandas as pd
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'secret-key')
CORS(app)

UPLOAD_FOLDER = 'uploads'
TEMPLATE_FOLDER = 'templates/email_templates'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

from database import Database
from email_sender import BulkEmailSender
from email_processor import EmailProcessor

db = Database()
email_sender = BulkEmailSender(db)
email_processor = EmailProcessor(db)

# Start background auto-reply service
def start_auto_reply_service():
    def run():
        while True:
            try:
                email_processor.process_incoming_emails()
                import time
                time.sleep(30)
            except Exception as e:
                logger.error(f"Auto-reply error: {e}")
                time.sleep(60)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    logger.info("Auto-reply service started")

# Create default template if not exists
default_template = os.path.join(TEMPLATE_FOLDER, '1-Ruckerstattung.html')
if not os.path.exists(default_template):
    with open(default_template, 'w', encoding='utf-8') as f:
        f.write('''<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body>
    <h2>Hallo {{first_name}} {{last_name}},</h2>
    <p>Ihre Rückerstattung in H�he von {{btc_sum}} BTC wurde genehmigt.</p>
    <p>Aktueller BTC/EUR Kurs: {{btc_eur_rate}} EUR</p>
    <p>Bitte bestätigen Sie die Rückerstattung:</p>
    <a href="mailto:{{reply_email}}?subject=R%C3%BCckerstattung-best%C3%A4tigt&amp;body=Ich%20best%C3%A4tige%20die%20R%C3%BCckerstattung%20und%20akzeptiere%20die%20AGB." 
       style="background:#126DE5; color:white; padding:10px 20px; text-decoration:none; border-radius:5px;">
        Rückerstattung bestätigen
    </a>
    <p>Datum: {{date}}</p>
</body>
</html>''')

# Create auto-reply templates if not exists
auto_templates = ['2-Auszahlung.html', '3-Auszahlung autorisiert - Bankkonto.html', '3-Auszahlung autorisiert - Wallet.html', '4-Auszahlung per Bankuberweisung.html', '4-Freigabecode.html']
for template in auto_templates:
    path = os.path.join(TEMPLATE_FOLDER, template)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(f'''<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body>
    <h2>Hallo {{first_name}} {{last_name}},</h2>
    <p>Ihre Daten:</p>
    <p>BTC Betrag: {{btc_sum}} BTC</p>
    <p>IBAN: {{iban}}</p>
    <p>Wallet: {{wallet_address}}</p>
    <p>Datum: {{date}}</p>
</body>
</html>''')

# ==================== CAMPAIGN ROUTES ====================
@app.route('/')
def index():
    return redirect(url_for('campaign_dashboard'))

@app.route('/campaign-dashboard')
def campaign_dashboard():
    templates = [f for f in os.listdir(TEMPLATE_FOLDER) if f.endswith('.html')]
    stats = {
        'total_clients': db.get_clients_count(),
        'emails_sent': db.get_queue_count('sent'),
        'pending_emails': db.get_queue_count('pending'),
        'failed_emails': db.get_queue_count('failed'),
        'warmup_week': email_sender.warmup_week,
        'daily_limit': email_sender.daily_limit,
        'sent_today': email_sender.sent_today
    }
    campaigns = db.get_campaigns()
    return render_template('campaign_dashboard.html', stats=stats, campaigns=campaigns, templates=templates)

@app.route('/campaign-clients')
def campaign_clients():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '')
    clients = db.get_clients_paginated(page, 50, search)
    total = db.get_clients_count(search)
    return render_template('campaign_clients.html', clients=clients, page=page, total=total, search=search)

@app.route('/campaign-clients/upload', methods=['POST'])
def upload_clients():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('campaign_clients'))
    
    if not ('.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in {'xlsx', 'xls'}):
        flash('Invalid file type. Use Excel files.', 'error')
        return redirect(url_for('campaign_clients'))
    
    try:
        df = pd.read_excel(file)
        df = df.where(pd.notnull(df), None)
        
        for _, row in df.iterrows():
            db.upsert_client({
                'id': row.get('ID'),
                'first_name': row.get('First'),
                'last_name': row.get('Last'),
                'email': row.get('Email'),
                'phone': row.get('Phone'),
                'btc_sum': row.get('BTC-Sum'),
                'btc_fee': row.get('BTC-Fee'),
                'btc_address': row.get('BTC-Address'),
                'btc_url': row.get('BTC-URL'),
                'qr_code': row.get('QR-Code'),
                'iban': row.get('IBAN'),
                'wallet_address': row.get('Wallet-Address'),
                'tx_id': row.get('TxID'),
                'tx_href': row.get('TxHref')
            })
        flash(f'Uploaded {len(df)} clients successfully', 'success')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
    
    return redirect(url_for('campaign_clients'))

@app.route('/campaign-templates')
def campaign_templates():
    templates = [f for f in os.listdir(TEMPLATE_FOLDER) if f.endswith('.html')]
    return render_template('campaign_templates.html', templates=templates)

@app.route('/campaign-templates/upload', methods=['POST'])
def upload_template():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('campaign_templates'))
    
    if not ('.' in file.filename and file.filename.rsplit('.', 1)[1].lower() == 'html'):
        flash('Only HTML files allowed', 'error')
        return redirect(url_for('campaign_templates'))
    
    filename = secure_filename(file.filename)
    file.save(os.path.join(TEMPLATE_FOLDER, filename))
    flash(f'Template {filename} uploaded', 'success')
    return redirect(url_for('campaign_templates'))

@app.route('/campaign-templates/delete/<filename>', methods=['POST'])
def delete_template(filename):
    path = os.path.join(TEMPLATE_FOLDER, filename)
    if os.path.exists(path):
        os.remove(path)
        flash(f'Deleted {filename}', 'success')
    return redirect(url_for('campaign_templates'))

@app.route('/campaigns/start', methods=['POST'])
def start_campaign():
    data = request.json
    campaign_name = data.get('campaign_name')
    template_name = data.get('template_name')
    
    clients = db.get_clients_for_campaign({})
    if not clients:
        return jsonify({'error': 'No clients found'}), 400
    
    campaign_id = db.create_campaign({
        'name': campaign_name,
        'template_name': template_name,
        'total': len(clients)
    })
    
    for client in clients:
        db.add_to_email_queue(client['id'], client['email'], template_name, 'Ihre Rückerstattung')
    
    return jsonify({'success': True, 'total': len(clients)})

@app.route('/campaigns/stop/<int:campaign_id>', methods=['POST'])
def stop_campaign_route(campaign_id):
    db.stop_campaign(campaign_id)
    return jsonify({'success': True})

# ==================== AUTO-REPLY ROUTES ====================
@app.route('/autoreply-dashboard')
def autoreply_dashboard():
    stats = db.get_autoreply_stats()
    replies = db.get_recent_auto_replies(30)
    return render_template('autoreply_dashboard.html', stats=stats, replies=replies)

@app.route('/autoreply-logs')
def autoreply_logs():
    page = request.args.get('page', 1, type=int)
    logs = db.get_autoreply_logs_paginated(page, 50)
    total = db.get_autoreply_logs_count()
    return render_template('autoreply_logs.html', logs=logs, page=page, total=total)

@app.route('/autoreply-settings', methods=['GET', 'POST'])
def autoreply_settings():
    if request.method == 'POST':
        settings = {
            'imap_server': request.form.get('imap_server', ''),
            'imap_port': request.form.get('imap_port', '993'),
            'imap_user': request.form.get('imap_user', ''),
            'imap_password': request.form.get('imap_password', ''),
            'auto_smtp_server': request.form.get('smtp_server', ''),
            'auto_smtp_port': request.form.get('smtp_port', '465'),
            'auto_smtp_user': request.form.get('smtp_user', ''),
            'auto_smtp_password': request.form.get('smtp_password', ''),
            'reply_email': request.form.get('reply_email', 'auszahlung@as-blockchain.com'),
            'warmup_enabled': request.form.get('warmup_enabled', 'true'),
            'warmup_week': request.form.get('warmup_week', '1'),
            'daily_email_limit': request.form.get('daily_email_limit', '50'),
            'smtp_server': request.form.get('smtp_server', ''),
            'smtp_port': request.form.get('smtp_port', '465'),
            'smtp_user': request.form.get('smtp_user', ''),
            'smtp_password': request.form.get('smtp_password', '')
        }
        db.update_autoreply_settings(settings)
        flash('Settings saved successfully', 'success')
        return redirect(url_for('autoreply_settings'))
    
    settings = db.get_autoreply_settings()
    warmup_schedule = email_sender.get_warmup_schedule()
    return render_template('autoreply_settings.html', settings=settings, warmup_schedule=warmup_schedule)

@app.route('/autoreply-templates')
def autoreply_templates():
    templates = {
        '2-Auszahlung.html': 'Sent after client confirms Rückerstattung',
        '3-Auszahlung autorisiert - Bankkonto.html': 'Sent after client requests bank payout',
        '3-Auszahlung autorisiert - Wallet.html': 'Sent after client requests wallet payout',
        '4-Auszahlung per Bankuberweisung.html': 'Final confirmation for bank transfer',
        '4-Freigabecode.html': 'Final confirmation with code for wallet'
    }
    existing = [f for f in os.listdir(TEMPLATE_FOLDER) if f.endswith('.html')]
    return render_template('autoreply_templates.html', templates=templates, existing=existing)

@app.route('/autoreply-templates/edit/<filename>', methods=['GET', 'POST'])
def edit_template(filename):
    path = os.path.join(TEMPLATE_FOLDER, filename)
    
    if request.method == 'POST':
        with open(path, 'w', encoding='utf-8') as f:
            f.write(request.form.get('content', ''))
        flash(f'Saved {filename}', 'success')
        return redirect(url_for('autoreply_templates'))
    
    content = ''
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        content = '''<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body>
    <h2>Hallo {{first_name}} {{last_name}},</h2>
    <p>Ihre Daten:</p>
    <p>BTC Betrag: {{btc_sum}} BTC</p>
    <p>IBAN: {{iban}}</p>
    <p>Wallet: {{wallet_address}}</p>
    <p>Datum: {{date}}</p>
</body>
</html>'''
    
    return render_template('autoreply_template_edit.html', filename=filename, content=content)

@app.route('/api/queue-status')
def api_queue_status():
    return jsonify({
        'pending': db.get_queue_count('pending'),
        'sending': db.get_queue_count('sending'),
        'sent': db.get_queue_count('sent'),
        'failed': db.get_queue_count('failed')
    })

if __name__ == '__main__':
    start_auto_reply_service()
    app.run(host='0.0.0.0', port=5000, debug=True)