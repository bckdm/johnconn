from app import app

if __name__ == '__main__':
    print("=" * 60)
    print("EMAIL AUTOMATION SYSTEM")
    print("=" * 60)
    print("CAMPAIGN MANAGER:")
    print("  http://localhost:5000/campaign-dashboard")
    print("")
    print("AUTO-REPLY MANAGER:")
    print("  http://localhost:5000/autoreply-dashboard")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5000, debug=True)