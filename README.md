# gold-data-pipepline

Developer setup
---------------

Create and activate a Python venv (optional but recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies (includes a forked tvdatafeed required for TradingView integration):

```bash
pip install -r requirements.txt
# or explicitly install the fork if needed:
pip install --upgrade --no-cache-dir git+https://github.com/rongardF/tvdatafeed.git
```

Configuration
-------------

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Edit `.env` and configure your settings:
   - MongoDB connection
   - TradingView credentials (optional)
   - **Discord Alerts** (optional but recommended)

### Discord Alerts Setup

To receive error alerts in Discord:

1. Create a Discord webhook in your server
2. Enable alerts in `.env`:
```env
DISCORD_ALERT_ENABLED=true
DISCORD_WEBHOOK_URL=your_webhook_url_here
```

3. Test the alerts:
```bash
python test_discord_alert.py
```

📖 **Detailed guide**: See [DISCORD_SETUP_VI.md](DISCORD_SETUP_VI.md) for Vietnamese instructions or [docs/DISCORD_ALERTS.md](docs/DISCORD_ALERTS.md) for full documentation.

Usage
-----

To run the application, use the following command:

```bash
python main.py
```

Contributing
------------

We welcome contributions! Please fork the repository and submit a pull request.

License
-------

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.