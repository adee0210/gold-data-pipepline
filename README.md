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
# gold-data-pipepline
