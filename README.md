## Installation

To install the API layer locally, just create a virtual environment and install the packages with `pip`

```bash
python3 -m venv .env
pip install -r requirements.txt
```

## Start the server

Starting the API locally is as easy as running uvicorn

```bash
uvicorn main:app --reload
```

## Test queries

With the server running you can easily test your GraphQL queries by surfing over to http://127.0.0.1:8000/graphql on your favorite browser.
