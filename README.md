# CIDC API

The next generation of the CIDC API, reworked to use Google Cloud-managed infrastructure. This API is built with the Eve REST API framework backed by Google Cloud SQL, running on Google App Engine.

## Development

### Install Python dependencies
```python
pip install -r requirements.txt
```

### Setting up a development database
In production, the CIDC API connects to a PostgreSQL instance hosted by Google Cloud SQL, but for local development, you should use a local PostgreSQL instance.

To do so, first install and start PostgreSQL:
```bash
brew install postgresql
brew services start postgresql # launches the postgres service whenever your computer launches
```
By default, the postgres service listens on port 5432. Next, create your local `cidc` development database and the `cidcdev` user:
```bash
psql --command="create database cidc"
psql --command="create user cidcdev with password '1234'"
psql --command="grant all privileges on database cidc to cidcdev"
```
Now, you should be able to connect to your development database with the URI `postgres://cidcdev:1234@localhost:5432/cidc`. Or, in the postgres REPL:
```bash
psql cidc
```

### Run tests
```bash
pytest
```
### Deployment
[TODO]