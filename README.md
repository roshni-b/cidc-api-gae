# CIDC API <!-- omit in TOC -->

The next generation of the CIDC API, reworked to use Google Cloud-managed services. This API is built with the Eve REST API framework backed by Google Cloud SQL, running on Google App Engine.

## Development <!-- omit in TOC -->

- [Install Python dependencies](#Install-Python-dependencies)
- [Database](#Database)
  - [Setting up a local development database](#Setting-up-a-local-development-database)
  - [Connecting to a Cloud SQL database instance](#Connecting-to-a-Cloud-SQL-database-instance)
  - [Running Migrations](#Running-Migrations)
- [Running Locally](#Running-Locally)
- [Testing](#Testing)
- [Code Formatting](#Code-Formatting)
- [Deployment](#Deployment)


### Install Python dependencies
Install both the production and development dependencies.
```python
pip install -r requirements.txt -r requirements.dev.txt
```

### Database

#### Setting up a local development database
In production, the CIDC API connects to a PostgreSQL instance hosted by Google Cloud SQL, but for local development, you should generally use a local PostgreSQL instance.

To do so, first install and start PostgreSQL:
```bash
brew install postgresql
brew services start postgresql # launches the postgres service whenever your computer launches
```
By default, the postgres service listens on port 5432. Next, create your local `cidc` development database and the `cidcdev` user:
```bash
psql -c "create database cidc"
psql -c "create user cidcdev with password '1234'"
psql -c "grant all privileges on database cidc to cidcdev"
```
Now, you should be able to connect to your development database with the URI `postgres://cidcdev:1234@localhost:5432/cidc`. Or, in the postgres REPL:
```bash
psql cidc
```

Next, you'll need to set up the appropriate tables, indexes, etc. in your local database. To do so, `cd` into the `cidc-api` directory, then run:
```bash
FLASK_APP=app.py flask db upgrade
```
For more details on creating and running migrations, see [Running Migrations](#Running-Migrations).

#### Connecting to a Cloud SQL database instance
[TODO]

#### Running Migrations
This project uses [`Flask Migrate`](https://flask-migrate.readthedocs.io/en/latest/) for managing database migrations. To create a new migration and upgrade the database specified in your `.env` config, run the following from inside the `cidc-api` directory:
```bash
export FLASK_APP=app.py
# Generate the migration script
flask db migrate
# Apply changes to the database
flask db upgrade
```
To revert an applied migration, run:
```bash
flask db downgrade
```
If you're updating `models.py`, you should create a migration and commit the resulting 

### Running Locally

Once you have a development database set up and running, run the API server:
```bash
python cidc-api/app.py
```

### Testing
This project uses [`pytest`](https://docs.pytest.org/en/latest/) for testing. 

To run the tests, simply run:
```bash
pytest
```

### Code Formatting
This project uses [`black`](https://black.readthedocs.io/en/stable/) for code styling. 

We recommend setting up autoformatting-on-save in your IDE of choice so that you don't have to worry about running `black` on your code.

### Deployment
Deploy the application to Google App Engine by running the following:
```bash
gcloud app deploy <app.staging.yaml or app.prod.yaml> --project <gcloud project id>
```

Going forward, deployment will happen as part the Travis CI pipeline, and we shouldn't need to deploy by hand.