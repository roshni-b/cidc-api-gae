# CIDC API Setup <!-- omit in TOC -->

This guide contains instructions for provisioning the CIDC API service from scratch. While the guide here outlines a manual process for spinning up and provisioning the different components of the system, there are lots of opportunities for automation that should be explored going forward.

- [Prerequisites](#Prerequisites)
- [Google App Engine](#Google-App-Engine)
- [Google Cloud SQL](#Google-Cloud-SQL)
- [Auth0](#Auth0)
- [Secrets Management](#Secrets-Management)
- [Travis CI](#Travis-CI)

## Prerequisites

Like other components of the CIDC system, the CIDC API service requires the following preconditions be satisfied before provisioning/deployment is possible:

- There exist two Auth0 tenants, e.g., `cidc-test` and `cidc`, for staging and production instances of the application.
- There exist two Google Cloud Platform (GCP) projects, `CIDC-DFCI-STAGING` and `CIDC-DFCI`, for staging and production instances of the application.

These Auth0 tenants and GCP projects are shared across all components of the CIDC system.

You'll also need to install the [Google Cloud SDK](https://cloud.google.com/sdk/install). If you don't have it already, follow the appropriate [gcloud quickstart](https://cloud.google.com/sdk/docs/quickstarts) to get set up.

## Google App Engine

The core CIDC API code runs in a Google App Engine service. We need to create GAE applications in both the staging and production projects to host this service. 

To do so from the command-line, run:
```bash
gcloud app create --project=$PROJECT --region='us-central1'
```
for both the staging and production projects.

**Note:** running the above commands does not actually deploy any code. Rather, it provisions App Engine environments to which you can deploy code.

**Note:** we choose `us-central1` as our region because we expect this API to be accessed by clients across the continental US.

Visit the [GCP management console](https://console.cloud.google.com) and check that you can visit the App Engine dashboard in both the production and staging projects.



## Google Cloud SQL

The CIDC API service running in GAE connects to a PostgreSQL instance running in Cloud SQL. We need to create a Cloud SQL instance in both the staging and production environments, then create the `cidc` database inside of both instances.

To create the Cloud SQL instances from the command-line, run (**with a replacement for `[A_SNEAKY_PASSWORD]`**):
```bash
# Create a Cloud SQL postgres instance in the staging project
gcloud sql instances create cidc-postgres --root-password=[A_SNEAKY_PASSWORD] --database-version=POSTGRES_9_6 --tier=db-n1-standard-1 --region='us-central1' --project=$PROJECT
```
for both the staging and production projects.

**Note:** the above command provisions an instance of type `db-n1-standard-1`, which allocates 1 vCPU, 3.75 GB of RAM, and a maximum storage capacity of about 30 TB. If usage expectations have changed since the writing of this document, check out Cloud SQL's [instance type overview](https://cloud.google.com/sql/pricing#2nd-gen-instance-pricing) and consider using a different instance type if appropriate.

To create the `cidc` database in both the staging and production Cloud SQL instances from the command-line, run:
```bash
gcloud sql databases create cidc --instance='cidc-postgres' --project=$PROJECT
```
for both the staging and production projects.

## Auth0
In both the `cidc-test` and `cidc` Auth0 tenants for the staging and production APIs respectively, do the following:

1) If there exists an application of type `Regular Web Application` that's already in use by the CIDC frontend associated with `$PROJECT`, select that application (currently, this exists and is called `cidc-portal` in both tenants). Otherwise, create a new application of type `Regular Web Application` -- this application will be shared between the API and the UI.
2) Add `https://$PROJECT.appspot.com` along with whatever public-facing URL you've configured (e.g., `https://staging-api.cimac-network.org`) to the "Allowed Web Origins" list).
3) Copy the Client ID, and add this to the appropriate `app.*.yaml` file for `$PROJECT`.

The staging Auth0 client can also be convenient for debugging authentication functionality locally, so set `AUTH0_CLIENT_ID` to the staging Client ID in your `.env` file.

## Secrets Management

The CIDC API service relies on secrets stored in a Google Cloud Storage bucket for connecting to the database and accessing third party services like Auth0. Why GCS? Because data stored in GCS is [encrypted-at-rest](https://cloud.google.com/security/encryption-at-rest/), and because this means sharing secrets across developers is as simple as granting read access to the appropriate secrets bucket.

First, make sure you have the `Storage Admin` role in both Google Cloud Projects. Create the secrets storage buckets using `gsutil`:
```bash
# Create the staging secrets storage bucket
gsutil mb -c regional -l us-central1 -p cidc-dfci-staging gs://cidc-secrets-staging

# Create the production secrets storage bucket
gsutil mb -c regional -l us-central1 -p cidc-dfci gs://cidc-secrets-prod 
```
The CIDC API currently requires access to the following secrets to run in staging/production mode:
- `AUTH0_CLIENT_SECRET`: the Auth0 application client secret. (Note: this value isn't currently used by the API, since we're using the implicit OpenID Connect flow in the UI; we might as well make this secret available to the backend, though, just in case this changes)
- `CLOUD_SQL_DB_PASS`: the password for the Cloud SQL `postgres` user (`--root-password` as set above)

To add a new secret value to GCS, run:
```bash
# Set the current project to be the project for which you want to add secrets
gcloud config set-value project $PROJECT

# Write the secret value to a file named after the secret
echo [SECRET_VALUE] > [SECRET_NAME]

# Upload the file to the appropriate secret bucket
gsutil mv [SECRET_NAME] [SECRET_BUCKET]
# NOTE: `gsutil mv` should delete the local file containing
# the secret value after upload, but if it doesn't, make
# sure not to check this file into source control (it's secret!).
```
Perform this operation for all of the required secrets for both the production and staging environments.


## Travis CI

The CIDC system uses Travis CI as its continuous integration and continuous deployment service. For Travis to be able to deploy to our staging and production projects in GCP, it needs access to [GCP service account](https://cloud.google.com/iam/docs/service-accounts) credentials.

To create a service account for Travis CI, grant it the appropriate permissions, and download the credentials associated with the service account, run:
```bash
# Create the travis-ci service account
gcloud iam service-accounts create travis-ci --project=$PROJECT --display-name='Travis CI'

# Add IAM roles necessary for deployment to the staging service account
gcloud projects add-iam-policy-binding $PROJECT \
    --member serviceAccount:travis-ci@$PROJECT.iam.gserviceaccount.com \
    --role roles/compute.storageAdmin

gcloud projects add-iam-policy-binding $PROJECT \
    --member serviceAccount:travis-ci@$PROJECT.iam.gserviceaccount.com \
    --role roles/appengine.appAdmin

gcloud projects add-iam-policy-binding $PROJECT \
    --member serviceAccount:travis-ci@$PROJECT.iam.gserviceaccount.com \
    --role roles/cloudbuild.builds.editor

# Download the credentials associated with the service account
# then base64 encode it
gcloud iam service-accounts keys create ~/travis-$PROJECT-key.json --iam-account travis-ci@$PROJECT.iam.gserviceaccount.com

cat ~/travis-$PROJECT-key.json | base64 > ~/travis-$PROJECT-key-b64
```
for both the staging and production projects.

Now, the service account credentials for each project should be stored in `~/travis-$PROJECT-key-b64` (they're base64 encoded to avoid having to escape special characters; they are *not* encrypted, so keep them secret!). 

We need to securely pass these credentials to Travis (i.e., without committing them in this repository). To do so, you'll need to install the `travis` command-line utility. Then, run:
```bash
# Authenticate / authorize with Travis
travis login --org

# Set the remote environment variables referenced in .travis.yml
# (Travis will store these values securely and keep them secret)
travis env set GCLOUD_SERVICE_ACCOUNT_STAGING $(cat ~/travis-cidc-dfci-staging-key-b64)
travis env set GCLOUD_SERVICE_ACCOUNT_PROD $(cat ~/travis-cidc-dfci-key-b64)
```
It's also possible to do this manually in the Travis dashboard for the repository by going to `More Options > Settings > Environment Variables` and setting these same environment variables appropriately.

Now, manually trigger a build in the Travis dashboard on the both the `master` and `production` branches. Both branches should successfully deploy to the staging and production GAE projects respectively at the end of their build.

