# CIDC API Setup <!-- omit in TOC -->

This guide contains instructions for provisioning the CIDC API service from scratch. While the guide here outlines a manual process for spinning up and provisioning the different components of the system, there are lots of opportunities for automation that should be explored going forward.

- [Prerequisites](#prerequisites)
- [Google App Engine](#google-app-engine)
- [Auth0](#auth0)
- [Travis CI](#travis-ci)

## Prerequisites

The CIDC API service requires the following preconditions be satisfied before provisioning/deployment is possible:

- There exist two Auth0 tenants, e.g., `cidc-test` and `cidc`, for staging and production instances of the application, both configured to use Google as an IDP.
- There exist two Google Cloud Platform (GCP) projects, `CIDC-DFCI-STAGING` and `CIDC-DFCI`, for staging and production instances of the application.
- The steps in [`cidc-devops/docs/provision.md`](https://github.com/CIMAC-CIDC/cidc-devops/blob/v2/docs/provision.md) have been completed for both projects.

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

## Auth0

Copy the Client IDs for the Auth0 applications you've configured for the staging and production environments into the appropriate `app.*.yaml` file.

The staging Auth0 client can also be convenient for debugging authentication functionality locally, so set `AUTH0_CLIENT_ID` to the staging Client ID in your `.env` file.

## GitHub Actions

Ensure that the following secrets are made available to this repository via the GitHub organization settings:

```bash
GCP_SA_KEY_PROD # base64-encoded service account key for the prod environment
GCP_SA_KEY_STAGING # base64-encoded service account key for the staging environment
PYPI_PASSWORD # password for account used to publish packages to pypi
```

and that `CC_TEST_REPORTER_ID` (the codeclimate API key) is available as a repo-level secret.
