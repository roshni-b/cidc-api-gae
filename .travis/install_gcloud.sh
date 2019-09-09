# !/usr/bin/bash
#
# Install the Google Cloud SDK. Values for these environment 
# variables must be set in the Travis CI console for this to work:
#   GCLOUD_SERVICE_ACCOUNT_JSON_STAGING
#     -> base64-encoded service account JSON credentials for the staging project
#   GCLOUD_SERVICE_ACCOUNT_JSON_PROD 
#     -> base64-encoded service account JSON credentials for the production project

if [ ! -d ${HOME}/google-cloud-sdk/bin ]; then
    rm -rf $HOME/google-cloud-sdk;
    curl https://sdk.cloud.google.com | bash > /dev/null;
fi
source $HOME/google-cloud-sdk/path.bash.inc
if [ "$TRAVIS_BRANCH" = production ]; then
    gcloud config set project $GCLOUD_PROJECT_ID_PROD;
    export GCLOUD_SERVICE_ACCOUNT_JSON="$GCLOUD_SERVICE_ACCOUNT_JSON_PROD";
else
    gcloud config set project $GCLOUD_PROJECT_ID_STAGING;
    export GCLOUD_SERVICE_ACCOUNT_JSON="$GCLOUD_SERVICE_ACCOUNT_JSON_STAGING";
fi
echo "$GCLOUD_SERVICE_ACCOUNT_JSON" | base64 --decode > $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud components update