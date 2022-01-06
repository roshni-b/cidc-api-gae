# Changelog

This Changelog tracks changes to this project. The notes below include a summary for each release, followed by details which contain one or more of the following tags:

- `added` for new features.
- `changed` for functionality and API changes.
- `deprecated` for soon-to-be removed features.
- `removed` for now removed features.
- `fixed` for any bug fixes.
- `security` in case of vulnerabilities.

## Version `0.25.57` - 06 Jan 2022

- `fixed` can't apply expiry condition to upload buckets as they are ACL-controlled

## Version `0.25.56` - 06 Jan 2022

- `fixed` typo by adding missing `and`

## Version `0.25.55` - 05 Jan 2022

- `added` back IAM download functionality for production environment only, partially reverting commit 7504926685dcd00b0c20b41911ec8aba7f8b98b0
- `change` version definition location from `setup.py` to `__init__.py` to match schemas/cli

## Version `0.25.54` - 22 Dec 2021

- `changed` admin grant all download permissions to run through cloud function

## Version `0.25.53` - 16 Dec 2021

- `removed` all IAM conditions on data bucket

## Version `0.25.52` - 15 Dec 2021

- `removed` all conditional IAM expressions on data bucket

## Version `0.25.51` - 15 Dec 2021

- `added` calls to ACL save, and smoketests
- `added` back calls for adding/removing lister permissions, and smoketests

## Version `0.25.50` - 14 Dec 2021

- `fixed` ACL syntax again; see https://googleapis.dev/python/storage/latest/acl.html#google.cloud.storage.acl.ACL

## Version `0.25.49` - 14 Dec 2021

- `fixed` ACL syntax
- `added` function to call to add permissions for particular upload job
- `removed` GOOGLE_DATA_BUCKET entirely from API

## Version `0.25.48` - 08 Dec 2021

- `add` error logging in Permission.insert

## Version `0.25.47` - 08 Dec 2021

- `remove` all gcloud client logic associated with download logic ie conditional IAM permissions
- `add` ACL gcloud client logic for downloads instead
- `remove` all lister permission as no longer needed with ACL instead of IAM
- `add` admin endpoint to call already existing function to grant all download permissions

## Version `0.25.46` - 30 Nov 2021

- `changed` schemas dependency (bump) for WES pipeline updates

## Version `0.25.45` - 23 Nov 2021

- `changed` schemas dependency for WES paired analysis comments field

## Version `0.25.44` - 22 Nov 2021

- `added` dry_run option for both CSMS insert functions

## Version `0.25.43` - 22 Nov 2021

- `added` conversion for CSMS value 'pbmc' for processed sample type
- `added` handling in shipments dashboard for no shipment assay_type

## Version `0.25.42` - 15 Nov 2021

- `fixed` correctly pass session in more places

## Version `0.25.41` - 15 Nov 2021

- `added` logging to see if `insert_manifest_into_blob` is called as expected

## Version `0.25.40` - 12 Nov 2021

- `fixed` bug in iterating offset in `csms.auth.get_with_paging`

## Version `0.25.39` - 12 Nov 2021

- `fixed` CSMS bug from chaining `detect_manifest_changes` and `insert_manifest_...`

## Version `0.25.38` - 11 Nov 2021

- `added` excluded property to CSMS test data and tests
  - `fixed` trying to add CSMS properties to CIDC entries
- `added` de-identified whole manifest from CSMS directly to test data
  - `fixed` reference to CIMAC ID in sample creation within models.templates.csms_api.insert_manifest_from_json()
  - `fixed` dict.items() is unhashable, so use dict.keys() to generate a set to check for _calc_difference()

## Version `0.25.37` - 08 Nov 2021

- `changed` bump schemas dependencies for mIF DM bug fix

## Version `0.25.36` - 08 Nov 2021

- `added` logging around error in CSMS testing (`deprecated`)

## Version `0.25.35` - 04 Nov 2021

- `changed` version for schemas dependency, for tweak to mIF template

## Version `0.25.34` - 03 Nov 2021

- `add` unstructured JSONB json_data column for shipments, participants, samples
- `add` copy of original JSON or CSMS data into json_data column
- `deprecated` non-critical columns in relational manifests, adding to json_data
- `add` correct exclusion of legacy CSMS manifests

## Version `0.25.33` - 29 Oct 2021

- `fixed` fix mIF excluded samples tab
- `fixed` fix typo 'errrors'

## Version `0.25.32` - 27 Oct 2021

- `added` subquery for counting ATACseq analysis to get_summaries for Data Overview dashboard

## Version `0.25.31` - 27 Oct 2021

- `changed` bump schemas version for ATACseq analysis updates

## Version `0.25.30` - 27 Oct 2021

- `fixed` set os environ TZ = UTC before datetime is imported every time

## Version `0.25.29` - 26 Oct 2021

- `fixed` correctly pass session throughout models/templates/csms_api

## Version `0.25.28` - 26 Oct 2021

- `remove` incorrect accessing of CSMS manifest protocol_identifier which is only stored on the samples

## Version `0.25.27` - 25 Oct 2021

- `added` facets and file details for mIF report file
- `remove` Templates facet entirely

## Version `0.25.26` - 22 Oct 2021

- `fixed` second call to get_with_authorization again

## Version `0.25.25` - 22 Oct 2021

- `fixed` second call to get_with_authorization

## Version `0.25.24` - 22 Oct 2021

- `changed` schemas bump for mIF QC report

## Version `0.25.23` - 21 Oct 2021

- `changed` moved validation of trial's existing in the JSON blobs to better reflect name and usage

## Version `0.25.22` - 21 Oct 2021

- `fixed` pass limit and offset as params instead of kwargs to requests.get

## Version `0.25.21` - 19 Oct 2021

- `added` handling to remove old-style permissions

## Version `0.25.20` - 19 Oct 2021

- `added` logging to set_iam_policy errors

## Version `0.25.19` - 15 Oct 2021

- `changed` CSMS_BASE_URL and CSMS_TOKEN_URL to be pulled from secrets

## Version `0.25.18` - 14 Oct 2021

- `fixed` changed prefix generator to correctly handle prefixes without regex support

## Version `0.25.17` - 13 Oct 2021

- `changed` GCP permissions from single conditions to multi-conditions using || and && operators
- `changed` expiring permission to be on the general CIDC Lister role instead of every startsWith condition separately

## Version `0.25.16` - 07 Oct 2021

- `added` function for finding CSMS changes and getting updates for relational db
- `added` function to execute corresponding updates to JSON blob from CSMS changes

## Version `0.25.15` - 04 Oct 2021

- `added` grant_lister_access and revoke_lister_access for custom role CIDC Lister that is required for all downloads

## Version `0.25.14` - 24  Sept 2021

- `added` API endpoint to add a new manifest given JSON from CSMS

## Version `0.25.13` - 23 Sept 2021

- `added` added TWIST enum values to WES in relational tables

## Version `0.25.12` - 23 Sept 2021

- `added` schemas bump to add TWIST enum values to WES in JSON

## Version `0.25.11` - 22 Sept 2021

- `added` module export for models.templates

## Version `0.25.10` - 22 Sept 2021

- `changed` schemas bump to add TCRseq controls

## Version `0.25.9` - 22 Sept 2021

- `changed` schemas bump for new TCRseq Adaptive template

## Version `0.25.8` - 08 Sept 2021

### Summary

Initial set up of tables and definition of needed classes for base metadata and assay uploads. Generated new-style templates and added full testing data for pbmc, tissue_slide, h_and_e, wes_<fastq/bam>; demo for clinical_data. Implemented JSON -> Relational sync function and wired for testing. Added relational hooks into existing manifest and assay/analysis uploads. Added way to trigger initial synchronization. Allows relational ClinicalTrials to be edited along with TrialMetadatas from the admin panel.

### Details

- `added` JIRA integration ([#564](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/564))
- `added` `changed` Step 1 of Relational DB towards CSMS Integration ([#549](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/549/))
- `added` Add logging to syncall_from_blobs ([#565](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/565))
- `added` Add admin controls for relational Clinical Trials ([#567](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/567))
- `fixed` Some perfecting tweaks ([#568](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/568))
- `fixed` Make sure that new templates are identical to old ones ([#569](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/569))
- `added` Add some safety and flexibility to reading ([#570](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/570))
- `fixed` Fix header check; add better error handling and tests ([#571](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/571))
- `added` Add 5 new optional columns to PBMC manifest for TCRseq ([#572](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/572))