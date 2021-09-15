# Changelog

This Changelog tracks changes to this project as well as details about upcoming releases. The project follows Semantic Versioning. The notes below include a summary for each release, followed by details which contain one or more of the following tags:
- `added` for new features.
- `changed` for functionality and API changes.
- `deprecated` for soon-to-be removed features.
- `removed` for now removed features.
- `fixed` for any bug fixes.
- `security` in case of vulnerabilities.


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
- `added` Add some safety and flexibility to reading ([#570](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/570)))
- `fixed` Fix header check; add better error handling and tests [#571](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/571)
- `added` Add 5 new optional columns to PBMC manifest for TCRseq [#572](https://github.com/CIMAC-CIDC/cidc-api-gae/pull/572)
