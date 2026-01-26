// ============================================================================
// MEMGRAPH INDEX CREATION SCRIPT
// ============================================================================
// This script creates all recommended indexes for the CCDI Federation Service
// Run this script on the TARGET Memgraph instance.
//
// Generated from:
//   - memgraph-query-results-export_target.tsv
//   - memgraph-query-results-export.tsv
//
// Includes all edge indexes, label indexes, and label+property indexes
// ============================================================================

// ----------------------------------------------------------------------------
// EDGE INDEXES (Relationship Type Indexes)
// ----------------------------------------------------------------------------

CREATE EDGE INDEX ON :IN_STUDY;
CREATE EDGE INDEX ON :of_cell_line;
CREATE EDGE INDEX ON :of_consent_group;
CREATE EDGE INDEX ON :of_diagnosis;
CREATE EDGE INDEX ON :of_participant;
CREATE EDGE INDEX ON :of_pathology_file;
CREATE EDGE INDEX ON :of_sample;
CREATE EDGE INDEX ON :of_sequencing_file;

// ----------------------------------------------------------------------------
// LABEL INDEXES (Node Type Indexes)
// ----------------------------------------------------------------------------

CREATE INDEX ON :cell_line;
CREATE INDEX ON :consent_group;
CREATE INDEX ON :diagnosis;
CREATE INDEX ON :participant;
CREATE INDEX ON :pathology_file;
CREATE INDEX ON :sample;
CREATE INDEX ON :sequencing_file;
CREATE INDEX ON :study;
CREATE INDEX ON :treatment_response;

// ----------------------------------------------------------------------------
// LABEL + PROPERTY INDEXES
// ----------------------------------------------------------------------------

// Cell line indexes
CREATE INDEX ON :cell_line(cell_line_id);
CREATE INDEX ON :cell_line(id);

// Consent group indexes
CREATE INDEX ON :consent_group(consent_group_id);
CREATE INDEX ON :consent_group(id);

// Diagnosis indexes
CREATE INDEX ON :diagnosis(diagnosis);
CREATE INDEX ON :diagnosis(disease_phase);
CREATE INDEX ON :diagnosis(id);
CREATE INDEX ON :diagnosis(tumor_classification);
CREATE INDEX ON :diagnosis(tumor_grade);

// Participant indexes
CREATE INDEX ON :participant(id);
CREATE INDEX ON :participant(participant_id);
CREATE INDEX ON :participant(race);
CREATE INDEX ON :participant(sex_at_birth);

// Pathology file indexes
CREATE INDEX ON :pathology_file(fixation_embedding_method);
CREATE INDEX ON :pathology_file(id);

// Sample indexes
CREATE INDEX ON :sample(anatomic_site);
CREATE INDEX ON :sample(id);
CREATE INDEX ON :sample(sample_id);

// Sequencing file indexes
CREATE INDEX ON :sequencing_file(file_type);
CREATE INDEX ON :sequencing_file(id);
CREATE INDEX ON :sequencing_file(library_selection);
CREATE INDEX ON :sequencing_file(library_source_material);
CREATE INDEX ON :sequencing_file(library_source_molecule);
CREATE INDEX ON :sequencing_file(library_strategy);

// Study indexes
CREATE INDEX ON :study(id);
CREATE INDEX ON :study(study_id);

// Study-related indexes
CREATE INDEX ON :study_admin(id);
CREATE INDEX ON :study_arm(id);
CREATE INDEX ON :study_funding(id);
CREATE INDEX ON :study_personnel(id);

// Survival indexes
CREATE INDEX ON :survival(id);
CREATE INDEX ON :survival(last_known_survival_status);

// Other entity indexes
CREATE INDEX ON :clinical_measure_file(id);
CREATE INDEX ON :cytogenomic_file(id);
CREATE INDEX ON :exposure(id);
CREATE INDEX ON :family_relationship(id);
CREATE INDEX ON :generic_file(id);
CREATE INDEX ON :genetic_analysis(id);
CREATE INDEX ON :laboratory_test(id);
CREATE INDEX ON :medical_history(id);
CREATE INDEX ON :methylation_array_file(id);
CREATE INDEX ON :pdx(id);
CREATE INDEX ON :publication(id);
CREATE INDEX ON :radiology_file(id);
CREATE INDEX ON :synonym(id);
CREATE INDEX ON :treatment(id);
CREATE INDEX ON :treatment_response(id);

// ----------------------------------------------------------------------------
// VERIFICATION
// ----------------------------------------------------------------------------

// Verify indexes were created
SHOW INDEXES;

// ----------------------------------------------------------------------------
// NOTES
// ----------------------------------------------------------------------------
// - Node indexes improve query performance for property lookups and filtering
// - Edge indexes improve traversal performance for relationship queries
// - The IN_STUDY edge index is particularly important for sample/subject queries
// - If an index already exists, Memgraph will return an error (safe to ignore)
// - This script includes all indexes from both target and export TSV files
// ============================================================================
