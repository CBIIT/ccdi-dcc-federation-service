-- ============================================================================
-- IN_STUDY Relationship Creation Script
-- ============================================================================
-- This script creates IN_STUDY relationships between:
--   1. Samples and studies
--   2. Participants and studies
-- 
-- IMPORTANT: This allows multiple relationships per entity (same sample_id 
-- or participant_id can appear in different studies), which is required for 
-- correct counting using (sample_id + study_id) or (participant_id + study_id) 
-- as unique identifier.
--
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Check Current State
-- ----------------------------------------------------------------------------
-- Check how many IN_STUDY relationships currently exist (samples)
MATCH (sa:sample)-[:IN_STUDY]->(st:study)
RETURN count(*) AS current_sample_relationships;

-- Check how many IN_STUDY relationships currently exist (participants)
MATCH (p:participant)-[:IN_STUDY]->(st:study)
RETURN count(*) AS current_participant_relationships;

-- Check if any samples have multiple IN_STUDY relationships
MATCH (sa:sample)-[:IN_STUDY]->(st:study)
WITH sa.sample_id AS sample_id, collect(DISTINCT st.study_id) AS studies
WHERE size(studies) > 1
RETURN sample_id, studies
ORDER BY size(studies) DESC
LIMIT 10;

-- Check if any participants have multiple IN_STUDY relationships
MATCH (p:participant)-[:IN_STUDY]->(st:study)
WITH p.participant_id AS participant_id, collect(DISTINCT st.study_id) AS studies
WHERE size(studies) > 1
RETURN participant_id, studies
ORDER BY size(studies) DESC
LIMIT 10;

-- Check total samples vs samples with IN_STUDY relationships
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND toString(sa.sample_id) <> ''
WITH count(DISTINCT sa) AS total_samples
MATCH (sa:sample)-[:IN_STUDY]->(st:study)
RETURN total_samples, count(DISTINCT sa) AS samples_with_in_study;

-- Check total participants vs participants with IN_STUDY relationships
MATCH (p:participant)
WHERE p.participant_id IS NOT NULL AND toString(p.participant_id) <> ''
WITH count(DISTINCT p) AS total_participants
MATCH (p:participant)-[:IN_STUDY]->(st:study)
RETURN total_participants, count(DISTINCT p) AS participants_with_in_study;

-- ----------------------------------------------------------------------------
-- STEP 2: Create IN_STUDY Relationships for SAMPLES
-- ----------------------------------------------------------------------------
-- Path 1: sample -> cell_line -> study
MATCH (sa:sample)-[:of_sample]->(cl:cell_line)-[:of_cell_line]->(st:study)
WHERE sa.sample_id IS NOT NULL 
  AND toString(sa.sample_id) <> ''
  AND st.study_id IS NOT NULL
MERGE (sa)-[:IN_STUDY]->(st);

-- Path 2: sample -> participant -> consent_group -> study
MATCH (sa:sample)-[:of_sample]->(p:participant)-[:of_participant]->(cg:consent_group)-[:of_consent_group]->(st:study)
WHERE sa.sample_id IS NOT NULL 
  AND toString(sa.sample_id) <> ''
  AND st.study_id IS NOT NULL
MERGE (sa)-[:IN_STUDY]->(st);

-- ----------------------------------------------------------------------------
-- STEP 3: Create IN_STUDY Relationships for PARTICIPANTS
-- ----------------------------------------------------------------------------
-- Path: participant -> consent_group -> study
MATCH (p:participant)-[:of_participant]->(cg:consent_group)-[:of_consent_group]->(st:study)
WHERE p.participant_id IS NOT NULL 
  AND toString(p.participant_id) <> ''
  AND st.study_id IS NOT NULL
MERGE (p)-[:IN_STUDY]->(st);

-- ----------------------------------------------------------------------------
-- STEP 4: Verify Creation
-- ----------------------------------------------------------------------------
-- Check total relationships created (samples)
MATCH (sa:sample)-[:IN_STUDY]->(st:study)
RETURN count(*) AS total_sample_relationships;

-- Check total relationships created (participants)
MATCH (p:participant)-[:IN_STUDY]->(st:study)
RETURN count(*) AS total_participant_relationships;

-- Check samples with multiple relationships (should exist if same sample_id in multiple studies)
MATCH (sa:sample)-[:IN_STUDY]->(st:study)
WITH sa.sample_id AS sample_id, collect(DISTINCT st.study_id) AS studies
WHERE size(studies) > 1
RETURN count(*) AS samples_with_multiple_studies,
       sample_id, studies
ORDER BY size(studies) DESC
LIMIT 10;

-- Check participants with multiple relationships (should exist if same participant_id in multiple studies)
MATCH (p:participant)-[:IN_STUDY]->(st:study)
WITH p.participant_id AS participant_id, collect(DISTINCT st.study_id) AS studies
WHERE size(studies) > 1
RETURN count(*) AS participants_with_multiple_studies,
       participant_id, studies
ORDER BY size(studies) DESC
LIMIT 10;

-- Verify all samples with valid study paths have IN_STUDY relationships
MATCH (sa:sample)
WHERE sa.sample_id IS NOT NULL AND toString(sa.sample_id) <> ''
OPTIONAL MATCH (sa)-[:of_sample]->(:cell_line)-[:of_cell_line]->(st1:study)
OPTIONAL MATCH (sa)-[:of_sample]->(:participant)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st2:study)
WITH sa, coalesce(st1, st2) AS st
WHERE st IS NOT NULL
OPTIONAL MATCH (sa)-[:IN_STUDY]->(st_check:study)
WHERE st_check.study_id = st.study_id
RETURN 
    count(DISTINCT sa) AS samples_with_study_path,
    count(DISTINCT CASE WHEN st_check IS NOT NULL THEN sa END) AS samples_with_in_study,
    count(DISTINCT CASE WHEN st_check IS NULL THEN sa END) AS samples_missing_in_study;

-- Verify all participants with valid study paths have IN_STUDY relationships
MATCH (p:participant)
WHERE p.participant_id IS NOT NULL AND toString(p.participant_id) <> ''
OPTIONAL MATCH (p)-[:of_participant]->(:consent_group)-[:of_consent_group]->(st:study)
WHERE st IS NOT NULL
OPTIONAL MATCH (p)-[:IN_STUDY]->(st_check:study)
WHERE st_check.study_id = st.study_id
RETURN 
    count(DISTINCT p) AS participants_with_study_path,
    count(DISTINCT CASE WHEN st_check IS NOT NULL THEN p END) AS participants_with_in_study,
    count(DISTINCT CASE WHEN st_check IS NULL THEN p END) AS participants_missing_in_study;

