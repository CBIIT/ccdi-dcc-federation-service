
// ============================================================================
// GENERATED MATERIALIZED VIEWS CREATION SCRIPT
// ============================================================================
// This script was auto-generated with actual data values
// Generated on: 2026-01-08T13:16:56.454925
// Database: bolt://ccdi-federation-dev-603767cfd3c16603.elb.us-east-1.amazonaws.com:7687
// Database Name: memgraph
// ============================================================================
// ----------------------------------------------------------------------------
// STEP 1: GET TYPE COUNTS DATA
// ----------------------------------------------------------------------------

// Getting type counts (this may take 20-25 seconds)...
// Type Count Results:
//   Total: 1595223
//   Missing: 12439
//   Values: 23

// ----------------------------------------------------------------------------
// STEP 2: GET DEPOSITIONS COUNTS DATA
// ----------------------------------------------------------------------------

// Getting depositions counts (this may take 20-25 seconds)...
// Depositions Count Results:
//   Total: 1595223
//   Missing: 0
//   Values: 25

// ----------------------------------------------------------------------------
// STEP 3: CREATE TYPE COUNT MATERIALIZED VIEW
// ----------------------------------------------------------------------------

// 3.1: Create stats node
CREATE (stats:FileCountStats {
    type: "by_type",
    total: 1595223,
    missing: 12439,
    last_updated: timestamp(),
    version: 1
})
RETURN stats;

// 3.2: Verify stats node created
// MATCH (stats:FileCountStats {type: "by_type"})
// RETURN stats;

// 3.3: Create count nodes
MATCH (stats:FileCountStats {type: "by_type"})
UNWIND [
    {value: "VCF", count: 269005},
    {value: "TXT", count: 263659},
    {value: "FASTQ", count: 221994},
    {value: "TBI", count: 195686},
    {value: "MAF", count: 137818},
    {value: "TSV", count: 96754},
    {value: "PDF", count: 61036},
    {value: "SEG", count: 49405},
    {value: "CSV", count: 45501},
    {value: "PNG", count: 44342},
    {value: "CRAM", count: 37686},
    {value: "CRAI", count: 30203},
    {value: "BAM", count: 29480},
    {value: "CNS", count: 25427},
    {value: "HTML", count: 25131},
    {value: "JSON", count: 16620},
    {value: "PED", count: 12563},
    {value: "BAI", count: 9987},
    {value: "TAR", count: 7302},
    {value: "gVCF", count: 3064},
    {value: "rds", count: 86},
    {value: "HDF5", count: 20},
    {value: "XLSX", count: 15}
] AS count_data
CREATE (count:FileCount {
    field: "file_type",
    value: count_data.value,
    count: count_data.count,
    stats_type: "by_type"
})
CREATE (count)-[:BELONGS_TO]->(stats)
RETURN count.value, count.count;

// 3.4: Verify type count view
MATCH (stats:FileCountStats {type: "by_type"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)
WITH stats, collect(count) as counts
UNWIND counts as c
WITH stats, collect(DISTINCT c) as all_counts, sum(CASE WHEN c IS NOT NULL AND c.count IS NOT NULL THEN c.count ELSE 0 END) as sum_counts
WITH stats, all_counts, sum_counts
RETURN 
    stats.total as total,
    stats.missing as missing,
    size([x IN all_counts WHERE x IS NOT NULL]) as count_nodes,
    sum_counts,
    stats.total - stats.missing as expected_sum,
    CASE WHEN sum_counts = stats.total - stats.missing 
         THEN "✅ MATCH" 
         ELSE "❌ MISMATCH" 
    END as verification;

// ----------------------------------------------------------------------------
// STEP 4: CREATE DEPOSITIONS COUNT MATERIALIZED VIEW
// ----------------------------------------------------------------------------

// 4.1: Create stats node
CREATE (stats:FileCountStats {
    type: "by_depositions",
    total: 1595223,
    missing: 0,
    last_updated: timestamp(),
    version: 1
})
RETURN stats;

// 4.2: Verify stats node created
MATCH (stats:FileCountStats {type: "by_depositions"})
RETURN stats;

// 4.3: Create count nodes
MATCH (stats:FileCountStats {type: "by_depositions"})
UNWIND [
    {value: "phs002517", count: 758204},
    {value: "phs002276", count: 292842},
    {value: "phs002790", count: 153812},
    {value: "phs002883", count: 113236},
    {value: "phs001846", count: 90054},
    {value: "phs001228", count: 67131},
    {value: "phs001327", count: 35675},
    {value: "phs001714", count: 19248},
    {value: "phs002431", count: 13822},
    {value: "phs002322", count: 12812},
    {value: "phs001878", count: 8629},
    {value: "phs002529", count: 5411},
    {value: "phs001738", count: 5289},
    {value: "phs003519", count: 3644},
    {value: "phs002187", count: 3342},
    {value: "phs002430", count: 2387},
    {value: "phs002518", count: 2246},
    {value: "phs003111", count: 2080},
    {value: "phs003432", count: 1426},
    {value: "phs002504", count: 1283},
    {value: "phs000720", count: 1004},
    {value: "phs002620", count: 652},
    {value: "phs002371", count: 361},
    {value: "phs003215", count: 320},
    {value: "phs002599", count: 313}
] AS count_data
CREATE (count:FileCount {
    field: "study_id",
    value: count_data.value,
    count: count_data.count,
    stats_type: "by_depositions"
})
CREATE (count)-[:BELONGS_TO]->(stats)
RETURN count.value, count.count;

// 4.4: Verify depositions count view
MATCH (stats:FileCountStats {type: "by_depositions"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)
WITH stats, collect(count) as counts
UNWIND counts as c
WITH stats, collect(DISTINCT c) as all_counts, sum(CASE WHEN c IS NOT NULL AND c.count IS NOT NULL THEN c.count ELSE 0 END) as sum_counts
WITH stats, all_counts, sum_counts
RETURN 
    stats.total as total,
    stats.missing as missing,
    size([x IN all_counts WHERE x IS NOT NULL]) as count_nodes,
    sum_counts,
    stats.total - stats.missing as expected_sum,
    CASE WHEN sum_counts = stats.total - stats.missing 
         THEN "✅ MATCH" 
         ELSE "❌ MISMATCH" 
    END as verification;

// ----------------------------------------------------------------------------
// STEP 5: CREATE INDEXES (Recommended for performance)
// ----------------------------------------------------------------------------

CREATE INDEX ON :FileCountStats(type);
CREATE INDEX ON :FileCount(stats_type);
CREATE INDEX ON :FileCount(value);

// Verify indexes created
SHOW INDEXES;

// ----------------------------------------------------------------------------
// STEP 6: TEST QUERY PERFORMANCE
// ----------------------------------------------------------------------------

// Test type count query (should be <100ms)
PROFILE
MATCH (stats:FileCountStats {type: "by_type"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)
WITH stats, collect(count) as counts
RETURN 
    stats.total as total,
    stats.missing as missing,
    [c IN counts WHERE c IS NOT NULL | {
        value: c.value,
        count: c.count
    }] as values;

// Test depositions count query (should be <100ms)
PROFILE
MATCH (stats:FileCountStats {type: "by_depositions"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)
WITH stats, collect(count) as counts
RETURN 
    stats.total as total,
    stats.missing as missing,
    [c IN counts WHERE c IS NOT NULL | {
        value: c.value,
        count: c.count
    }] as values;

// ----------------------------------------------------------------------------
// SUMMARY
// ----------------------------------------------------------------------------
// Type counts: Total=1595223, Missing=12439, Values=23
// Depositions counts: Total=1595223, Missing=0, Values=25
//
// This script is ready to run in Memgraph console
// Run each section separately and verify before proceeding

