// ============================================================================
// DROP MATERIALIZED VIEWS SCRIPT
// ============================================================================
// This script safely drops all materialized view nodes and relationships
// for file count endpoints.
// ============================================================================

// STEP 1: Drop file count by type materialized view
// ----------------------------------------------------------------------------
MATCH (stats:FileCountStats {type: "by_type"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)
DETACH DELETE stats, count
RETURN 
    "Dropped file count by type view" as action,
    count(stats) as stats_nodes_deleted,
    count(count) as count_nodes_deleted;

// STEP 2: Drop file count by depositions materialized view
// ----------------------------------------------------------------------------
MATCH (stats:FileCountStats {type: "by_depositions"})
OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)
DETACH DELETE stats, count
RETURN 
    "Dropped file count by depositions view" as action,
    count(stats) as stats_nodes_deleted,
    count(count) as count_nodes_deleted;

// STEP 3: Verify all materialized views are dropped
// ----------------------------------------------------------------------------
MATCH (stats:FileCountStats)
RETURN 
    "Remaining FileCountStats nodes" as check,
    count(stats) as count,
    collect(stats.type) as types;

MATCH (count:FileCount)
RETURN 
    "Remaining FileCount nodes" as check,
    count(count) as count,
    collect(DISTINCT count.stats_type) as stats_types;

// ============================================================================
// DROP COMPLETE
// ============================================================================
// If the above queries return 0 for all counts, the materialized views
// have been successfully dropped.
// ============================================================================

