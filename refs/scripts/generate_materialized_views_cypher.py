#!/usr/bin/env python3
"""
Generate Cypher script with actual data values for materialized views.

This script:
1. Runs the count queries to get actual data
2. Generates Cypher statements with real values
3. Outputs a ready-to-use Cypher script

Usage:
    # Use default database (from environment variables)
    python scripts/generate_materialized_views_cypher.py > materialized_views_generated.cypher
    
    # Specify database instance via environment variables
    MEMGRAPH_URI=bolt://your-host:7687 MEMGRAPH_USER=memgraph MEMGRAPH_PASSWORD=password \
        python scripts/generate_materialized_views_cypher.py > materialized_views_generated.cypher
    
    # Or use command-line arguments
    python scripts/generate_materialized_views_cypher.py \
        --uri bolt://your-host:7687 \
        --user memgraph \
        --password password \
        > materialized_views_generated.cypher
"""

import asyncio
import sys
import os
import json
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.memgraph import get_session
from app.repositories.file import FileRepository
from app.lib.field_allowlist import get_field_allowlist
from app.core.config import Settings


def generate_unwind_list(values_data):
    """Generate UNWIND list format from values data."""
    items = []
    for item in values_data:
        value = item['value'].replace('"', '\\"')  # Escape quotes
        count = item['count']
        items.append(f'    {{value: "{value}", count: {count}}}')
    return ',\n'.join(items)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate materialized views Cypher script with actual data from database'
    )
    parser.add_argument(
        '--uri',
        type=str,
        default=None,
        help='Memgraph URI (e.g., bolt://localhost:7687). Overrides MEMGRAPH_URI env var.'
    )
    parser.add_argument(
        '--user',
        type=str,
        default=None,
        help='Memgraph username. Overrides MEMGRAPH_USER env var.'
    )
    parser.add_argument(
        '--password',
        type=str,
        default=None,
        help='Memgraph password. Overrides MEMGRAPH_PASSWORD env var.'
    )
    parser.add_argument(
        '--database',
        type=str,
        default=None,
        help='Memgraph database name. Overrides MEMGRAPH_DATABASE env var.'
    )
    return parser.parse_args()


async def generate_cypher_script(uri=None, user=None, password=None, database=None):
    """Generate Cypher script with actual data."""
    
    # Override environment variables if command-line args provided
    if uri:
        os.environ['MEMGRAPH_URI'] = uri
    if user:
        os.environ['MEMGRAPH_USER'] = user
    if password:
        os.environ['MEMGRAPH_PASSWORD'] = password
    if database:
        os.environ['MEMGRAPH_DATABASE'] = database
    
    # Clear Settings cache to reload with new environment variables
    # get_settings() uses @lru_cache, so we need to clear it
    from app.core.config import get_settings
    try:
        get_settings.cache_clear()
    except AttributeError:
        pass
    
    # Get settings (will use environment variables or command-line args)
    # Create new Settings instance to pick up environment variables
    settings = Settings()
    
    print("// ============================================================================")
    print("// GENERATED MATERIALIZED VIEWS CREATION SCRIPT")
    print("// ============================================================================")
    print("// This script was auto-generated with actual data values")
    print("// Generated on:", __import__('datetime').datetime.now().isoformat())
    print(f"// Database: {settings.memgraph_uri}")
    print(f"// Database Name: {settings.memgraph_database}")
    print("// ============================================================================")
    print()
    
    async for session in get_session():
        try:
            allowlist = get_field_allowlist()
            repo = FileRepository(session, allowlist)
            
            # Get type counts
            print("// ----------------------------------------------------------------------------")
            print("// STEP 1: GET TYPE COUNTS DATA")
            print("// ----------------------------------------------------------------------------")
            print()
            print("// Getting type counts (this may take 20-25 seconds)...")
            print("// NOTE: Queries use WITH DISTINCT sf, coalesce(st1, st2) AS st to ensure")
            print("//       consistent counting (each file counted once, picking one study via coalesce)")
            print("//       This ensures sum(values) + missing = total")
            print()
            
            type_result = await repo.count_files_by_field("type", {})
            type_total = type_result.get('total', 0)
            type_missing = type_result.get('missing', 0)
            type_values = type_result.get('values', [])
            
            print(f"// Type Count Results:")
            print(f"//   Total: {type_total}")
            print(f"//   Missing: {type_missing}")
            print(f"//   Values: {len(type_values)}")
            print()
            
            # Get depositions counts
            print("// ----------------------------------------------------------------------------")
            print("// STEP 2: GET DEPOSITIONS COUNTS DATA")
            print("// ----------------------------------------------------------------------------")
            print()
            print("// Getting depositions counts (this may take 20-25 seconds)...")
            print("// NOTE: Queries use WITH DISTINCT sf, coalesce(st1, st2) AS st to ensure")
            print("//       consistent counting (each file counted once, picking one study via coalesce)")
            print("//       This ensures sum(values) + missing = total")
            print()
            
            dep_result = await repo.count_files_by_field("depositions", {})
            dep_total = dep_result.get('total', 0)
            dep_missing = dep_result.get('missing', 0)
            dep_values = dep_result.get('values', [])
            
            print(f"// Depositions Count Results:")
            print(f"//   Total: {dep_total}")
            print(f"//   Missing: {dep_missing}")
            print(f"//   Values: {len(dep_values)}")
            print()
            
            # Generate type count creation script
            print("// ----------------------------------------------------------------------------")
            print("// STEP 3: CREATE TYPE COUNT MATERIALIZED VIEW")
            print("// ----------------------------------------------------------------------------")
            print()
            print("// 3.1: Create stats node")
            print(f'CREATE (stats:FileCountStats {{')
            print(f'    type: "by_type",')
            print(f'    total: {type_total},')
            print(f'    missing: {type_missing},')
            print(f'    last_updated: timestamp(),')
            print(f'    version: 1')
            print(f'}})')
            print(f'RETURN stats;')
            print()
            print("// 3.2: Verify stats node created")
            print('MATCH (stats:FileCountStats {type: "by_type"})')
            print('RETURN stats;')
            print()
            print("// 3.3: Create count nodes")
            print('MATCH (stats:FileCountStats {type: "by_type"})')
            print('UNWIND [')
            print(generate_unwind_list(type_values))
            print('] AS count_data')
            print('CREATE (count:FileCount {')
            print('    field: "file_type",')
            print('    value: count_data.value,')
            print('    count: count_data.count,')
            print('    stats_type: "by_type"')
            print('})')
            print('CREATE (count)-[:BELONGS_TO]->(stats)')
            print('RETURN count.value, count.count;')
            print()
            print("// 3.4: Verify type count view")
            print('MATCH (stats:FileCountStats {type: "by_type"})')
            print('OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)')
            print('WITH stats, collect(count) as counts')
            print('UNWIND counts as c')
            print('WITH stats, collect(DISTINCT c) as all_counts, sum(CASE WHEN c IS NOT NULL AND c.count IS NOT NULL THEN c.count ELSE 0 END) as sum_counts')
            print('WITH stats, all_counts, sum_counts')
            print('RETURN ')
            print('    stats.total as total,')
            print('    stats.missing as missing,')
            print('    size([x IN all_counts WHERE x IS NOT NULL]) as count_nodes,')
            print('    sum_counts,')
            print('    stats.total - stats.missing as expected_sum,')
            print('    CASE WHEN sum_counts = stats.total - stats.missing ')
            print('         THEN "✅ MATCH" ')
            print('         ELSE "❌ MISMATCH" ')
            print('    END as verification;')
            print()
            
            # Generate depositions count creation script
            print("// ----------------------------------------------------------------------------")
            print("// STEP 4: CREATE DEPOSITIONS COUNT MATERIALIZED VIEW")
            print("// ----------------------------------------------------------------------------")
            print()
            print("// 4.1: Create stats node")
            print(f'CREATE (stats:FileCountStats {{')
            print(f'    type: "by_depositions",')
            print(f'    total: {dep_total},')
            print(f'    missing: {dep_missing},')
            print(f'    last_updated: timestamp(),')
            print(f'    version: 1')
            print(f'}})')
            print(f'RETURN stats;')
            print()
            print("// 4.2: Verify stats node created")
            print('MATCH (stats:FileCountStats {type: "by_depositions"})')
            print('RETURN stats;')
            print()
            print("// 4.3: Create count nodes")
            print('MATCH (stats:FileCountStats {type: "by_depositions"})')
            print('UNWIND [')
            print(generate_unwind_list(dep_values))
            print('] AS count_data')
            print('CREATE (count:FileCount {')
            print('    field: "study_id",')
            print('    value: count_data.value,')
            print('    count: count_data.count,')
            print('    stats_type: "by_depositions"')
            print('})')
            print('CREATE (count)-[:BELONGS_TO]->(stats)')
            print('RETURN count.value, count.count;')
            print()
            print("// 4.4: Verify depositions count view")
            print('MATCH (stats:FileCountStats {type: "by_depositions"})')
            print('OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)')
            print('WITH stats, collect(count) as counts')
            print('UNWIND counts as c')
            print('WITH stats, collect(DISTINCT c) as all_counts, sum(CASE WHEN c IS NOT NULL AND c.count IS NOT NULL THEN c.count ELSE 0 END) as sum_counts')
            print('WITH stats, all_counts, sum_counts')
            print('RETURN ')
            print('    stats.total as total,')
            print('    stats.missing as missing,')
            print('    size([x IN all_counts WHERE x IS NOT NULL]) as count_nodes,')
            print('    sum_counts,')
            print('    stats.total - stats.missing as expected_sum,')
            print('    CASE WHEN sum_counts = stats.total - stats.missing ')
            print('         THEN "✅ MATCH" ')
            print('         ELSE "❌ MISMATCH" ')
            print('    END as verification;')
            print()
            
            # Generate indexes
            print("// ----------------------------------------------------------------------------")
            print("// STEP 5: CREATE INDEXES (Recommended for performance)")
            print("// ----------------------------------------------------------------------------")
            print()
            print('CREATE INDEX ON :FileCountStats(type);')
            print('CREATE INDEX ON :FileCount(stats_type);')
            print('CREATE INDEX ON :FileCount(value);')
            print()
            print('// Verify indexes created')
            print('SHOW INDEXES;')
            print()
            
            # Generate test queries
            print("// ----------------------------------------------------------------------------")
            print("// STEP 6: TEST QUERY PERFORMANCE")
            print("// ----------------------------------------------------------------------------")
            print()
            print('// Test type count query (should be <100ms)')
            print('PROFILE')
            print('MATCH (stats:FileCountStats {type: "by_type"})')
            print('OPTIONAL MATCH (count:FileCount {stats_type: "by_type"})-[:BELONGS_TO]->(stats)')
            print('WITH stats, collect(count) as counts')
            print('RETURN ')
            print('    stats.total as total,')
            print('    stats.missing as missing,')
            print('    [c IN counts WHERE c IS NOT NULL | {')
            print('        value: c.value,')
            print('        count: c.count')
            print('    }] as values;')
            print()
            print('// Test depositions count query (should be <100ms)')
            print('PROFILE')
            print('MATCH (stats:FileCountStats {type: "by_depositions"})')
            print('OPTIONAL MATCH (count:FileCount {stats_type: "by_depositions"})-[:BELONGS_TO]->(stats)')
            print('WITH stats, collect(count) as counts')
            print('RETURN ')
            print('    stats.total as total,')
            print('    stats.missing as missing,')
            print('    [c IN counts WHERE c IS NOT NULL | {')
            print('        value: c.value,')
            print('        count: c.count')
            print('    }] as values;')
            print()
            
            # Summary
            print("// ----------------------------------------------------------------------------")
            print("// SUMMARY")
            print("// ----------------------------------------------------------------------------")
            print(f"// Type counts: Total={type_total}, Missing={type_missing}, Values={len(type_values)}")
            print(f"// Depositions counts: Total={dep_total}, Missing={dep_missing}, Values={len(dep_values)}")
            print("//")
            print("// This script is ready to run in Memgraph console")
            print("// Run each section separately and verify before proceeding")
            print()
            
        except Exception as e:
            print(f"// ERROR: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        break  # Only use first session


if __name__ == "__main__":
    args = parse_args()
    
    print("// Generating materialized views Cypher script with actual data...", file=sys.stderr)
    if args.uri:
        print(f"// Connecting to: {args.uri}", file=sys.stderr)
    elif os.getenv('MEMGRAPH_URI'):
        print(f"// Connecting to: {os.getenv('MEMGRAPH_URI')}", file=sys.stderr)
    else:
        print("// Using default database connection settings", file=sys.stderr)
    print("// This may take 40-50 seconds (running count queries)...", file=sys.stderr)
    print()
    
    asyncio.run(generate_cypher_script(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database
    ))

