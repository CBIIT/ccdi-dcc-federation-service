"""
Integration test fixtures for repository tests.

Provides database containers and session management for integration tests.
"""

import pytest
from typing import AsyncGenerator
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from testcontainers.neo4j import Neo4jContainer

from app.lib.field_allowlist import FieldAllowlist
from app.core.config import Settings
from app.repositories.subject import SubjectRepository
from app.repositories.sample import SampleRepository
from app.repositories.file import FileRepository


@pytest.fixture(scope="session")
def neo4j_container():
    """Create a Neo4j container for integration tests."""
    # Neo4j container is compatible with Memgraph for testing
    with Neo4jContainer("neo4j:5.15") as container:
        yield container


@pytest.fixture(scope="session")
def neo4j_driver(neo4j_container, event_loop):
    """Create an async Neo4j driver for integration tests."""
    import asyncio
    uri = neo4j_container.get_connection_url()
    driver = AsyncGraphDatabase.driver(uri, auth=("neo4j", neo4j_container.password))
    
    async def setup():
        await driver.verify_connectivity()
        return driver
    
    async def teardown(driver):
        await driver.close()
    
    # Run setup in event loop
    driver = event_loop.run_until_complete(setup())
    
    yield driver
    
    # Run teardown in event loop
    event_loop.run_until_complete(teardown(driver))


@pytest.fixture
async def db_session(neo4j_driver: AsyncDriver) -> AsyncGenerator[AsyncSession, None]:
    """Create a database session for integration tests."""
    async with neo4j_driver.session() as session:
        yield session


@pytest.fixture
def test_settings() -> Settings:
    """Create test settings."""
    return Settings(
        memgraph_host="localhost",
        memgraph_port=7687,
        memgraph_user="neo4j",
        memgraph_password="password",
        identifier_server_url="https://test.example.com"
    )


@pytest.fixture
def test_allowlist() -> FieldAllowlist:
    """Create a test field allowlist."""
    allowlist = FieldAllowlist()
    # Allow all fields for testing
    allowlist._loaded = True
    return allowlist


@pytest.fixture
def subject_repository(db_session, test_allowlist, test_settings) -> SubjectRepository:
    """Create a SubjectRepository for integration tests."""
    return SubjectRepository(db_session, test_allowlist, test_settings)


@pytest.fixture
def sample_repository(db_session, test_allowlist, test_settings) -> SampleRepository:
    """Create a SampleRepository for integration tests."""
    return SampleRepository(db_session, test_allowlist, test_settings)


@pytest.fixture
def file_repository(db_session, test_allowlist) -> FileRepository:
    """Create a FileRepository for integration tests."""
    return FileRepository(db_session, test_allowlist)


@pytest.fixture
async def test_data_setup(db_session: AsyncSession):
    """Set up comprehensive test data in the database."""
    # Create test studies first
    result1 = await db_session.run("""
        CREATE (s:study {
            study_id: 'phs002431',
            study_name: 'Test Study',
            study_description: 'Test Study Description',
            study_acronym: 'TS',
            study_dd: 'phs002431'
        })
        RETURN s
    """)
    await result1.consume()
    
    result2 = await db_session.run("""
        CREATE (s:study {
            study_id: 'phs002432',
            study_name: 'Test Study 2',
            study_description: 'Test Study Description 2',
            study_acronym: 'TS2',
            study_dd: 'phs002432'
        })
        RETURN s
    """)
    await result2.consume()
    
    # Create study_funding nodes
    result3 = await db_session.run("""
        MATCH (s:study {study_id: 'phs002431'})
        CREATE (sf:study_funding {
            grant_id: 'R01CA123456'
        })
        CREATE (s)-[:HAS_FUNDING]->(sf)
        RETURN sf
    """)
    await result3.consume()
    
    # Create participants with proper relationships
    # Use sex_at_birth instead of sex (based on repository code)
    result4 = await db_session.run("""
        MATCH (s:study {study_id: 'phs002431'})
        CREATE (p:participant {
            participant_id: 'TEST-001',
            sex_at_birth: 'Female',
            race: ['White'],
            ethnicity: 'Not Hispanic or Latino',
            vital_status: 'Alive',
            age_at_vital_status: 45
        })
        CREATE (p)-[:IN_STUDY]->(s)
        RETURN p
    """)
    await result4.consume()
    
    result5 = await db_session.run("""
        MATCH (s:study {study_id: 'phs002431'})
        CREATE (p:participant {
            participant_id: 'TEST-002',
            sex_at_birth: 'Male',
            race: ['Black or African American', 'White'],
            ethnicity: 'Hispanic or Latino',
            vital_status: 'Dead',
            age_at_vital_status: 60
        })
        CREATE (p)-[:IN_STUDY]->(s)
        RETURN p
    """)
    await result5.consume()
    
    result6 = await db_session.run("""
        MATCH (s:study {study_id: 'phs002432'})
        CREATE (p:participant {
            participant_id: 'TEST-003',
            sex_at_birth: 'Female',
            race: ['Asian'],
            ethnicity: 'Not Hispanic or Latino',
            vital_status: 'Alive',
            age_at_vital_status: 30
        })
        CREATE (p)-[:IN_STUDY]->(s)
        RETURN p
    """)
    await result6.consume()
    
    # Create diagnosis nodes
    result7 = await db_session.run("""
        MATCH (p:participant {participant_id: 'TEST-001'})
        CREATE (d:diagnosis {
            diagnosis: 'Neuroblastoma'
        })
        CREATE (p)-[:HAS_DIAGNOSIS]->(d)
        RETURN d
    """)
    await result7.consume()
    
    result8 = await db_session.run("""
        MATCH (p:participant {participant_id: 'TEST-002'})
        CREATE (d:diagnosis {
            diagnosis: 'Acute Lymphoblastic Leukemia'
        })
        CREATE (p)-[:HAS_DIAGNOSIS]->(d)
        RETURN d
    """)
    await result8.consume()
    
    # Create samples
    result9 = await db_session.run("""
        MATCH (p:participant {participant_id: 'TEST-001'}), (s:study {study_id: 'phs002431'})
        CREATE (sa:sample {
            sample_id: 'SAMPLE-001',
            sample_tumor_status: 'Tumor',
            diagnosis: 'Neuroblastoma',
            disease_phase: 'Initial Diagnosis',
            anatomical_sites: ['C71.9 : Brain, NOS'],
            library_strategy: 'WXS',
            library_source_material: 'Genomic DNA'
        })
        CREATE (sa)-[:FROM_PARTICIPANT]->(p)
        CREATE (sa)-[:FROM_STUDY]->(s)
        RETURN sa
    """)
    await result9.consume()
    
    result10 = await db_session.run("""
        MATCH (p:participant {participant_id: 'TEST-002'}), (s:study {study_id: 'phs002431'})
        CREATE (sa:sample {
            sample_id: 'SAMPLE-002',
            sample_tumor_status: 'Normal',
            diagnosis: 'Acute Lymphoblastic Leukemia',
            disease_phase: 'Relapse',
            anatomical_sites: ['C42.1 : Bone Marrow'],
            library_strategy: 'RNA-Seq',
            library_source_material: 'RNA'
        })
        CREATE (sa)-[:FROM_PARTICIPANT]->(p)
        CREATE (sa)-[:FROM_STUDY]->(s)
        RETURN sa
    """)
    await result10.consume()
    
    result11 = await db_session.run("""
        MATCH (p:participant {participant_id: 'TEST-003'}), (s:study {study_id: 'phs002432'})
        CREATE (sa:sample {
            sample_id: 'SAMPLE-003',
            sample_tumor_status: 'Tumor',
            diagnosis: 'Neuroblastoma',
            disease_phase: 'Initial Diagnosis',
            anatomical_sites: ['C71.9 : Brain, NOS'],
            library_strategy: 'WGS',
            library_source_material: 'Genomic DNA'
        })
        CREATE (sa)-[:FROM_PARTICIPANT]->(p)
        CREATE (sa)-[:FROM_STUDY]->(s)
        RETURN sa
    """)
    await result11.consume()
    
    # Create files
    result12 = await db_session.run("""
        MATCH (sa:sample {sample_id: 'SAMPLE-001'}), (s:study {study_id: 'phs002431'})
        CREATE (f:file {
            file_id: 'FILE-001',
            file_name: 'test1.bam',
            file_type: 'BAM',
            file_size: 1000000,
            md5sum: 'abc123def456'
        })
        CREATE (f)-[:FROM_SAMPLE]->(sa)
        CREATE (f)-[:FROM_STUDY]->(s)
        RETURN f
    """)
    await result12.consume()
    
    result13 = await db_session.run("""
        MATCH (sa:sample {sample_id: 'SAMPLE-002'}), (s:study {study_id: 'phs002431'})
        CREATE (f:file {
            file_id: 'FILE-002',
            file_name: 'test2.fastq.gz',
            file_type: 'FASTQ',
            file_size: 2000000,
            md5sum: 'def456ghi789'
        })
        CREATE (f)-[:FROM_SAMPLE]->(sa)
        CREATE (f)-[:FROM_STUDY]->(s)
        RETURN f
    """)
    await result13.consume()
    
    result14 = await db_session.run("""
        MATCH (sa:sample {sample_id: 'SAMPLE-003'}), (s:study {study_id: 'phs002432'})
        CREATE (f:file {
            file_id: 'FILE-003',
            file_name: 'test3.vcf.gz',
            file_type: 'VCF',
            file_size: 500000,
            md5sum: 'ghi789jkl012'
        })
        CREATE (f)-[:FROM_SAMPLE]->(sa)
        CREATE (f)-[:FROM_STUDY]->(s)
        RETURN f
    """)
    await result14.consume()
    
    yield
    
    # Cleanup
    cleanup_result = await db_session.run("MATCH (n) DETACH DELETE n")
    await cleanup_result.consume()

