"""Initial schema: building_permits, schema_metadata, job_executions

Revision ID: 001_initial_schema
Revises: 
Create Date: 2024-10-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create building_permits table
    op.create_table(
        'building_permits',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('permit_number', sa.String(length=50), nullable=False),
        sa.Column('application_date', sa.Date(), nullable=True),
        sa.Column('issued_date', sa.Date(), nullable=True),
        sa.Column('permit_type', sa.String(length=100), nullable=True),
        sa.Column('work_description', sa.String(length=500), nullable=True),
        sa.Column('street_number', sa.String(length=20), nullable=True),
        sa.Column('street_name', sa.String(length=200), nullable=True),
        sa.Column('postal_code', sa.String(length=10), nullable=True),
        sa.Column('ward', sa.String(length=50), nullable=True),
        sa.Column('estimated_cost', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('permit_number')
    )
    
    # Create indexes for building_permits
    op.create_index('ix_building_permits_permit_number', 'building_permits', ['permit_number'])
    op.create_index('ix_building_permits_issued_date', 'building_permits', ['issued_date'])
    op.create_index('ix_building_permits_permit_type', 'building_permits', ['permit_type'])
    op.create_index('ix_building_permits_postal_code', 'building_permits', ['postal_code'])
    op.create_index('ix_building_permits_ward', 'building_permits', ['ward'])
    op.create_index('idx_issued_date_permit_type', 'building_permits', ['issued_date', 'permit_type'])
    op.create_index('idx_ward_issued_date', 'building_permits', ['ward', 'issued_date'])
    
    # Create schema_metadata table
    op.create_table(
        'schema_metadata',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('table_name', sa.String(length=100), nullable=False),
        sa.Column('column_name', sa.String(length=100), nullable=False),
        sa.Column('data_type', sa.String(length=50), nullable=False),
        sa.Column('discovered_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('is_active', sa.String(length=10), nullable=False, server_default='active'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('table_name', 'column_name', name='uq_table_column')
    )
    
    op.create_index('ix_schema_metadata_table_name', 'schema_metadata', ['table_name'])
    
    # Create job_executions table
    op.create_table(
        'job_executions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('job_name', sa.String(length=100), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.Column('records_processed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('records_failed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('metadata', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('ix_job_executions_job_name', 'job_executions', ['job_name'])
    op.create_index('ix_job_executions_status', 'job_executions', ['status'])
    op.create_index('ix_job_executions_start_time', 'job_executions', ['start_time'])


def downgrade() -> None:
    op.drop_index('ix_job_executions_start_time', table_name='job_executions')
    op.drop_index('ix_job_executions_status', table_name='job_executions')
    op.drop_index('ix_job_executions_job_name', table_name='job_executions')
    op.drop_table('job_executions')
    
    op.drop_index('ix_schema_metadata_table_name', table_name='schema_metadata')
    op.drop_table('schema_metadata')
    
    op.drop_index('idx_ward_issued_date', table_name='building_permits')
    op.drop_index('idx_issued_date_permit_type', table_name='building_permits')
    op.drop_index('ix_building_permits_ward', table_name='building_permits')
    op.drop_index('ix_building_permits_postal_code', table_name='building_permits')
    op.drop_index('ix_building_permits_permit_type', table_name='building_permits')
    op.drop_index('ix_building_permits_issued_date', table_name='building_permits')
    op.drop_index('ix_building_permits_permit_number', table_name='building_permits')
    op.drop_table('building_permits')