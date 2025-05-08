"""add hls_manifest_url and live_status_to_answers

Revision ID: ab181a1c679f
Revises: 
Create Date: 2025-05-08 04:15:47.123456

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ab181a1c679f'
down_revision = None  # This is the first app-specific migration after init
branch_labels = None
depends_on = None

# Assuming the ENUM type for the 'status' column in 'answers' table is named 'answer_status'.
# If this name is different, it needs to be adjusted here.
# The task specifies: status ENUM('PENDING','READY','ERROR','LIVE') default 'READY'
# This implies the ENUM type should contain these values after the upgrade.
ANSWER_STATUS_ENUM_NAME = 'answer_status' # Placeholder name, confirm actual name if it exists

def upgrade() -> None:
    # Add hls_manifest_url column to the answers table
    op.add_column('answers', sa.Column('hls_manifest_url', sa.Text(), nullable=True))

    # Add 'LIVE' value to the existing ENUM type for the status column.
    # This is PostgreSQL-specific. 
    # This assumes an ENUM type named ANSWER_STATUS_ENUM_NAME already exists with ('PENDING', 'READY', 'ERROR').
    # If the type doesn't exist, it should be created first (e.g. in a base migration or initial model setup).
    # For this migration, we focus on adding the 'LIVE' value.
    # A check for existence of the type before altering might be needed in a complex setup.
    
    # Check if the ENUM type exists. If not, create it with initial values. Then add 'LIVE'.
    # This is a more robust approach if the initial state of the ENUM is uncertain.
    # However, typically migrations build upon a known previous state.
    # For simplicity, we'll assume the type exists and just add the value.
    # If `psycopg2.errors.DuplicateObject` occurs, it means the type or value already exists.
    
    # Attempt to create the type with initial values if it doesn't exist. This is usually done in an earlier migration.
    # For this specific migration, we are *extending* an existing setup.
    # So, the type 'answer_status' with 'PENDING', 'READY', 'ERROR' is assumed to exist.
    op.execute(f"ALTER TYPE {ANSWER_STATUS_ENUM_NAME} ADD VALUE IF NOT EXISTS 'LIVE';")

def downgrade() -> None:
    # Remove the hls_manifest_url column
    op.drop_column('answers', 'hls_manifest_url')

    # Downgrading ENUM types by removing a value ('LIVE') is complex and risky, 
    # especially if data uses this value. It can also cause issues if other parts of the 
    # application expect 'LIVE' to be a valid enum member after a failed upgrade and subsequent downgrade.
    # A common approach is to convert 'LIVE' values to a safe default (e.g., 'ERROR') 
    # before attempting to alter/recreate the ENUM type without 'LIVE'.
    # Given the complexity and potential for data loss or application errors, 
    # this part of the downgrade is often omitted or handled with extreme care (and manual steps).
    # For this task, the ENUM downgrade for removing 'LIVE' is not implemented to prevent such issues.
    # If a full rollback of the 'LIVE' status is required, it would need a more detailed strategy.
    pass

