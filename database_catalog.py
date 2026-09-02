"""Production RDS databases exported by the backup runner."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Database:
    key: str
    name: str
    snapshot_type: str
    source_id: str
    default_schema: str


DATABASES = (
    Database(
        key="aisl",
        name="AI Shipping Labs",
        snapshot_type="instance",
        source_id="ai-shipping-labs",
        default_schema="aisl_prod",
    ),
    Database(
        key="cmp",
        name="Course Management",
        snapshot_type="cluster",
        source_id="course-management-manual",
        default_schema="prod",
    ),
    Database(
        key="website",
        name="DataTalks.Club website",
        snapshot_type="instance",
        source_id="website-production",
        default_schema="dtc_website",
    ),
    Database(
        key="relay",
        name="DataTalks.Club Relay",
        snapshot_type="instance",
        source_id="relay-production",
        default_schema="relay",
    ),
)

DATABASES_BY_KEY = {database.key: database for database in DATABASES}
