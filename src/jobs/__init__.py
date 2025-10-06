"""ETL job definitions"""

from jobs.base_job import ETLJob, JobStatus, JobResult, JobError
from jobs.building_permits_job import BuildingPermitsETLJob

__all__ = [
    "ETLJob",
    "JobStatus",
    "JobResult",
    "JobError",
    "BuildingPermitsETLJob",
]