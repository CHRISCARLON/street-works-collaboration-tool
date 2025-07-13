from abc import ABC, abstractmethod
from ..schemas.schemas import Project, ImpactScore


class MetricCalculationStrategy(ABC):
    """Abstract base class for metric calculation strategies"""

    @abstractmethod
    def calculate_and_store(self, project: Project) -> ImpactScore:
        """Calculate the metric and return an ImpactScore object"""
        pass

    @abstractmethod
    def validate_project_data(self, project: Project) -> bool:
        """Validate that project has required data for this metric"""
        pass
