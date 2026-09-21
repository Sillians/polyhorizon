from abc import ABC, abstractmethod
from feast import FeatureView

from polyhorizon.features.configs.settings import Config


class BaseFeatureDefinition(ABC):
    """Abstract base class for all FeatureView definitions."""

    def __init__(self, config: Config) -> None:
        self.config = config

    @abstractmethod
    def build_feature_view(self) -> FeatureView:
        """Return a Feast FeatureView object."""
        raise NotImplementedError
