from abc import ABC, abstractmethod, property
from collections.abc import Iterator, Callable
class Artifacts(ABC):
    @property
    @abstractmethod
    def actions(self)-> list[Callable]:
        # what should be the callable parameter be????
        pass

    def generate_artifacts(self):
        res = []
        for action in self.actions:
            res.append(action())
        return res
class RandomForestArtifact(Artifacts):
    def __init__(self, clf):
        pass
    def actions(self):
        return []