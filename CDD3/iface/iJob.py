
from abc import ABC, abstractmethod

from CDD3.impl.DefaultJob import DefaultJob


class iJob(ABC):
    @abstractmethod
    def execute(self) -> bool:
        return DefaultJob()