from abc import ABC, abstractmethod

class tableWriter(ABC):
    @abstractmethod
    def write(self) -> bool:
        pass
