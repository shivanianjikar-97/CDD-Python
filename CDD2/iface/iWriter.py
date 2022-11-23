from abc import ABC, abstractmethod

class iWriter(ABC):
    @abstractmethod
    def write(self) -> bool:
        pass
