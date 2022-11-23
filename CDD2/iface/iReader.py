from abc import ABC, abstractmethod

class iReader(ABC):
    @abstractmethod
    def read(self) -> bool:
        pass