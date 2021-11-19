import dataclasses
from typing import NamedTuple


class Address(NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


class ClusterNode(NamedTuple):
    addr: Address
    node_id: str


@dataclasses.dataclass
class ClusterSlot:
    begin: int
    end: int
    master: ClusterNode

    def in_range(self, slot: int) -> bool:
        return self.begin <= slot <= self.end
