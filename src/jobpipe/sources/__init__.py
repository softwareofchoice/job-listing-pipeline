from .ashby import Ashby
from .base import Source
from .greenhouse import Greenhouse
from .lever import Lever

SOURCES: dict[str, Source] = {s.name: s for s in (Greenhouse(), Lever(), Ashby())}

__all__ = ["SOURCES", "Source"]
