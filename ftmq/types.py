import os
from collections.abc import Generator
from pathlib import Path
from typing import Literal, TypeAlias, TypeVar

from anystore.types import BytesGenerator, SDict, SDictGenerator, StrGenerator
from nomenklatura.entity import CE
from nomenklatura.statement.statement import Statement
from nomenklatura.stream import StreamEntity

from ftmq import enums

# property multi-value
Value: TypeAlias = list[str]

SE = TypeVar("SE", bound=StreamEntity)
Proxy: TypeAlias = SE | CE

# entity generators
CEGenerator: TypeAlias = Generator[CE, None, None]
SEGenerator: TypeAlias = Generator[SE, None, None]
ProxyGenerator: TypeAlias = Generator[Proxy, None, None]

# statement generator
SGenerator: TypeAlias = Generator[Statement, None, None]

Schemata: TypeAlias = Literal[tuple(s.name for s in enums.Schemata)]
Properties: TypeAlias = Literal[tuple(p.name for p in enums.Properties)]
Frequencies: TypeAlias = Literal[tuple(f.name for f in enums.Frequencies)]

PathLike: TypeAlias = str | os.PathLike[str] | Path

__all__ = [
    "BytesGenerator",
    "CE",
    "SE",
    "Proxy",
    "CEGenerator",
    "SEGenerator",
    "ProxyGenerator",
    "Frequencies",
    "PathLike",
    "Properties",
    "Schemata",
    "SDict",
    "SGenerator",
    "SDictGenerator",
    "StrGenerator",
    "Value",
]
