from typing import Generator, TypeAlias, TypeVar

from followthemoney import Statement, StatementEntity, ValueEntity

# property multi-value
Value: TypeAlias = list[str]
"""FtM property value is always multi-valued string"""

Entity = TypeVar("Entity", StatementEntity, ValueEntity)
"""Generic type used mostly in ftmq"""

# entity generators
Entities: TypeAlias = Generator[Entity, None, None]
"""A generator for generic entity type"""
StatementEntities: TypeAlias = Generator[StatementEntity, None, None]
"""A generator for StatementEntity instances"""
ValueEntities: TypeAlias = Generator[ValueEntity, None, None]
"""A generator for ValueEntity instances"""

# statements
Statements: TypeAlias = Generator[Statement, None, None]
"""A generator for Statement instances"""
