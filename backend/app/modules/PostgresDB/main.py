import os
import asyncpg
from typing import Any, Optional

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")


class PostgresDBClient:
    def __init__(
        self,
        host: str,
        port: int = 5432,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        database: str = POSTGRES_DB,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=1,
                max_size=10,
            )

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def run_query(
        self, query: str, *args: Any
    ) -> list[dict[str, Any]] | str | None:
        """
        Execute a query and return results.

        For SELECT queries, returns a list of dictionaries.
        For INSERT/UPDATE/DELETE, returns the status string.
        """
        if self._pool is None:
            await self.connect()

        try:
            async with self._pool.acquire() as conn:
                # Check if it's a SELECT query
                if query.strip().upper().startswith("SELECT"):
                    rows = await conn.fetch(query, *args)
                    return [dict(row) for row in rows]
                else:
                    result = await conn.execute(query, *args)
                    return result
        except asyncpg.PostgresError as e:
            return f"PostgreSQL Error: {str(e)}"

    async def run_query_one(
        self, query: str, *args: Any
    ) -> dict[str, Any] | str | None:
        """
        Execute a query and return a single row.
        """
        if self._pool is None:
            await self.connect()

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(query, *args)
                return dict(row) if row else None
        except asyncpg.PostgresError as e:
            return f"PostgreSQL Error: {str(e)}"

    async def execute_many(self, query: str, args_list: list[tuple]) -> str | None:
        """
        Execute a query with multiple sets of arguments (batch insert).
        """
        if self._pool is None:
            await self.connect()

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(query, args_list)
                return None
        except asyncpg.PostgresError as e:
            return f"PostgreSQL Error: {str(e)}"


if __name__ == "__main__":
    import asyncio

    async def main():
        db = PostgresDBClient(
            host="localhost",
            port=5432,
        )

        await db.connect()

        # Example query
        result = await db.run_query("SELECT version()")
        print(result)

        await db.close()

    asyncio.run(main())
