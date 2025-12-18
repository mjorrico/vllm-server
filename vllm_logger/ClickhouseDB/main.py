import os
import json
import dotenv
import asyncio
import base64
import httpx

dotenv.load_dotenv()

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")


class ClickhouseDB:
    def __init__(
        self,
        host,
        port,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.base_url = f"http://{host}:{port}/?database={database}"

    async def run_query(self, query):
        # 1. Automatically append FORMAT JSON if it's a SELECT query
        if query.strip().upper().startswith("SELECT") and "FORMAT" not in query.upper():
            query += " FORMAT JSON"

        auth_str = f"{self.user}:{self.password}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "text/plain",
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.base_url,
                    content=query.encode("utf-8"),
                    headers=headers,
                    timeout=30,
                )
                response.raise_for_status()
                # For INSERTs or non-SELECTs, clickhouse might return empty body or simple string
                if not response.content:
                    return None
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return response.text
            except httpx.RequestError as e:
                return f"ClickHouse Error: {str(e)}"
            except httpx.HTTPStatusError as e:
                return f"ClickHouse Error: {e.response.text}"


if __name__ == "__main__":

    async def main():
        db = ClickhouseDB(
            host="irvins",
            port=8123,
        )

        result = await db.run_query("SELECT * FROM vllm_logger.vllm_log LIMIT 3")
        # print(json.dumps(result, indent=2))
        if isinstance(result, dict) and "data" in result:
            print(result["data"])
        else:
            print(result)

    asyncio.run(main())
