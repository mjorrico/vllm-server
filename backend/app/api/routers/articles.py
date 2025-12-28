from fastapi import APIRouter, HTTPException, Depends
from app.modules.PostgresDB.main import PostgresDBClient
from pydantic import BaseModel
from typing import Any, Dict, List
import os

router = APIRouter(prefix="/articles", tags=["Articles"])


@router.get("/")
async def list_articles(page: int = 1, with_content: bool = False) -> Dict[str, Any]:
    """
    Fetch paginated articles with metadata.
    """
    page_size = 6
    offset = (page - 1) * page_size

    db = PostgresDBClient(
        host="postgredb",
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("BACKEND_POSTGRES_DB", "postgres"),
    )

    try:
        await db.connect()

        # Get total count of articles
        count_query = "SELECT COUNT(*) as count FROM articles"
        count_result = await db.run_query(count_query)
        
        if isinstance(count_result, str):
            raise HTTPException(status_code=500, detail=count_result)
        
        total_count = count_result[0]["count"] if count_result else 0

        # UUIDv7 is time-ordered, so sorting by ID DESC gives newest first
        if with_content:
            query = "SELECT * FROM articles ORDER BY id DESC LIMIT $1 OFFSET $2"
        else:
            query = "SELECT id, introtext, category, duration, updated_at FROM articles ORDER BY id DESC LIMIT $1 OFFSET $2"
            
        result = await db.run_query(query, page_size, offset)

        if isinstance(result, str):
            raise HTTPException(status_code=500, detail=result)

        last_page = (total_count + page_size - 1) // page_size

        return {
            "articles": result,
            "page": page,
            "total_count": len(result),
            "last_page": last_page
        }

    except Exception as e:
        print(f"Error fetching articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await db.close()


@router.get("/{article_id}")
async def get_article(article_id: str) -> Dict[str, Any]:
    """
    Fetch an article by its ID.
    """
    db = PostgresDBClient(
        host="postgredb",
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("BACKEND_POSTGRES_DB", "postgres"),
    )

    try:
        await db.connect()

        # Primary assumption: table 'articles', column 'id'
        query = "SELECT * FROM articles WHERE id = $1"
        result = await db.run_query_one(query, article_id)

        if not result:
            raise HTTPException(status_code=404, detail="Article not found")

        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error checking article: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await db.close()


class CreateArticleRequest(BaseModel):
    content: str
    introtext: str
    category: str
    duration: int = 5


@router.post("/")
async def create_article(article: CreateArticleRequest) -> Dict[str, Any]:
    """
    Create a new article.
    """
    db = PostgresDBClient(
        host="postgredb",
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("BACKEND_POSTGRES_DB", "postgres"),
    )

    try:
        await db.connect()

        query = """
            INSERT INTO articles (content, introtext, category, duration)
            VALUES ($1, $2, $3, $4)
            RETURNING id, content, introtext, category, duration, updated_at
        """
        result = await db.run_query_one(
            query,
            article.content,
            article.introtext,
            article.category,
            article.duration,
        )

        if not result:
            raise HTTPException(status_code=500, detail="Failed to create article")

        return result

    except Exception as e:
        print(f"Error creating article: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await db.close()
