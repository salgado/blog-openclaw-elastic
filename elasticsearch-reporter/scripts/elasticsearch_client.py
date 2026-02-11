"""
Elasticsearch Serverless Client for Fresh Produce Query Handler
Handles all queries to the fresh_produce index with read-only API key
"""

import os
import logging
from typing import Dict, List, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FreshProduceClient:
    """Client for querying Fresh Produce data from Elasticsearch Serverless"""

    def __init__(self):
        """Initialize Elasticsearch client with API Key authentication"""
        self.es_url = os.getenv("ELASTICSEARCH_URL")
        self.api_key = os.getenv("ELASTICSEARCH_API_KEY")
        self.index = os.getenv("ELASTICSEARCH_INDEX", "fresh_produce")

        if not self.es_url or not self.api_key:
            raise ValueError(
                "Missing ELASTICSEARCH_URL or ELASTICSEARCH_API_KEY in environment variables"
            )

        try:
            self.client = Elasticsearch(
                hosts=[self.es_url],
                api_key=self.api_key,
                verify_certs=True,
                request_timeout=30,
            )
            # Test connection with a simple count query (read-only safe)
            self.client.count(index=self.index)
            logger.info("✅ Connected to Elasticsearch Serverless")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Elasticsearch: {e}")
            raise

    def search_by_name(self, product_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search products by name or description
        
        Args:
            product_name: Product name to search for
            limit: Maximum number of results
            
        Returns:
            List of matching products
        """
        try:
            query = {
                "multi_match": {
                    "query": product_name,
                    "fields": ["name^2", "description"],  # Boost name matches
                    "fuzziness": "AUTO",
                }
            }

            response = self.client.search(
                index=self.index, query=query, size=limit, timeout="5s"
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_id"] = hit["_id"]
                products.append(product)

            logger.info(
                f"✅ Search '{product_name}': found {len(products)} products"
            )
            return products

        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []

    def get_on_sale_products(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get all products currently on sale
        Sorted by discount percentage (highest first)
        
        Args:
            limit: Maximum number of results
            
        Returns:
            List of products on sale
        """
        try:
            query = {
                "bool": {
                    "must": [{"term": {"on_sale": True}}, {"term": {"status": "active"}}]
                }
            }

            response = self.client.search(
                index=self.index,
                query=query,
                sort=[{"discount_percent": {"order": "desc"}}],
                size=limit,
                timeout="5s",
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_id"] = hit["_id"]
                # Calculate discounted price
                product["discounted_price"] = round(
                    product["price_per_kg"] * (1 - product["discount_percent"] / 100), 2
                )
                products.append(product)

            logger.info(f"✅ Found {len(products)} products on sale")
            return products

        except Exception as e:
            logger.error(f"❌ On-sale query failed: {e}")
            return []

    def get_low_stock_products(self, threshold_kg: float = 50) -> List[Dict[str, Any]]:
        """
        Get products with stock below threshold
        Sorted by stock quantity (lowest first)
        
        Args:
            threshold_kg: Stock threshold in kg
            
        Returns:
            List of low-stock products
        """
        try:
            query = {
                "bool": {
                    "must": [
                        {"range": {"stock_kg": {"lt": threshold_kg}}},
                        {"term": {"status": "active"}},
                    ]
                }
            }

            response = self.client.search(
                index=self.index,
                query=query,
                sort=[{"stock_kg": {"order": "asc"}}],
                size=100,
                timeout="5s",
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_id"] = hit["_id"]
                products.append(product)

            logger.info(f"✅ Found {len(products)} products with low stock")
            return products

        except Exception as e:
            logger.error(f"❌ Low-stock query failed: {e}")
            return []

    def get_by_category(self, category: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get all products in a specific category
        Sorted by quality rating (highest first)
        
        Args:
            category: Category name (keyword)
            limit: Maximum number of results
            
        Returns:
            List of products in category
        """
        try:
            # Validate category against predefined list
            valid_categories = [
                "vegetables",
                "leafy_greens",
                "roots",
                "fruits",
            ]
            category_lower = category.lower().replace(" ", "_")

            if category_lower not in valid_categories:
                logger.warning(f"⚠️ Invalid category: {category}")
                return []

            query = {
                "bool": {
                    "must": [
                        {"term": {"category": category_lower}},
                        {"term": {"status": "active"}},
                    ]
                }
            }

            response = self.client.search(
                index=self.index,
                query=query,
                sort=[{"quality_rating": {"order": "desc"}}],
                size=limit,
                timeout="5s",
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_id"] = hit["_id"]
                products.append(product)

            logger.info(f"✅ Found {len(products)} products in category '{category}'")
            return products

        except Exception as e:
            logger.error(f"❌ Category query failed: {e}")
            return []

    def get_all_products(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get all active products
        Used for report generation
        
        Args:
            limit: Maximum number of results
            
        Returns:
            List of all products
        """
        try:
            query = {"term": {"status": "active"}}

            response = self.client.search(
                index=self.index, query=query, size=limit, timeout="5s"
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_id"] = hit["_id"]
                products.append(product)

            logger.info(f"✅ Retrieved {len(products)} total products")
            return products

        except Exception as e:
            logger.error(f"❌ Get all products failed: {e}")
            return []

    def get_product_by_id(self, product_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific product by ID
        
        Args:
            product_id: Product document ID
            
        Returns:
            Product data or None if not found
        """
        try:
            response = self.client.get(index=self.index, id=product_id)
            product = response["_source"]
            product["_id"] = response["_id"]
            logger.info(f"✅ Retrieved product: {product.get('name')}")
            return product

        except Exception as e:
            logger.error(f"❌ Get product by ID failed: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get aggregate statistics about the fresh_produce index
        Used for dashboard/reports
        
        Returns:
            Dictionary with statistics
        """
        try:
            # Get total count
            count_response = self.client.count(index=self.index)
            total_products = count_response["count"]

            # Get aggregation stats
            agg_query = {
                "aggs": {
                    "by_category": {"terms": {"field": "category", "size": 10}},
                    "avg_price": {"avg": {"field": "price_per_kg"}},
                    "avg_rating": {"avg": {"field": "quality_rating"}},
                    "total_stock": {"sum": {"field": "stock_kg"}},
                    "organic_count": {
                        "filter": {"term": {"organic": True}},
                    },
                    "on_sale_count": {
                        "filter": {"term": {"on_sale": True}},
                    },
                }
            }

            response = self.client.search(
                index=self.index, size=0, aggs=agg_query["aggs"], timeout="5s"
            )

            stats = {
                "total_products": total_products,
                "average_price_per_kg": round(
                    response["aggregations"]["avg_price"]["value"], 2
                ),
                "average_rating": round(
                    response["aggregations"]["avg_rating"]["value"], 1
                ),
                "total_stock_kg": round(
                    response["aggregations"]["total_stock"]["value"], 1
                ),
                "organic_products": response["aggregations"]["organic_count"][
                    "doc_count"
                ],
                "products_on_sale": response["aggregations"]["on_sale_count"][
                    "doc_count"
                ],
                "categories": {
                    cat["key"]: cat["doc_count"]
                    for cat in response["aggregations"]["by_category"]["buckets"]
                },
            }

            logger.info(f"✅ Generated index statistics")
            return stats

        except Exception as e:
            logger.error(f"❌ Get stats failed: {e}")
            return {}

    def semantic_search_elser(
        self, query_text: str, limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Perform semantic search using ELSER (Elastic Learned Sparse EncodeR).
        Uses Elasticsearch's built-in ML model - no external API needed!

        Args:
            query_text: Natural language search query
            limit: Maximum number of results to return

        Returns:
            List of products with semantic similarity to the query
        """
        try:
            response = self.client.search(
                index=self.index,
                query={
                    "text_expansion": {
                        "ml.inference.semantic_text_expanded": {
                            "model_id": ".elser_model_2_linux-x86_64",
                            "model_text": query_text,
                        }
                    }
                },
                size=limit,
                _source=[
                    "name",
                    "description",
                    "price_per_kg",
                    "category",
                    "quality_rating",
                    "organic",
                    "on_sale",
                    "discount_percent",
                    "stock_kg",
                    "origin",
                    "tags",
                ],
            )

            products = []
            for hit in response["hits"]["hits"]:
                product = hit["_source"]
                product["_score"] = hit["_score"]  # Include similarity score
                products.append(product)

            logger.info(
                f"✅ ELSER semantic search completed: {len(products)} products found"
            )
            return products

        except es_exceptions.RequestError as e:
            logger.error(f"❌ ELSER semantic search failed: {e}")
            logger.error("   Make sure ELSER is set up (run setup_elser.py)")
            return []
        except Exception as e:
            logger.error(f"❌ ELSER semantic search failed: {e}")
            return []

    def close(self):
        """Close Elasticsearch connection"""
        try:
            self.client.close()
            logger.info("✅ Elasticsearch connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


# Example usage for testing
if __name__ == "__main__":
    try:
        client = FreshProduceClient()

        # Test search
        print("\n🔍 Testing search for 'tomato':")
        results = client.search_by_name("tomato")
        for product in results:
            print(f"  • {product['name']} - R${product['price_per_kg']}/kg")

        # Test on-sale
        print("\n🛍️ Testing on-sale products:")
        results = client.get_on_sale_products()
        for product in results:
            print(f"  • {product['name']} - {product['discount_percent']}% OFF")

        # Test low stock
        print("\n⚠️ Testing low stock products:")
        results = client.get_low_stock_products()
        for product in results:
            print(f"  • {product['name']} - {product['stock_kg']}kg")

        # Test by category
        print("\n🥬 Testing category query (vegetables):")
        results = client.get_by_category("vegetables")
        for product in results:
            print(f"  • {product['name']} - ⭐ {product['quality_rating']}")

        # Test stats
        print("\n📊 Testing statistics:")
        stats = client.get_stats()
        print(f"  Total products: {stats['total_products']}")
        print(f"  Avg price: R${stats['average_price_per_kg']}/kg")
        print(f"  Avg rating: {stats['average_rating']}/5")

        client.close()

    except Exception as e:
        print(f"❌ Error: {e}")
