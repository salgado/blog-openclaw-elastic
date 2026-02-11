#!/usr/bin/env python3
"""
Complete Elasticsearch setup script for ELSER semantic search.
This script consolidates all setup steps into a single automated workflow.

Steps:
1. Verify Elasticsearch connection
2. Create index with proper mapping (including semantic_text)
3. Load products from JSON data file
4. Deploy ELSER model
5. Create ingest pipeline
6. Generate ELSER embeddings
7. Validate setup

Author: Alex Salgado
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


class ElasticsearchSetup:
    """Complete Elasticsearch setup for ELSER semantic search"""

    def __init__(self):
        """Initialize Elasticsearch client"""
        self.es_url = os.getenv("ELASTICSEARCH_URL")
        self.api_key = os.getenv("ELASTICSEARCH_API_KEY")
        self.index = os.getenv("ELASTICSEARCH_INDEX", "fresh_produce")
        self.elser_model = ".elser_model_2_linux-x86_64"
        
        if not self.es_url or not self.api_key:
            raise ValueError(
                "Missing ELASTICSEARCH_URL or ELASTICSEARCH_API_KEY in .env file"
            )

        logger.info("🔍 Connecting to Elasticsearch Serverless...")
        try:
            self.client = Elasticsearch(
                hosts=[self.es_url],
                api_key=self.api_key,
                verify_certs=True,
                request_timeout=60,
            )
            # Test connection
            info = self.client.info()
            logger.info(f"✅ Connected to Elasticsearch")
            logger.info(f"   Cluster: {info.get('cluster_name', 'N/A')}")
            logger.info(f"   Version: {info.get('version', {}).get('number', 'N/A')}")
        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            raise

    def create_index(self, force: bool = False) -> bool:
        """Create index with proper mapping for ELSER semantic search"""
        logger.info("\n📦 Creating index...")
        
        # Check if index exists
        try:
            if self.client.indices.exists(index=self.index):
                if not force:
                    logger.info(f"⚠️  Index '{self.index}' already exists")
                    logger.info("   Use --force to recreate")
                    return True
                else:
                    logger.info(f"🗑️  Deleting existing index '{self.index}'...")
                    self.client.indices.delete(index=self.index)
                    logger.info("✅ Deleted")
        except Exception as e:
            logger.error(f"❌ Error checking index: {e}")
            return False

        # Index mapping with semantic_text field
        mapping = {
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "price": {"type": "float"},
                    "discount_price": {"type": "float"},
                    "stock_kg": {"type": "float"},
                    "category": {"type": "keyword"},
                    "description": {"type": "text"},
                    "on_sale": {"type": "boolean"},
                    "image_url": {"type": "keyword"},
                    "updated_at": {"type": "date"},
                    # ELSER semantic field
                    "semantic_text": {
                        "type": "semantic_text",
                        "inference_id": self.elser_model
                    }
                }
            }
        }

        try:
            self.client.indices.create(index=self.index, body=mapping)
            logger.info(f"✅ Index '{self.index}' created")
            logger.info("   Mapping includes:")
            logger.info("   • name, price, stock, category, description")
            logger.info("   • semantic_text (for ELSER)")
            return True
        except Exception as e:
            logger.error(f"❌ Error creating index: {e}")
            return False

    def load_products(self) -> bool:
        """Load products from data/products.json"""
        logger.info("\n📝 Loading products...")
        
        # Find data file
        data_file = Path(__file__).parent.parent / "data" / "products.json"
        
        if not data_file.exists():
            logger.error(f"❌ Data file not found: {data_file}")
            return False

        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            logger.info(f"   Found {len(products)} products in {data_file.name}")
            
            # Index products with semantic_text
            indexed = 0
            for i, product in enumerate(products, 1):
                # Create semantic_text from multiple fields
                semantic_text = f"{product.get('name', '')} {product.get('description', '')} {product.get('category', '')}"
                product['semantic_text'] = semantic_text.strip()
                
                # Index document
                doc_id = product.get('id', i)
                try:
                    self.client.index(
                        index=self.index,
                        id=doc_id,
                        document=product
                    )
                    logger.info(f"   [{i}/{len(products)}] {product.get('name', 'N/A')}... ✅")
                    indexed += 1
                except Exception as e:
                    logger.error(f"   [{i}/{len(products)}] Error: {e}")
            
            # Refresh index
            self.client.indices.refresh(index=self.index)
            
            logger.info(f"✅ {indexed}/{len(products)} products indexed")
            return indexed > 0
            
        except Exception as e:
            logger.error(f"❌ Error loading products: {e}")
            return False

    def check_elser_deployment(self) -> bool:
        """Check if ELSER model is deployed"""
        logger.info("\n🔍 Checking ELSER model...")
        
        try:
            # Check if model exists
            response = self.client.ml.get_trained_models(
                model_id=self.elser_model
            )
            
            if response["count"] == 0:
                logger.info(f"❌ ELSER model '{self.elser_model}' not found")
                return False
            
            logger.info(f"✅ ELSER model found: {self.elser_model}")
            
            # Check deployment status
            stats = self.client.ml.get_trained_models_stats(
                model_id=self.elser_model
            )
            
            if stats["count"] > 0:
                deployment = stats["trained_model_stats"][0]
                state = deployment.get("deployment_stats", {}).get("state", "not_deployed")
                
                if state == "started":
                    logger.info(f"✅ ELSER is deployed and running")
                    return True
                else:
                    logger.info(f"⚠️  ELSER state: {state}")
                    return False
            else:
                logger.info("⚠️  ELSER not deployed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error checking ELSER: {e}")
            return False

    def deploy_elser(self, skip: bool = False) -> bool:
        """Deploy ELSER model"""
        if skip:
            logger.info("\n⏭️  Skipping ELSER deployment (--skip-elser)")
            return True
            
        logger.info("\n🚀 Deploying ELSER model...")
        logger.info("   This may take 3-5 minutes...")
        
        try:
            # Start deployment
            self.client.ml.start_trained_model_deployment(
                model_id=self.elser_model,
                wait_for="started",
                timeout="5m"
            )
            
            logger.info("✅ ELSER deployed successfully!")
            logger.info("   Model is ready for semantic search")
            return True
            
        except Exception as e:
            error_msg = str(e)
            if "resource_already_exists_exception" in error_msg:
                logger.info("✅ ELSER already deployed")
                return True
            else:
                logger.error(f"❌ Error deploying ELSER: {e}")
                logger.info("\n💡 Troubleshooting:")
                logger.info("   1. Verify you're using Elasticsearch Serverless")
                logger.info("   2. Check ELSER is available in your region")
                logger.info("   3. Ensure API key has 'manage_ml' privilege")
                return False

    def create_ingest_pipeline(self) -> bool:
        """Create ingest pipeline for ELSER inference"""
        logger.info("\n⚙️  Creating ingest pipeline...")
        
        pipeline_id = "elser-fresh-produce-pipeline"
        pipeline_config = {
            "description": "Ingest pipeline for ELSER semantic search",
            "processors": [
                {
                    "inference": {
                        "model_id": self.elser_model,
                        "input_output": [
                            {
                                "input_field": "semantic_text",
                                "output_field": "ml.inference.semantic_embedding"
                            }
                        ]
                    }
                }
            ]
        }
        
        try:
            self.client.ingest.put_pipeline(
                id=pipeline_id,
                body=pipeline_config
            )
            
            logger.info(f"✅ Pipeline created: {pipeline_id}")
            logger.info("   Processor: inference")
            logger.info(f"   Model: {self.elser_model}")
            logger.info("   Input: semantic_text → Output: ml.inference.semantic_embedding")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating pipeline: {e}")
            return False

    def generate_embeddings(self) -> bool:
        """Generate ELSER embeddings for all documents"""
        logger.info("\n🧠 Generating ELSER embeddings...")
        
        try:
            # Update all documents through the pipeline
            result = self.client.update_by_query(
                index=self.index,
                pipeline="elser-fresh-produce-pipeline",
                body={"query": {"match_all": {}}}
            )
            
            updated = result.get("updated", 0)
            logger.info(f"✅ Embeddings generated for {updated} documents")
            logger.info("   Field: ml.inference.semantic_embedding populated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error generating embeddings: {e}")
            return False

    def validate_setup(self) -> bool:
        """Validate complete setup"""
        logger.info("\n🔍 Validating setup...")
        
        checks = []
        
        # 1. Index exists
        try:
            if self.client.indices.exists(index=self.index):
                checks.append("✅ Index exists")
            else:
                checks.append("❌ Index not found")
                return False
        except Exception as e:
            checks.append(f"❌ Index check failed: {e}")
            return False
        
        # 2. Documents indexed
        try:
            count = self.client.count(index=self.index)["count"]
            if count > 0:
                checks.append(f"✅ {count} documents indexed")
            else:
                checks.append("❌ No documents found")
                return False
        except Exception as e:
            checks.append(f"❌ Count check failed: {e}")
            return False
        
        # 3. ELSER deployed
        if self.check_elser_deployment():
            checks.append("✅ ELSER deployed and ready")
        else:
            checks.append("❌ ELSER not ready")
            return False
        
        # 4. Pipeline exists
        try:
            self.client.ingest.get_pipeline(id="elser-fresh-produce-pipeline")
            checks.append("✅ Pipeline configured")
        except:
            checks.append("❌ Pipeline not found")
            return False
        
        # 5. Semantic embeddings present
        try:
            result = self.client.search(
                index=self.index,
                body={
                    "query": {"match_all": {}},
                    "_source": ["name", "semantic_text"],
                    "size": 1
                }
            )
            
            if result["hits"]["total"]["value"] > 0:
                doc = result["hits"]["hits"][0]["_source"]
                if "semantic_text" in doc:
                    checks.append("✅ semantic_text field present")
                else:
                    checks.append("❌ semantic_text field missing")
                    return False
        except Exception as e:
            checks.append(f"❌ Embedding check failed: {e}")
            return False
        
        # Print results
        logger.info("")
        for check in checks:
            logger.info(f"   {check}")
        
        return True

    def run_setup(self, force: bool = False, skip_elser: bool = False) -> bool:
        """Execute complete setup workflow"""
        logger.info("\n" + "="*60)
        logger.info("🚀 ELASTICSEARCH + ELSER SETUP")
        logger.info("="*60)
        
        steps = [
            ("Create Index", lambda: self.create_index(force)),
            ("Load Products", self.load_products),
            ("Check ELSER", self.check_elser_deployment),
            ("Deploy ELSER", lambda: self.deploy_elser(skip_elser)),
            ("Create Pipeline", self.create_ingest_pipeline),
            ("Generate Embeddings", self.generate_embeddings),
            ("Validate Setup", self.validate_setup),
        ]
        
        for i, (step_name, step_func) in enumerate(steps, 1):
            logger.info(f"\n[{i}/{len(steps)}] {step_name}...")
            try:
                if not step_func():
                    logger.error(f"\n❌ Setup failed at step: {step_name}")
                    return False
            except Exception as e:
                logger.error(f"\n❌ Error in {step_name}: {e}")
                return False
        
        # Success summary
        logger.info("\n" + "="*60)
        logger.info("🎉 SETUP COMPLETE!")
        logger.info("="*60)
        logger.info("\n✅ Your Elasticsearch cluster is ready for semantic search!")
        logger.info("\n🎯 Next steps:")
        logger.info("   1. Test semantic search:")
        logger.info("      openclaw run elasticsearch-reporter semantic-search 'healthy meals'")
        logger.info("")
        logger.info("   2. Generate a report:")
        logger.info("      openclaw run elasticsearch-reporter generate-report 'colorful vegetables'")
        logger.info("")
        logger.info("   3. Check status:")
        logger.info("      openclaw run elasticsearch-reporter check-status")
        
        return True


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Complete Elasticsearch + ELSER setup"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reindex even if data exists"
    )
    parser.add_argument(
        "--skip-elser",
        action="store_true",
        help="Skip ELSER deployment (use if already deployed)"
    )
    
    args = parser.parse_args()
    
    try:
        setup = ElasticsearchSetup()
        success = setup.run_setup(force=args.force, skip_elser=args.skip_elser)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
