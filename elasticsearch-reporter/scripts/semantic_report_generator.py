#!/usr/bin/env python3
"""
Semantic Search Report Generator
Uses ELSER semantic search and generates HTML report for Vercel deployment
"""

import os
from datetime import datetime
from elasticsearch_client import FreshProduceClient
from product_images_config import get_image_url

# Initialize client
client = FreshProduceClient()


def generate_semantic_report(query: str, limit: int = 10):
    """
    Generate HTML report from semantic search results.
    
    Args:
        query: Natural language search query
        limit: Maximum number of results
    """
    print(f"\n🔍 SEMANTIC SEARCH REPORT")
    print("=" * 80)
    print(f"Query: '{query}'")
    print("=" * 80)
    
    # Perform semantic search
    try:
        products = client.semantic_search_elser(query, limit=limit)
        
        if not products:
            print("❌ No products found")
            return None
        
        print(f"\n✅ Found {len(products)} products")
        
        # Generate HTML
        today = datetime.now().strftime("%Y-%m-%d")
        html_file = f"reports/semantic_search_{today}.html"
        
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Semantic Search Results - {today}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        h1 {{
            text-align: center;
            color: white;
            margin-bottom: 20px;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}

        .query-box {{
            background: rgba(255,255,255,0.95);
            padding: 20px;
            border-radius: 15px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}

        .query-box h2 {{
            color: #667eea;
            margin-bottom: 10px;
        }}

        .query-text {{
            font-size: 1.3em;
            color: #333;
            font-style: italic;
            padding: 10px;
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            border-radius: 5px;
        }}

        .info-box {{
            background: rgba(255,255,255,0.1);
            color: white;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}

        .product-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }}

        .product-card {{
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s, box-shadow 0.3s;
            position: relative;
        }}

        .product-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.3);
        }}

        .product-image {{
            width: 100%;
            height: 200px;
            object-fit: cover;
        }}

        .product-content {{
            padding: 20px;
        }}

        .product-name {{
            font-size: 1.5em;
            color: #333;
            margin-bottom: 10px;
            font-weight: bold;
        }}

        .product-description {{
            color: #666;
            margin-bottom: 15px;
            line-height: 1.5;
        }}

        .product-details {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-bottom: 15px;
        }}

        .detail-item {{
            background: #f8f9fa;
            padding: 10px;
            border-radius: 8px;
            text-align: center;
        }}

        .detail-label {{
            font-size: 0.8em;
            color: #666;
            margin-bottom: 5px;
        }}

        .detail-value {{
            font-size: 1.1em;
            color: #333;
            font-weight: bold;
        }}

        .semantic-score {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            margin-top: 15px;
        }}

        .score-label {{
            font-size: 0.9em;
            opacity: 0.9;
            margin-bottom: 5px;
        }}

        .score-value {{
            font-size: 2em;
            font-weight: bold;
        }}

        .badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
            margin-right: 5px;
            margin-bottom: 5px;
        }}

        .badge-organic {{
            background: #28a745;
            color: white;
        }}

        .badge-sale {{
            background: #dc3545;
            color: white;
        }}

        .badge-premium {{
            background: #ffc107;
            color: #333;
        }}

        .rank-badge {{
            position: absolute;
            top: 15px;
            left: 15px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            width: 50px;
            height: 50px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5em;
            font-weight: bold;
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
        }}

        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
        }}

        @media (max-width: 768px) {{
            .product-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🧠 Semantic Search Results</h1>
        
        <div class="query-box">
            <h2>Your Question:</h2>
            <div class="query-text">{query}</div>
        </div>

        <div class="info-box">
            <p><strong>🎯 Found {len(products)} products using ELSER Semantic Search</strong></p>
            <p>Results ranked by semantic relevance (understanding meaning, not just keywords)</p>
            <p>Generated on {today}</p>
        </div>

        <div class="product-grid">"""

        # Add products
        for i, product in enumerate(products, 1):
            score = product.get("_score", 0)
            name = product.get("name", "Unknown")
            description = product.get("description", "")
            price = product.get("price_per_kg", 0)
            category = product.get("category", "").replace("_", " ").title()
            origin = product.get("origin", "N/A")
            rating = product.get("quality_rating", 0)
            stock = product.get("stock_kg", 0)
            organic = product.get("organic", False)
            on_sale = product.get("on_sale", False)
            
            # Get image
            image_url = get_image_url(name, product.get("image_url", ""))
            if not image_url:
                image_url = "https://via.placeholder.com/400x200?text=No+Image"
            
            # Badges
            badges = ""
            if organic:
                badges += '<span class="badge badge-organic">🌱 Organic</span>'
            if on_sale:
                badges += '<span class="badge badge-sale">🛍️ On Sale</span>'
            if rating >= 4.5:
                badges += '<span class="badge badge-premium">⭐ Premium</span>'
            
            html_content += f"""
            <div class="product-card">
                <div class="rank-badge">#{i}</div>
                <img src="{image_url}" alt="{name}" class="product-image">
                <div class="product-content">
                    <div class="product-name">{name}</div>
                    <div class="product-description">{description[:100]}...</div>
                    
                    <div>{badges}</div>
                    
                    <div class="product-details">
                        <div class="detail-item">
                            <div class="detail-label">💰 Price</div>
                            <div class="detail-value">R${price:.2f}/kg</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">⭐ Rating</div>
                            <div class="detail-value">{rating:.1f}/5</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">🏷️ Category</div>
                            <div class="detail-value">{category}</div>
                        </div>
                        <div class="detail-item">
                            <div class="detail-label">🌍 Origin</div>
                            <div class="detail-value">{origin}</div>
                        </div>
                    </div>
                    
                    <div class="semantic-score">
                        <div class="score-label">Semantic Relevance Score</div>
                        <div class="score-value">{score:.2f}</div>
                    </div>
                </div>
            </div>"""

        html_content += """
        </div>

        <div class="footer">
            <p><strong>🧠 Powered by ELSER</strong> (Elastic Learned Sparse EncodeR)</p>
            <p style="margin-top: 10px; opacity: 0.8;">
                Semantic search understands the <strong>meaning</strong> of your question,<br>
                not just matching keywords!
            </p>
            <p style="margin-top: 10px;">
                Stack: Elasticsearch Serverless + ELSER + OpenClaw + Vercel
            </p>
        </div>
    </div>
</body>
</html>"""

        # Write HTML
        os.makedirs("reports", exist_ok=True)
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print(f"\n✅ Report generated: {html_file}")
        print(f"📊 Products: {len(products)}")
        
        # Show top 3 results
        print("\n🎯 Top 3 Results:")
        for i, p in enumerate(products[:3], 1):
            print(f"{i}. {p['name']} (score: {p.get('_score', 0):.2f})")
        
        print("\n💡 Deploy with:")
        print("   cd reports && vercel --prod")
        
        return html_file
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = "o que posso usar para fazer uma refeição saudável e colorida"
    
    generate_semantic_report(query, limit=10)
