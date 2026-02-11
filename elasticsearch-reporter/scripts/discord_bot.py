"""
Discord Bot for Fresh Produce Query Handler
Integrates with Elasticsearch Serverless to provide product information
Supports slash commands for searching, filtering, and generating reports
"""

import os
import logging
from typing import List, Optional
from datetime import datetime
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
from elasticsearch_client import FreshProduceClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Discord bot with intents
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Elasticsearch client (initialized on bot ready)
es_client: Optional[FreshProduceClient] = None

# Rate limiting (simple dict-based for demo)
# In production, use redis or discord.py's built-in rate limiting
user_query_counts = {}


class FreshProduceBot(commands.Cog):
    """Slash commands for Fresh Produce queries"""

    def __init__(self, bot):
        self.bot = bot
        self.es_client = None

    async def cog_load(self):
        """Initialize Elasticsearch client when cog loads"""
        global es_client
        try:
            self.es_client = FreshProduceClient()
            es_client = self.es_client
            logger.info("✅ Elasticsearch client initialized in Discord bot")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Elasticsearch: {e}")
            raise

    def format_product_embed(self, product: dict) -> discord.Embed:
        """Format product data as Discord embed"""
        embed = discord.Embed(
            title=product.get("name", "Unknown"),
            description=product.get("description", ""),
            color=discord.Color.green(),
        )

        # Add image if available
        if product.get("image_url"):
            embed.set_image(url=product["image_url"])

        # Price and discount
        price_text = f"R${product['price_per_kg']:.2f}/kg"
        if product.get("on_sale"):
            discounted = product["price_per_kg"] * (
                1 - product.get("discount_percent", 0) / 100
            )
            price_text += f"\n🎉 **ON SALE:** -${product.get('discount_percent', 0)}% → R${discounted:.2f}/kg"

        embed.add_field(name="💰 Price", value=price_text, inline=True)

        # Stock and quality
        embed.add_field(
            name="📦 Stock",
            value=f"{product.get('stock_kg', 0):.1f} kg\n({product.get('shelf_life_days', 0)} days shelf life)",
            inline=True,
        )

        embed.add_field(
            name="⭐ Quality",
            value=f"{product.get('quality_rating', 0):.1f}/5",
            inline=True,
        )

        # Category and origin
        category = product.get("category", "").replace("_", " ").title()
        embed.add_field(name="🏷️ Category", value=category, inline=True)
        embed.add_field(name="🌍 Origin", value=product.get("origin", "N/A"), inline=True)

        # Organic badge
        organic_text = "✅ Organic" if product.get("organic") else "❌ Conventional"
        embed.add_field(name="🌱 Type", value=organic_text, inline=True)

        # Tags
        tags = product.get("tags", [])
        if tags:
            embed.add_field(name="🏷️ Tags", value=", ".join(tags), inline=False)

        # Timestamp
        embed.set_footer(text=f"Last restocked: {product.get('last_restocked', 'N/A')}")

        return embed

    @app_commands.command(
        name="search", description="Search for products by name or description"
    )
    @app_commands.describe(product_name="Product name to search for")
    async def search(self, interaction: discord.Interaction, product_name: str):
        """Search for products"""
        await interaction.response.defer()

        # Input validation
        if len(product_name) > 100:
            await interaction.followup.send("❌ Search query too long (max 100 chars)")
            return

        try:
            products = self.es_client.search_by_name(product_name, limit=5)

            if not products:
                await interaction.followup.send(f"❌ No products found for '{product_name}'")
                return

            # Create embeds for each product
            embeds = [self.format_product_embed(p) for p in products]

            # Send all embeds
            await interaction.followup.send(
                content=f"🔍 Found **{len(products)}** product(s):", embeds=embeds
            )

            # Log query
            logger.info(
                f"🔍 Search | user:{interaction.user.name} | query:{product_name} | results:{len(products)}"
            )

        except Exception as e:
            logger.error(f"❌ Search command error: {e}")
            await interaction.followup.send(
                f"❌ Error searching: {str(e)[:100]}"
            )

    @app_commands.command(
        name="semantic", description="🧠 Semantic search using ELSER (understands meaning!)"
    )
    @app_commands.describe(query="Natural language query (e.g., 'healthy nutritious vegetables')")
    async def semantic(self, interaction: discord.Interaction, query: str):
        """Semantic search using ELSER"""
        await interaction.response.defer()

        # Input validation
        if len(query) > 200:
            await interaction.followup.send("❌ Query too long (max 200 chars)")
            return

        try:
            products = self.es_client.semantic_search_elser(query, limit=5)

            if not products:
                await interaction.followup.send(f"❌ No products found for '{query}'")
                return

            # Create embeds for each product with semantic score
            embeds = []
            for product in products:
                embed = self.format_product_embed(product)
                # Add semantic relevance score
                score = product.get("_score", 0)
                embed.add_field(
                    name="🧠 Semantic Score", 
                    value=f"{score:.2f} (relevance)", 
                    inline=False
                )
                embeds.append(embed)

            # Send results with semantic search indicator
            await interaction.followup.send(
                content=f"🧠 **SEMANTIC SEARCH** (ELSER): Found **{len(products)}** relevant product(s) for: *'{query}'*\n"
                        f"💡 ELSER understands meaning, not just keywords!",
                embeds=embeds
            )

            # Log query
            logger.info(
                f"🧠 Semantic | user:{interaction.user.name} | query:{query} | results:{len(products)}"
            )

        except Exception as e:
            logger.error(f"❌ Semantic search error: {e}")
            await interaction.followup.send(
                f"❌ Error in semantic search: Make sure ELSER is set up!\n"
                f"Error: {str(e)[:100]}"
            )

    @app_commands.command(
        name="on_sale", description="Show all products currently on sale"
    )
    async def on_sale(self, interaction: discord.Interaction):
        """Show products on sale"""
        await interaction.response.defer()

        try:
            products = self.es_client.get_on_sale_products(limit=10)

            if not products:
                await interaction.followup.send("❌ No products on sale at the moment")
                return

            # Create summary text
            sale_summary = "🛍️ **Products on Sale:**\n\n"
            for i, p in enumerate(products, 1):
                discount = p.get("discount_percent", 0)
                original = p["price_per_kg"]
                discounted = p.get("discounted_price", original)
                sale_summary += (
                    f"{i}. **{p['name']}**\n"
                    f"   -${discount}% OFF: R${original:.2f} → R${discounted:.2f}/kg\n"
                    f"   Stock: {p.get('stock_kg', 0):.1f}kg\n\n"
                )

            # Create embeds for first 3 products
            embeds = [self.format_product_embed(p) for p in products[:3]]

            await interaction.followup.send(
                content=sale_summary, embeds=embeds
            )

            logger.info(
                f"🛍️ On-sale | user:{interaction.user.name} | products:{len(products)}"
            )

        except Exception as e:
            logger.error(f"❌ On-sale command error: {e}")
            await interaction.followup.send(f"❌ Error: {str(e)[:100]}")

    @app_commands.command(
        name="stock_low",
        description="Show products with low stock",
    )
    @app_commands.describe(threshold="Stock threshold in kg (default: 50)")
    async def stock_low(self, interaction: discord.Interaction, threshold: int = 50):
        """Show low-stock products"""
        await interaction.response.defer()

        try:
            products = self.es_client.get_low_stock_products(threshold_kg=threshold)

            if not products:
                await interaction.followup.send(
                    f"✅ No products with stock below {threshold}kg"
                )
                return

            # Create summary
            low_stock_summary = f"⚠️ **Low Stock Alert** (< {threshold}kg):\n\n"
            for i, p in enumerate(products, 1):
                days_left = p.get("shelf_life_days", 0)
                low_stock_summary += (
                    f"{i}. **{p['name']}** - **{p['stock_kg']:.1f}kg**\n"
                    f"   Shelf life: {days_left} days\n"
                    f"   Price: R${p['price_per_kg']:.2f}/kg\n\n"
                )

            embeds = [self.format_product_embed(p) for p in products[:3]]

            await interaction.followup.send(
                content=low_stock_summary, embeds=embeds
            )

            logger.info(
                f"⚠️ Low-stock | user:{interaction.user.name} | products:{len(products)}"
            )

        except Exception as e:
            logger.error(f"❌ Stock-low command error: {e}")
            await interaction.followup.send(f"❌ Error: {str(e)[:100]}")

    @app_commands.command(
        name="category",
        description="List products by category",
    )
    @app_commands.describe(category="Product category")
    @app_commands.choices(
        category=[
            app_commands.Choice(name="Vegetables", value="vegetables"),
            app_commands.Choice(name="Leafy Greens", value="leafy_greens"),
            app_commands.Choice(name="Roots", value="roots"),
            app_commands.Choice(name="Fruits", value="fruits"),
        ]
    )
    async def category(self, interaction: discord.Interaction, category: str):
        """List products by category"""
        await interaction.response.defer()

        try:
            products = self.es_client.get_by_category(category, limit=10)

            if not products:
                await interaction.followup.send(
                    f"❌ No products found in category '{category}'"
                )
                return

            # Create category summary
            category_name = category.replace("_", " ").title()
            category_summary = f"🥬 **{category_name}:**\n\n"
            for i, p in enumerate(products, 1):
                category_summary += (
                    f"{i}. **{p['name']}** - R${p['price_per_kg']:.2f}/kg\n"
                    f"   ⭐ {p['quality_rating']}/5 | 📦 {p['stock_kg']:.1f}kg\n\n"
                )

            embeds = [self.format_product_embed(p) for p in products[:3]]

            await interaction.followup.send(
                content=category_summary, embeds=embeds
            )

            logger.info(
                f"🏷️ Category | user:{interaction.user.name} | category:{category} | products:{len(products)}"
            )

        except Exception as e:
            logger.error(f"❌ Category command error: {e}")
            await interaction.followup.send(f"❌ Error: {str(e)[:100]}")

    @app_commands.command(
        name="stats",
        description="Get inventory statistics and insights",
    )
    async def stats(self, interaction: discord.Interaction):
        """Show inventory statistics"""
        await interaction.response.defer()

        try:
            stats = self.es_client.get_stats()

            if not stats:
                await interaction.followup.send("❌ Could not retrieve statistics")
                return

            # Create stats embed
            embed = discord.Embed(
                title="📊 Inventory Statistics",
                color=discord.Color.blue(),
            )

            embed.add_field(
                name="📦 Total Products",
                value=str(stats.get("total_products", 0)),
                inline=True,
            )
            embed.add_field(
                name="💰 Avg Price",
                value=f"R${stats.get('average_price_per_kg', 0):.2f}/kg",
                inline=True,
            )
            embed.add_field(
                name="⭐ Avg Rating",
                value=f"{stats.get('average_rating', 0):.1f}/5",
                inline=True,
            )

            embed.add_field(
                name="📦 Total Stock",
                value=f"{stats.get('total_stock_kg', 0):.1f}kg",
                inline=True,
            )
            embed.add_field(
                name="🌱 Organic",
                value=str(stats.get("organic_products", 0)),
                inline=True,
            )
            embed.add_field(
                name="🛍️ On Sale",
                value=str(stats.get("products_on_sale", 0)),
                inline=True,
            )

            # Categories breakdown
            categories = stats.get("categories", {})
            if categories:
                cat_text = "\n".join(
                    [f"• {cat.title()}: {count}" for cat, count in categories.items()]
                )
                embed.add_field(name="🏷️ By Category", value=cat_text, inline=False)

            embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            await interaction.followup.send(embed=embed)

            logger.info(f"📊 Stats | user:{interaction.user.name}")

        except Exception as e:
            logger.error(f"❌ Stats command error: {e}")
            await interaction.followup.send(f"❌ Error: {str(e)[:100]}")

    @app_commands.command(
        name="report",
        description="🎯 Generate HTML report + deploy to Vercel (semantic search → web)",
    )
    @app_commands.describe(query="Natural language query (e.g., 'spicy salad ingredients')")
    async def report(self, interaction: discord.Interaction, query: str):
        """Generate semantic search report and deploy to Vercel"""
        await interaction.response.defer()

        # Input validation
        if len(query) > 200:
            await interaction.followup.send("❌ Query too long (max 200 chars)")
            return

        try:
            # Step 1: Semantic search
            await interaction.followup.send(f"🔍 Searching for: **{query}**...")
            products = self.es_client.semantic_search_elser(query, limit=10)

            if not products:
                await interaction.followup.send(f"❌ No products found for '{query}'")
                return

            # Step 2: Generate HTML report
            await interaction.followup.send(f"📝 Generating HTML report...")
            
            import subprocess
            from datetime import datetime
            
            # Run semantic_report_generator.py
            result = subprocess.run(
                ["python3", "semantic_report_generator.py", query],
                capture_output=True,
                text=True,
                cwd="/Users/alexsalgado/Desktop/sandbox/blog-ai-elastic"
            )
            
            if result.returncode != 0:
                await interaction.followup.send(f"❌ Report generation failed: {result.stderr[:200]}")
                return
            
            # Step 3: Deploy to Vercel
            await interaction.followup.send(f"🚀 Deploying to Vercel...")
            
            vercel_result = subprocess.run(
                [
                    "vercel", "--prod", "--yes", 
                    "--token", os.getenv("VERCEL_TOKEN", "")
                ],
                capture_output=True,
                text=True,
                cwd="/Users/alexsalgado/Desktop/sandbox/blog-ai-elastic/reports",
                env={**os.environ, "VERCEL_ORG_ID": "team_VMJLnseGOskFFzRCmNw6qHFm", "VERCEL_PROJECT_ID": "prj_vf5QF2qLMTH2izz1LIfugFAaIDmu"}
            )
            
            if vercel_result.returncode != 0:
                await interaction.followup.send(f"❌ Vercel deploy failed: {vercel_result.stderr[:200]}")
                return
            
            # Extract URL from output
            vercel_url = "https://fruits-report-private.vercel.app"
            today = datetime.now().strftime("%Y-%m-%d")
            report_url = f"{vercel_url}/semantic_search_{today}.html"
            
            # Step 4: Send success message with top 3 results
            top_3 = "\n".join([
                f"{i}. **{p['name']}** (score: {p.get('_score', 0):.2f})"
                for i, p in enumerate(products[:3], 1)
            ])
            
            success_msg = (
                f"✅ **Report Generated & Deployed!**\n\n"
                f"📊 Query: `{query}`\n"
                f"🔢 Products: {len(products)}\n\n"
                f"🎯 **Top 3 Results:**\n{top_3}\n\n"
                f"🌐 **View Full Report:**\n{report_url}\n\n"
                f"📋 **Index Page:**\n{vercel_url}"
            )
            
            await interaction.followup.send(success_msg)
            
            logger.info(
                f"📝 Report | user:{interaction.user.name} | query:{query} | products:{len(products)} | url:{report_url}"
            )

        except Exception as e:
            logger.error(f"❌ Report command error: {e}")
            await interaction.followup.send(f"❌ Error: {str(e)[:200]}")


@bot.event
async def on_ready():
    """Bot ready event"""
    global es_client
    try:
        if es_client is None:
            es_client = FreshProduceClient()
        await bot.tree.sync()
        logger.info(f"✅ Bot logged in as {bot.user}")
        logger.info(f"✅ Slash commands synced")
    except Exception as e:
        logger.error(f"❌ Bot setup error: {e}")


@bot.event
async def on_command_error(ctx, error):
    """Global error handler"""
    logger.error(f"❌ Command error: {error}")
    await ctx.send(f"❌ An error occurred: {str(error)[:100]}")


async def main():
    """Main entry point"""
    # Load cog
    await bot.add_cog(FreshProduceBot(bot))

    # Get token
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError("Missing DISCORD_TOKEN in environment variables")

    # Run bot
    await bot.start(token)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
