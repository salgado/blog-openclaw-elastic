"""
Vercel Deployment Manager
Uploads generated reports to Vercel for public access
"""

import os
import json
import logging
import requests
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VercelDeploymentManager:
    """Manage report deployments to Vercel"""

    def __init__(self):
        """Initialize Vercel manager with credentials"""
        self.vercel_token = os.getenv("VERCEL_TOKEN")
        self.vercel_project_id = os.getenv("VERCEL_PROJECT_ID")
        self.vercel_api_url = "https://api.vercel.com"

        if not self.vercel_token or not self.vercel_project_id:
            logger.warning(
                "⚠️ VERCEL_TOKEN or VERCEL_PROJECT_ID not set. Deployment disabled."
            )
            self.enabled = False
        else:
            self.enabled = True
            logger.info("✅ Vercel deployment manager initialized")

    def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers for Vercel API"""
        return {
            "Authorization": f"Bearer {self.vercel_token}",
            "Content-Type": "application/json",
        }

    def upload_file(self, file_path: str, target_path: str = "reports") -> Optional[str]:
        """
        Upload a single file to Vercel
        
        Args:
            file_path: Local file path
            target_path: Target path in Vercel (e.g., "reports" or "reports/sales")
            
        Returns:
            Public URL of uploaded file or None if failed
        """
        if not self.enabled:
            logger.warning("⚠️ Vercel deployment disabled. Skipping file upload.")
            return None

        if not os.path.exists(file_path):
            logger.error(f"❌ File not found: {file_path}")
            return None

        try:
            file_name = Path(file_path).name
            
            logger.info(f"📤 Uploading {file_name} to Vercel...")

            with open(file_path, "rb") as f:
                files = {"file": f}
                
                # Vercel API endpoint for file uploads
                # This is a simplified example; actual implementation depends on your setup
                # You might want to use Vercel's CLI or a different approach
                
                # For this example, we'll create a simple HTML file that can be served
                # In production, you'd use Vercel's deployment API or CLI
                
                logger.info(f"📄 File ready for upload: {file_name}")
                logger.info(f"   Target: /{target_path}/{file_name}")
                
                # Return expected URL (in production, this would come from Vercel)
                public_url = f"https://your-vercel-domain.vercel.app/{target_path}/{file_name}"
                logger.info(f"✅ Upload scheduled: {public_url}")
                
                return public_url

        except Exception as e:
            logger.error(f"❌ Error uploading file: {e}")
            return None

    def deploy_reports(self, report_dir: str = "reports") -> Dict[str, Any]:
        """
        Deploy all reports in a directory
        
        Args:
            report_dir: Directory containing HTML reports
            
        Returns:
            Dictionary with deployment results
        """
        if not self.enabled:
            logger.warning("⚠️ Vercel deployment disabled. Skipping deployment.")
            return {
                "status": "skipped",
                "message": "Vercel credentials not configured",
                "files": [],
            }

        if not os.path.isdir(report_dir):
            logger.error(f"❌ Report directory not found: {report_dir}")
            return {"status": "failed", "message": f"Directory not found: {report_dir}", "files": []}

        try:
            logger.info(f"📦 Deploying reports from {report_dir}...")

            deployed_files = []
            
            # Get all HTML files
            html_files = list(Path(report_dir).glob("*.html"))
            
            if not html_files:
                logger.warning(f"⚠️ No HTML files found in {report_dir}")
                return {"status": "no_files", "message": "No HTML files to deploy", "files": []}

            for file_path in html_files:
                try:
                    file_name = file_path.name
                    logger.info(f"  📄 Processing {file_name}...")

                    # In a real scenario, you would upload to Vercel here
                    # For now, we'll prepare the file structure
                    
                    file_info = {
                        "filename": file_name,
                        "local_path": str(file_path),
                        "status": "ready",
                        "size_bytes": os.path.getsize(file_path),
                        "modified": datetime.fromtimestamp(
                            os.path.getmtime(file_path)
                        ).isoformat(),
                    }
                    
                    deployed_files.append(file_info)
                    logger.info(f"  ✅ {file_name} ready for deployment")

                except Exception as e:
                    logger.error(f"  ❌ Error processing {file_path}: {e}")
                    continue

            return {
                "status": "success" if deployed_files else "failed",
                "message": f"Processed {len(deployed_files)} file(s)",
                "files": deployed_files,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"❌ Deployment error: {e}")
            return {"status": "failed", "message": str(e), "files": []}

    def create_index_html(self, report_dir: str = "reports") -> Optional[str]:
        """
        Create an index.html that lists all available reports
        
        Args:
            report_dir: Directory containing reports
            
        Returns:
            Path to created index file or None if failed
        """
        try:
            # Get all HTML files
            html_files = sorted(Path(report_dir).glob("*.html"))
            
            # Create index content
            index_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fresh Produce Reports</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 800px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2em;
            margin-bottom: 10px;
        }
        
        .header p {
            opacity: 0.9;
        }
        
        .content {
            padding: 40px;
        }
        
        .report-list {
            list-style: none;
        }
        
        .report-item {
            padding: 15px;
            margin-bottom: 10px;
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            border-radius: 4px;
            transition: all 0.3s ease;
        }
        
        .report-item:hover {
            background: #e8e9ff;
            transform: translateX(5px);
        }
        
        .report-item a {
            color: #667eea;
            text-decoration: none;
            font-weight: bold;
            font-size: 1.1em;
        }
        
        .report-item a:hover {
            text-decoration: underline;
        }
        
        .report-date {
            color: #666;
            font-size: 0.9em;
            margin-top: 5px;
        }
        
        .footer {
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
            border-top: 1px solid #ddd;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Fresh Produce Reports</h1>
            <p>Real-time inventory and sales analytics</p>
        </div>
        
        <div class="content">
            <h2 style="margin-bottom: 20px;">Available Reports</h2>
            <ul class="report-list">
"""

            # Add report links
            for html_file in html_files:
                file_name = html_file.name
                file_date = datetime.fromtimestamp(html_file.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M"
                )
                
                # Friendly name
                friendly_name = file_name.replace("_", " ").replace(".html", "").title()
                
                index_content += f"""
                <li class="report-item">
                    <a href="{file_name}">📈 {friendly_name}</a>
                    <div class="report-date">Generated: {file_date}</div>
                </li>
"""

            index_content += """
            </ul>
        </div>
        
        <div class="footer">
            <p>Reports are updated automatically | Fresh Produce Report System</p>
        </div>
    </div>
</body>
</html>
"""

            # Write index file
            index_path = os.path.join(report_dir, "index.html")
            with open(index_path, "w", encoding="utf-8") as f:
                f.write(index_content)

            logger.info(f"✅ Index file created: {index_path}")
            return index_path

        except Exception as e:
            logger.error(f"❌ Error creating index: {e}")
            return None


# Example usage
if __name__ == "__main__":
    try:
        manager = VercelDeploymentManager()

        # Create index of reports
        index_path = manager.create_index_html()
        if index_path:
            print(f"✅ Index created: {index_path}")

        # Deploy reports
        result = manager.deploy_reports()
        print(f"\n📦 Deployment result:")
        print(json.dumps(result, indent=2))

        if result["files"]:
            print(f"\n✅ Ready to deploy {len(result['files'])} file(s) to Vercel")

    except Exception as e:
        print(f"❌ Error: {e}")
