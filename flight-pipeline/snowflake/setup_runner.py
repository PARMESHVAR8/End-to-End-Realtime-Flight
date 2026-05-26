#!/usr/bin/env python
"""
Snowflake Setup Runner
======================
Executes all SQL setup files in the correct order to initialize the database.

Usage:
    python snowflake/setup_runner.py

This script will:
1. Load all SQL files from snowflake/setup/ in alphabetical order
2. Execute them against your Snowflake account
3. Report success/failure for each file
4. Exit with status code 0 (success) or 1 (failure)
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Tuple
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from snowflake.connection import SnowflakeConnection
from monitoring.logging_config import setup_logging

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


def get_setup_files() -> List[Tuple[str, str]]:
    """
    Get all SQL setup files in order.
    
    Returns:
        List of (filename, full_path) tuples sorted alphabetically
    """
    setup_dir = Path(__file__).parent / "setup"
    
    if not setup_dir.exists():
        logger.error(f"Setup directory not found: {setup_dir}")
        return []
    
    sql_files = sorted(setup_dir.glob("*.sql"))
    return [(f.name, str(f)) for f in sql_files]


def execute_setup_file(sf: SnowflakeConnection, filename: str, filepath: str) -> bool:
    """
    Execute a single SQL setup file.
    
    Args:
        sf: Snowflake connection
        filename: Name of the file (for logging)
        filepath: Full path to the SQL file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(filepath, 'r') as f:
            sql_content = f.read()
        
        logger.info(f"Executing {filename}...")
        
        # Process the SQL to remove comments and filter statements
        lines = []
        for line in sql_content.split('\n'):
            # Skip comment lines
            stripped = line.strip()
            if stripped.startswith('--'):
                continue
            lines.append(line)
        
        sql_content = '\n'.join(lines)
        
        # Split by semicolons and filter
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        
        # Filter out SHOW commands and placeholder statements
        filtered_statements = []
        skipped = 0
        for stmt in statements:
            if stmt.upper().startswith('SHOW '):
                logger.debug(f"  Skipping SHOW command")
                skipped += 1
                continue
            if 'YOUR_USERNAME' in stmt:
                logger.info(f"  Skipping placeholder statement (requires manual setup)")
                skipped += 1
                continue
            filtered_statements.append(stmt)
        
        if skipped > 0:
            logger.info(f"  Skipped {skipped} statement(s)")
        
        # Execute all statements as one batch to preserve context
        combined_sql = ';\n'.join(filtered_statements) + ';'
        
        try:
            sf.execute(combined_sql)
            logger.info(f"✓ {filename} completed ({len(filtered_statements)} statements)")
            return True
        except Exception as e:
            error_msg = str(e).lower()
            
            # If the batch fails, try executing statements individually to get better error info
            logger.warning(f"Batch execution failed, retrying individually: {error_msg[:80]}")
            
            failed_count = 0
            for i, stmt in enumerate(filtered_statements, 1):
                try:
                    sf.execute(stmt)
                except Exception as stmt_error:
                    stmt_error_msg = str(stmt_error).lower()
                    
                    # Skip permission/authorization errors
                    if ("grant" in stmt.lower() and "not authorized" in stmt_error_msg) or \
                       ("grant" in stmt.lower() and "does not exist" in stmt_error_msg):
                        logger.warning(f"  ⚠ Statement {i}: Authorization issue (skipped)")
                        continue
                    
                    # For other errors, fail
                    logger.error(f"  ✗ Statement {i}: {stmt_error_msg[:100]}")
                    logger.error(f"     SQL: {stmt[:100]}")
                    failed_count += 1
            
            if failed_count > 0:
                logger.error(f"✗ {failed_count} statement(s) failed")
                return False
            else:
                logger.info(f"✓ {filename} completed (individual execution, {len(filtered_statements)} statements)")
                return True
    
    except Exception as e:
        logger.error(f"✗ {filename} failed: {e}")
        return False


def main():
    """Main entry point."""
    try:
        logger.info("=" * 60)
        logger.info("Snowflake Setup Runner")
        logger.info("=" * 60)
        
        # Get all setup files
        setup_files = get_setup_files()
        
        if not setup_files:
            logger.error("No SQL setup files found in snowflake/setup/")
            return 1
        
        logger.info(f"Found {len(setup_files)} setup files:")
        for filename, _ in setup_files:
            logger.info(f"  • {filename}")
        
        # Connect to Snowflake
        logger.info("\nConnecting to Snowflake...")
        sf = SnowflakeConnection()
        sf.connect()
        logger.info("✓ Connected to Snowflake")
        
        # Execute each setup file
        logger.info(f"\nExecuting setup files...")
        results = []
        
        for filename, filepath in setup_files:
            success = execute_setup_file(sf, filename, filepath)
            results.append((filename, success))
        
        # Close connection
        sf.close()
        logger.info("✓ Connection closed")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Setup Summary")
        logger.info("=" * 60)
        
        success_count = sum(1 for _, success in results if success)
        total_count = len(results)
        
        for filename, success in results:
            status = "✓" if success else "✗"
            logger.info(f"{status} {filename}")
        
        logger.info(f"\n{success_count}/{total_count} files completed successfully")
        
        if success_count == total_count:
            logger.info("\n✓ All setup files executed successfully!")
            logger.info("Your Snowflake database is ready.")
            logger.info("\nNext steps:")
            logger.info("  1. Verify the warehouse and database in Snowflake Web UI")
            logger.info("  2. Run: python -m transformation.run_transformation --limit 100")
            return 0
        else:
            logger.error(f"\n✗ {total_count - success_count} file(s) failed")
            logger.error("Check the errors above and review the SQL files")
            return 1
    
    except KeyboardInterrupt:
        logger.warning("\nSetup interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}")
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
