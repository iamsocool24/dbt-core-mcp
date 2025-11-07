"""
DBT Core MCP Server Implementation.

This server provides tools for interacting with DBT projects via the Model Context Protocol.
"""

import logging
import os

from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware
from fastmcp.server.middleware.timing import TimingMiddleware

logger = logging.getLogger(__name__)


class DBTCoreMCPServer:
    """
    DBT Core MCP Server.

    Provides tools for interacting with DBT projects.
    """

    def __init__(self) -> None:
        """Initialize the server.

        DBT project directories will be detected from MCP workspace roots during initialization.
        """
        # FastMCP initialization with recommended arguments
        from . import __version__

        self.app = FastMCP(
            version=__version__,
            name="DBT Core MCP",
            instructions="""DBT Core MCP Server for interacting with DBT projects.

            This server provides tools to:
            - Query DBT project metadata
            - Run DBT commands
            - Inspect models, sources, and tests
            - View compiled SQL
            - Access DBT documentation

            Usage:
            - Use the tools to interact with your DBT project
            - Query model lineage and dependencies
            - Run and test DBT models
            """,
            on_duplicate_resources="warn",
            on_duplicate_prompts="replace",
            include_fastmcp_meta=True,  # Include FastMCP metadata for clients
        )

        # DBT project directories will be set from workspace roots during MCP initialization
        self.project_dirs: list[str] = []
        self.profiles_dir = os.path.expanduser("~/.dbt")

        # Add built-in FastMCP middleware (2.11.0)
        self.app.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))
        self.app.add_middleware(TimingMiddleware())  # Time actual execution
        self.app.add_middleware(LoggingMiddleware(include_payloads=True, max_payload_length=1000))

        # Register tools
        self._register_tools()

        logger.info("DBT Core MCP Server initialized")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    def _register_tools(self) -> None:
        """Register all DBT tools."""
        # TODO: Implement DBT-specific tools
        # Examples:
        # - list_models
        # - get_model_info
        # - run_model
        # - test_model
        # - get_compiled_sql
        # - list_sources
        # - etc.

        @self.app.tool()
        def get_project_info() -> dict[str, object]:
            """Get information about the DBT project.

            Returns:
                Dictionary with project information
            """
            return {
                "project_dirs": self.project_dirs,
                "profiles_dir": self.profiles_dir,
                "status": "initialized",
            }

        logger.info("Registered DBT tools")

    def run(self) -> None:
        """Run the MCP server."""
        self.app.run()


def create_server() -> DBTCoreMCPServer:
    """Create a new DBT Core MCP server instance.

    Returns:
        DBTCoreMCPServer instance
    """
    return DBTCoreMCPServer()
