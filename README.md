# DBT Core MCP Server

An MCP (Model Context Protocol) server for interacting with DBT (Data Build Tool) projects.

## Overview

This server provides tools to interact with DBT projects via the Model Context Protocol, enabling AI assistants to:
- Query DBT project metadata
- Inspect models, sources, and tests
- View compiled SQL
- Run DBT commands
- Access DBT documentation and lineage

## Installation

### From PyPI (when published)

The easiest way to use this MCP server is with `uvx` (no installation needed):

```bash
# Run directly with uvx (recommended)
uvx dbt-core-mcp
```

Or install it permanently:

```bash
# Using uv
uv tool install dbt-core-mcp

# Using pipx
pipx install dbt-core-mcp

# Or using pip
pip install dbt-core-mcp
```

## Usage

### Running the Server

```bash
# Run with default settings
dbt-core-mcp

# Enable debug logging
dbt-core-mcp --debug
```

The server automatically detects DBT projects from workspace roots provided by VS Code.

### Configuration for VS Code

Add to your VS Code MCP settings:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "uvx",
      "args": ["dbt-core-mcp"]
    }
  }
}
```

Or if you prefer `pipx`:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "pipx",
      "args": ["run", "dbt-core-mcp"]
    }
  }
}
```

### For the impatient (bleeding edge from GitHub)

If you want to always run the latest code directly from GitHub:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/NiclasOlofsson/dbt-core-mcp.git",
        "dbt-core-mcp"
      ]
    }
  }
}
```

Or with `pipx`:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "pipx",
      "args": [
        "run",
        "--no-cache",
        "--spec",
        "git+https://github.com/NiclasOlofsson/dbt-core-mcp.git",
        "dbt-core-mcp"
      ]
    }
  }
}
```

## Features (Planned)

- [ ] List DBT models
- [ ] Get model information and metadata
- [ ] View compiled SQL
- [ ] Run specific models
- [ ] Test models
- [ ] List sources
- [ ] View model lineage
- [ ] Access DBT documentation
- [ ] Query manifest.json
- [ ] Execute DBT commands

## Contributing

Want to help improve this server? Check out [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT License - see LICENSE file for details.

## Author

Niclas Olofsson - [GitHub](https://github.com/NiclasOlofsson)
