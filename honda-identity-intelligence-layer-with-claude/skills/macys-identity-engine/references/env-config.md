# Environment Configuration

## Region Config (per region)
```bash
REGION_{PREFIX}_NAME          # Display name ("Macys", "US", "Middle East")
REGION_{PREFIX}_TENANT        # Amperity subdomain (e.g., "macys")
REGION_{PREFIX}_TOKEN_URL     # https://{tenant}.amperity.com/api/v0/oauth2/token
REGION_{PREFIX}_CLIENT_ID     # OAuth2 client ID
REGION_{PREFIX}_CLIENT_SECRET # OAuth2 client secret
REGION_{PREFIX}_DATABASE_ID   # C360 database ID
REGION_{PREFIX}_SEGMENT_ID    # Draft SQL segment ID
REGION_{PREFIX}_DATASET_ID    # From browser DevTools network tab
```

## Current Macy's Config
```bash
REGION_MACYS_NAME=Macys
REGION_MACYS_TENANT=macys
REGION_MACYS_DATABASE_ID=db-gF8c22ZVbBB
```

## MCP Bridge Config (v2)
```bash
MCP_BRIDGE_URL=http://127.0.0.1:5081
```

## Snowflake (set in mcp_bridge.py, not .env)
```
SNOWFLAKE_ACCOUNT=xpahnkf-amperity-data-warehouse
SNOWFLAKE_USER=your.email@amperity.com
SNOWFLAKE_DATABASE=PROD
SNOWFLAKE_WAREHOUSE=NEMO_READ
SNOWFLAKE_ROLE=FISHBOWL_R
```
