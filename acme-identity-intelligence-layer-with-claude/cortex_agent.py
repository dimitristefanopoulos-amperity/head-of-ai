"""
Cortex Agent client - calls MCP bridge service via HTTP.
"""
import os
import requests
from typing import Optional, Dict, Any, List


class CortexAgent:
    """HTTP client for MCP Bridge service."""

    def __init__(self, bridge_url: Optional[str] = None):
        """
        Initialize Cortex Agent client.

        Args:
            bridge_url: URL of MCP bridge service (default: http://127.0.0.1:5081)
        """
        self.bridge_url = bridge_url or os.getenv("MCP_BRIDGE_URL", "http://127.0.0.1:5081")
        self._check_connection()

    def _check_connection(self):
        """Check if bridge service is reachable."""
        try:
            response = requests.get(f"{self.bridge_url}/health", timeout=2)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(
                f"Cannot connect to MCP bridge at {self.bridge_url}. "
                f"Make sure mcp_bridge.py is running. Error: {e}"
            )

    def query(
        self,
        question: str,
        agent: str = "nemo",
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Query a Cortex Agent via the MCP bridge.

        Args:
            question: User question/query
            agent: Agent name (nemo, nemosupport, nemoclientrelations)
            context: Optional context dict (e.g., tenant, region info)

        Returns:
            Dict with response text and metadata
        """
        try:
            response = requests.post(
                f"{self.bridge_url}/ask",
                json={
                    "question": question,
                    "agent": agent,
                    "context": context
                },
                timeout=180
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            return {
                "response": "Request timed out",
                "agent": agent,
                "error": "timeout"
            }
        except requests.exceptions.RequestException as e:
            return {
                "response": f"Error calling bridge: {str(e)}",
                "agent": agent,
                "error": str(e)
            }

    def list_agents(self) -> List[Dict[str, str]]:
        """
        List available Cortex Agents.

        Returns:
            List of agent info dicts
        """
        try:
            response = requests.get(f"{self.bridge_url}/agents", timeout=5)
            response.raise_for_status()
            return response.json().get("agents", [])

        except requests.exceptions.RequestException as e:
            return [
                {
                    "id": "error",
                    "name": "Error",
                    "description": f"Could not fetch agents: {e}"
                }
            ]

    def close(self):
        """No cleanup needed for HTTP client."""
        pass
