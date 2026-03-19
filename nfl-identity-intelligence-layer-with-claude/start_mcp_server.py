#!/usr/bin/env python3
"""
Patched snowflake-labs-mcp server wrapper.

Applies the critical SSE response parser fix from the Nemo plugin
without the broken create_mcp_server import.

Patches applied:
1. Cortex Agent timeout: 120s -> 300s
2. SSE response parser: handles all Cortex Agent response formats
3. Response filtering: strips internal trace, returns only final text
"""

import json
import sys

import requests as _requests

# ---------------------------------------------------------------------------
# Patch 1: Increase cortex_agent HTTP timeout to 5 minutes
# ---------------------------------------------------------------------------

_original_post = _requests.post


def _patched_post(url, **kwargs):
    if "/agents/" in str(url) and ":run" in str(url):
        kwargs["timeout"] = 300
    return _original_post(url, **kwargs)


_requests.post = _patched_post


# ---------------------------------------------------------------------------
# Patch 2: Robust Cortex Agent SSE response parser
# ---------------------------------------------------------------------------

from mcp_server_snowflake.utils import AgentResponse, SnowflakeResponse  # noqa: E402


def _extract_text_only(content):
    """Extract only text items from a content array, discarding internal trace."""
    if not isinstance(content, list):
        return []
    texts = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text":
            text = item.get("text", "")
            if text:
                texts.append(text)
    return texts


def _extract_from_agent_json(data):
    """Try to extract final text from various JSON response structures."""
    # Format: {"role": "assistant", "content": [{"type":"text","text":"..."}, ...]}
    content = data.get("content", [])
    if isinstance(content, list) and content:
        texts = _extract_text_only(content)
        if texts:
            return "\n".join(texts)

    # Format: {"choices": [{"message": {"content": [...]}}]}
    choices = data.get("choices", [])
    if choices:
        msg_content = choices[0].get("message", {}).get("content", [])
        texts = _extract_text_only(msg_content)
        if texts:
            return "\n".join(texts)

    # Format: {"message": {"content": [...]}}
    msg = data.get("message", {})
    if isinstance(msg, dict):
        msg_content = msg.get("content", [])
        texts = _extract_text_only(msg_content)
        if texts:
            return "\n".join(texts)

    return None


def _patched_parse_agent_response(self, response_stream):
    """Parse Cortex Agent response — handles SSE and JSON formats."""
    content_type = response_stream.headers.get("content-type", "")

    # --- Non-streaming JSON response ---
    if "text/event-stream" not in content_type:
        try:
            data = response_stream.json()
            text = _extract_from_agent_json(data)
            if text:
                return AgentResponse(results=text).model_dump_json()
            return AgentResponse(results=json.dumps(data, indent=2)).model_dump_json()
        except Exception:
            return AgentResponse(
                results="Error: Could not parse agent JSON response."
            ).model_dump_json()

    # --- Streaming SSE response ---
    collected_text = []
    current_event = None

    for line in response_stream.iter_lines(decode_unicode=True):
        line = line.strip()

        if not line:
            current_event = None
            continue

        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
            continue

        if line.startswith("data:"):
            json_str = line[len("data:"):].strip()
            if json_str == "[DONE]":
                break

            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                continue

            if current_event == "metadata":
                continue

            # Format 1: event: response
            if current_event == "response":
                collected_text.extend(_extract_text_only(data.get("content", [])))

            # Format 2: event: text
            elif current_event == "text":
                text = data.get("text", "")
                if text:
                    collected_text.append(text)

            # Format 3: event: message.delta (OpenAI-style)
            elif current_event == "message.delta":
                delta_content = data.get("delta", {}).get("content", [])
                collected_text.extend(_extract_text_only(delta_content))

            # Format 4: no event type, data-only SSE
            elif current_event is None:
                choices = data.get("choices", [])
                for choice in choices:
                    delta = choice.get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        collected_text.append(content)

                collected_text.extend(_extract_text_only(data.get("content", [])))

                text = _extract_from_agent_json(data)
                if text:
                    collected_text.append(text)

    if collected_text:
        return AgentResponse(results="".join(collected_text)).model_dump_json()

    return AgentResponse(
        results="Agent returned no response content."
    ).model_dump_json()


SnowflakeResponse.parse_agent_response = _patched_parse_agent_response


# ---------------------------------------------------------------------------
# Launch the server
# ---------------------------------------------------------------------------

from mcp_server_snowflake.server import main  # noqa: E402

if __name__ == "__main__":
    main()
