# Release Captain

An agent built on TrueForge that prepares software releases and stops for human approval before publishing.

## What it does

1. Fetches all commits since the last release tag on this GitHub repository
2. Runs the project's test suite inside a sandboxed environment
3. Drafts release notes summarizing the changes
4. Pauses and asks for explicit approval before tagging or publishing anything
5. Only tags and publishes after the human says yes

## Why

Release prep is a routine but risky chore. This agent handles the repetitive parts but never takes the irreversible step without a human in the loop.

## Setup

1. Run TrueForge locally: npx @truefoundry/trueforge
2. Open http://localhost:8790
3. Configure a model provider under Settings then Models
4. Connect GitHub under Settings then Connectors, using a fine-grained token scoped to this repository
5. Go to Build Agent, paste in the instructions from agent-instructions.txt, and attach the GitHub MCP tools
6. Start a session and try: "Fetch the latest release tag and list commits since then."

## Built with

TrueForge and Claude (Anthropic) as an AI coding assistant.

## Demo repo

This is the repo used for the live demo, a distributed rate limiter in Go.
