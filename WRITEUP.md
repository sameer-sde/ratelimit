# Release Captain

**The problem:** Shipping a software release involves repetitive, error-prone manual work — reviewing what changed, running tests, writing release notes, then publishing. Teams often skip steps under time pressure, risking broken or undocumented releases.

**What it does:** Release Captain reads all commits since the last release tag on a connected GitHub repository, runs the project's test suite inside a sandbox, and drafts human-readable release notes. It then presents the proposed release and stops.

**Where it stops:** The agent never tags or publishes a release without explicit human approval. Tagging and publishing are irreversible actions, so the agent is instructed to pause, show exactly what it intends to do, and wait for a "yes" before proceeding. This is the one line it will not cross alone.

**Architecture:** Built on TrueForge, an open-source agent harness. The model (via a configured provider) handles reasoning and drafting. GitHub access is provided through TrueForge's built-in GitHub MCP connector, scoped with a fine-grained personal access token limited to a single repository. Test execution runs inside TrueForge's sandboxed execution environment, provisioned automatically when the agent needs to run code. The approval gate uses TrueForge's built-in human-checkpoint capability, triggered by explicit instructions rather than custom-built logic.

**How TrueForge was used:** TrueForge provided the full runtime — tool routing to GitHub, sandboxed code execution for running tests, and the approval-checkpoint mechanism — without needing to build any of that infrastructure from scratch.

**Real vs. mocked:** All GitHub interactions (commits, tags, releases) are against a real, live repository — github.com/sameer-sde/ratelimit — using real credentials. Test execution runs the repository's actual Go test suite in the sandbox, not simulated output.

**Known limits:** The agent currently handles a single repository and does not yet support multi-repo releases, semantic-version auto-increment logic, or publishing to a package registry beyond GitHub itself.
