# Security Policy

This page covers vulnerability reporting and technical security boundaries. For
a first learning exercise with invented data, start with
[Your first research project](../guide/first-research-project.md).

## Supported Versions

GraphForge is pre-v1.0. The API is still maturing, and only the **latest release** receives security updates. If you discover a vulnerability, please update to the latest version first.

| Version                  | Supported          |
| ------------------------ | ------------------ |
| Latest published release | :white_check_mark: |
| Older releases           | :x:                |

We will begin supporting multiple versions once a stable v1.0 API is reached.

## Reporting a Vulnerability

We take the security of GraphForge seriously. If you discover a security vulnerability, please follow these steps:

### 1. **Do Not** Open a Public Issue

Please do not report security vulnerabilities through public GitHub issues.

### 2. Report Privately

Use GitHub's private vulnerability reporting:

1. Go to the [Security tab](https://github.com/CurateLabs/graphforge/security)
2. Click "Report a vulnerability"
3. Fill out the form with details

### 3. Include These Details

Please include as much information as possible:

- **Type of vulnerability** (e.g., SQL injection, XSS, privilege escalation)
- **Full paths of affected source files**
- **Location of affected code** (tag/branch/commit or direct URL)
- **Step-by-step instructions to reproduce** the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact** of the vulnerability
- **Suggested fix** (if you have one)

### 4. What to Expect

Maintainers use the private report thread to assess the vulnerability, discuss
a fix, and coordinate disclosure with the reporter. Include your preference
for attribution in that thread.

## Security Update Process

1. **Vulnerability confirmed:** We verify the issue and assess severity
2. **Fix developed:** A patch is developed and tested
3. **Advisory drafted:** Security advisory prepared (GitHub Security Advisories)
4. **Release:** Patched version released with security notes
5. **Disclosure:** Coordinate publication of the advisory and mitigation guidance

## Security Best Practices for Users

When using GraphForge:

### Input Validation

Always validate and sanitize user input before passing to Cypher queries:

```python
from graphforge import GraphForge

db = GraphForge()

# ❌ DON'T: Direct user input in queries (injection risk)
user_input = request.form['name']
db.execute(f"MATCH (n:Person {{name: '{user_input}'}}) RETURN n")

# ✅ DO: Use parameterized queries or validate input
db.execute("MATCH (n:Person) WHERE n.name = $name RETURN n", {"name": user_input})
```

### Project files and sharing

A durable GraphForge project is a directory containing graph data and metadata.
Protect that directory and any exported packages using the operating system's
file permissions. A private hosted project controls who can access it; it does
not make a downloaded copy inaccessible to its recipient. Public projects make
participation and contributions visible.

Use the [portable project workflow](../guide/portable-projects.md) to move or
share a project. Access control for a hosted service belongs to that service;
local research labels and governance records do not enforce access control.

### Atomic Writes

Every `execute()` write publishes atomically. When graph and knowledge mutations
must share one committed generation, validate and submit one
`publish_composite_transaction()` request, or stage supported mutation families
through `begin_transaction()` / `commit()` / `rollback()` on a
`GraphTransaction` handle. Administrative families classified as rejected cannot
join an explicit transaction.

### Dependency Security

Keep dependencies updated:

```bash
# Check for vulnerabilities
pip install safety
safety check

# Update dependencies
uv sync --upgrade
```

## Known Security Considerations

### Local engine and storage

The Rust engine runs inside the calling application. Graph data uses Parquet
and metadata uses JSON; graph results cross language boundaries as Arrow.
GraphForge is not a hosted authentication or tenant-isolation service. An
application exposing it to other users must enforce its own access rules.

Durable project admission, publication, recovery, and supported write modes are
defined in [concurrency and recovery](../book/architecture/concurrency-recovery.md).
These guarantees depend on supported local filesystems and do not replace
operating-system file permissions.

### Query resources

The engine has per-instance execution resource controls, including worker
counts, a DataFusion memory pool, spill policy, and query admission. Their scope
is described in [execution resource policy](../development/execution-resource-policy.md).
These controls are not a sandbox for arbitrary code or a universal cap on every
allocation in the host application. Hosts remain responsible for deciding which
queries and operations their users may run.

### Imported data

Portable project verification checks format, integrity, and compatibility.
It does not establish that a source is trustworthy or that its research claims
are correct. Use the documented [verify and import workflow](../guide/portable-projects.md)
and inspect the source and selected content before reusing it.

## Security-Related Configuration

### Bandit (Security Linting)

Security scanning is configured in CI:

```bash
# Run security checks locally
bandit -c pyproject.toml -r crates/graphforge-bindings-py/python
```

### Dependency Scanning

Automated dependency updates via Dependabot:

```bash
# Manual security audit
pip install safety
safety check
```

## Disclosure Policy

- **Coordinated disclosure:** We follow responsible disclosure practices
- **CVE assignment:** We'll request CVEs for significant vulnerabilities
- **Security advisories:** Published on GitHub Security Advisories

## Attribution

We believe in recognizing security researchers who help improve GraphForge. With your permission, we'll:

- Credit you in security advisories
- Mention you in release notes

## Additional Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE Top 25](https://cwe.mitre.org/top25/)
- [GitHub Security Best Practices](https://docs.github.com/en/code-security)

---

**Last Updated:** 2026-05-06

Thank you for helping keep GraphForge secure!
