# Security Policy

## Supported Versions

GraphForge is pre-v1.0. The API is still maturing, and only the **latest release** receives security updates. If you discover a vulnerability, please update to the latest version first.

| Version | Supported |
| ------- | --------- |
| Latest (0.3.x) | :white_check_mark: |
| Older releases | :x: |

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

- **Acknowledgment:** We'll acknowledge your report within 48 hours
- **Assessment:** We'll assess the vulnerability and determine severity
- **Timeline:** We'll provide an estimated timeline for a fix
- **Updates:** We'll keep you informed of progress
- **Credit:** We'll credit you in the security advisory (unless you prefer to remain anonymous)

## Security Update Process

1. **Vulnerability confirmed:** We verify the issue and assess severity
2. **Fix developed:** A patch is developed and tested
3. **Advisory drafted:** Security advisory prepared (GitHub Security Advisories)
4. **Release:** Patched version released with security notes
5. **Disclosure:** Public disclosure after users have time to update (typically 7 days)

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

### File Permissions

Protect your database files:

```bash
# Set restrictive permissions on database files
chmod 600 mydata.db
```

### Transaction Security

Always use transactions for write operations:

```python
db.begin()
try:
    # Your operations
    db.commit()
except Exception:
    db.rollback()
    raise
```

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

### 1. SQLite Backend

GraphForge uses SQLite for persistence:

- **File-based:** Database files should have appropriate permissions
- **No network security:** SQLite doesn't have built-in network security
- **Single-user:** Not designed for concurrent multi-user access

### 2. Query Execution

- **No query timeout:** Long-running queries can cause DoS
- **No resource limits:** No built-in limits on memory/CPU usage
- **No access control:** No user authentication/authorization system

### 3. Serialization

- **MessagePack:** Uses msgpack for serialization (ensure trusted data only)

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
- **Public disclosure timeline:** 90 days from initial report or when patch is released
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
