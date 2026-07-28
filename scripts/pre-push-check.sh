#!/bin/bash
# Pre-push validation script - runs the same checks as CI

set -e

echo "🔍 Running pre-push checks..."
echo ""

echo "📝 Checking code formatting..."
uv run ruff format --check .

echo ""
echo "🔎 Running linter..."
uv run ruff check .

echo ""
echo "🔬 Running type checker..."
uv run mypy src/graphforge --strict-optional --show-error-codes

echo ""
echo "✅ All checks passed! Safe to push."
