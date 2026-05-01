#!/bin/bash

# Dependabot PR Management Script
# This script helps manage multiple dependabot PRs

set -e

echo "🔍 Checking for open Dependabot PRs..."

# Get all open dependabot PRs
PRS=$(gh pr list --author dependabot --state open --json number,title,headRefName --jq '.[] | @base64')

if [ -z "$PRS" ]; then
    echo "✅ No open Dependabot PRs found."
    exit 0
fi

echo "📋 Found the following Dependabot PRs:"
echo "$PRS" | while read -r pr; do
    PR_DATA=$(echo "$pr" | base64 -d)
    NUMBER=$(echo "$PR_DATA" | jq -r '.number')
    TITLE=$(echo "$PR_DATA" | jq -r '.title')
    BRANCH=$(echo "$PR_DATA" | jq -r '.headRefName')
    echo "  #$NUMBER: $TITLE (branch: $BRANCH)"
done

echo ""
echo "🔄 Processing Dependabot PRs..."

# Process each PR
echo "$PRS" | while read -r pr; do
    PR_DATA=$(echo "$pr" | base64 -d)
    NUMBER=$(echo "$PR_DATA" | jq -r '.number')
    TITLE=$(echo "$PR_DATA" | jq -r '.title')

    echo "Processing PR #$NUMBER: $TITLE"

    # Check if PR has conflicts
    if gh pr view "$NUMBER" --json mergeStateStatus | jq -r '.mergeStateStatus' | grep -q "CONFLICTING"; then
        echo "  ❌ PR #$NUMBER has merge conflicts. Skipping."
        continue
    fi

    # Check if CI is passing
    CI_STATUS=$(gh pr view "$NUMBER" --json statusCheckRollup | jq -r '.statusCheckRollup | if . == null then "NO_CHECKS" else (.contexts | map(.state) | all(. == "SUCCESS")) end')

    if [ "$CI_STATUS" = "true" ]; then
        echo "  ✅ PR #$NUMBER CI is passing. Auto-merging..."
        gh pr merge "$NUMBER" --auto --squash
    elif [ "$CI_STATUS" = "NO_CHECKS" ]; then
        echo "  ⚠️  PR #$NUMBER has no CI checks. Manual review needed."
    else
        echo "  ❌ PR #$NUMBER CI is failing. Needs attention."
    fi
done

echo ""
echo "🎯 Dependabot PR management complete!"
echo ""
echo "💡 Tips:"
echo "  - Review failed PRs manually"
echo "  - Consider grouping similar updates"
echo "  - Adjust dependabot.yml schedule if too many PRs"