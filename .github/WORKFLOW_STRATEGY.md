# GitHub Actions Workflow Strategy

## 📊 Workflow Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     GIT FLOW STRATEGY                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  feature/xxx                                                    │
│      ↓                                                          │
│  Pull Request (to develop)                                     │
│      ↓                                                          │
│  ✅ CI Pipeline Checks (pipeline.yml)                          │
│      • Linting                                                 │
│      • Unit Tests                                              │
│      • Security Scan                                           │
│      • Docker Build                                            │
│      • Validation                                              │
│      ↓                                                          │
│  ✅ Code Review + Approval                                     │
│      ↓                                                          │
│  Merge to develop                                              │
│      ↓                                                          │
│  ✅ Staging Deployment (Optional)                              │
│                                                                 │
│  ┌──────────────────────────────────────────────┐              │
│  │  When Ready for Release (manual trigger)     │              │
│  │  Create Release PR: develop → main           │              │
│  └──────────────────────────────────────────────┘              │
│      ↓                                                          │
│  Merge to main + Tag (v1.0.0)                                 │
│      ↓                                                          │
│  ✅ Deploy Pipeline (deploy.yml)                              │
│      • Docker Push to Registry                                │
│      • Smoke Tests                                            │
│      • Create Release Notes                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Workflow Files

### 1. **pipeline.yml** (CI - Continuous Integration)
**Triggers**: `push` to main/develop OR `pull_request` to main/develop

**Jobs**:
- **lint**: Code quality checks (Black, isort, Flake8, Ruff)
- **test**: Unit tests with pytest and coverage reporting
- **dependencies**: Security scanning (Safety, Bandit)
- **build**: Docker image building (Airflow, API, Bot)
- **integration-test**: Integration tests using docker-compose
- **validate-dags**: Airflow DAG syntax validation
- **notify**: Final status notification

**When it runs**:
- ✅ Every push to `main` or `develop`
- ✅ Every pull request to `main` or `develop`
- ✅ Automatically gates code quality before merging

---

### 3. **dependabot.yml** (Dependabot PR Checks)
**Triggers**: `pull_request` from dependabot[bot]

**Purpose**: Lightweight checks for dependency update PRs
- **Safety scans** for security vulnerabilities
- **Basic syntax validation** for Python files
- **Auto-approval** for passing dependabot PRs

**When it runs**:
- ✅ Only on dependabot PRs
- ✅ Skips heavy Docker builds and integration tests
- ✅ Auto-merges passing dependency updates

---

## 🎯 Recommended Development Workflow

### Step 1: Create Feature Branch
```bash
git checkout develop
git pull origin develop
git checkout -b feature/add-job-alerts
```

### Step 2: Commit & Push
```bash
git add .
git commit -m "feat: add Discord job alerts"
git push origin feature/add-job-alerts
```

### Step 3: Create Pull Request
- GitHub will automatically run **pipeline.yml**
- All checks must pass before merging
- Require at least 1 code review approval

### Step 4: Merge to Develop
- After approval, merge the PR
- All services are ready for staging tests

### Step 5: Release to Production
```bash
# Create release PR
git checkout main
git pull origin main
git checkout -b release/v1.0.0
git merge develop

# Tag the release
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin main --tags
```

---

## 📋 Branch Protection Rules

Protect your branches with these GitHub settings:

**For `develop` branch**:
- ✅ Require status checks to pass: `lint`, `test`, `validate-dags`
- ✅ Require code reviews before merging (minimum 1)
- ✅ Dismiss stale pull request approvals

**For `main` branch**:
- ✅ Require all status checks to pass
- ✅ Require code reviews (minimum 2)
- ✅ Require status checks to pass before merging
- ✅ Require branches to be up to date before merging
- ✅ Include administrators in restrictions

---

## 🛠️ Local Setup for Testing

Before pushing, test locally:

```bash
# Install pre-commit hooks (optional but recommended)
pip install pre-commit
pre-commit install

# Run linting
black .
isort .
flake8 .

# Run tests
pytest tests/

# Run security scan
bandit -r airflow/ api/ bot/

# Build and test with Docker
docker compose up --build
```

---

## 📦 Docker Registry Configuration

### Enable Container Registry
1. Go to GitHub repo → Settings → Actions → General
2. Grant `GITHUB_TOKEN` permission to write packages
3. Workflows can automatically push to `ghcr.io`

### Manual Push Example
```bash
docker build -t ghcr.io/yourusername/job-pulse-api:latest ./api
docker push ghcr.io/yourusername/job-pulse-api:latest
```

---

## 🔐 Secrets Management

Add to GitHub Secrets (Settings → Secrets and Variables → Actions):

```
DISCORD_WEBHOOK_URL      # Discord notifications
DOCKER_USERNAME          # Docker Hub (if needed)
DOCKER_PASSWORD          # Docker Hub (if needed)
AIRFLOW_DB_PASSWORD      # Production DB credentials
OLLAMA_API_KEY          # LLM API key
MONGODB_URI             # MongoDB connection
```

Usage in workflows:
```yaml
env:
  DISCORD_WEBHOOK: ${{ secrets.DISCORD_WEBHOOK_URL }}
```

---

## 📊 Monitoring & Troubleshooting

### Check Workflow Status
- Go to **Actions** tab in GitHub
- Click on workflow run to see detailed logs
- Each job shows pass/fail status

### Common Issues

**Docker build fails**:
- Check Dockerfile syntax: `docker build ./service`
- Verify all dependencies in requirements.txt
- Check Docker build context

**Tests fail**:
- Run locally: `pytest tests/`
- Check test database connections
- Verify environment variables in `.env.example`

**Linting errors**:
```bash
black --format airflow/ api/ bot/
isort airflow/ api/ bot/
```

---

## � Dependabot PR Management

### **Common Issues & Solutions**

**Problem**: Too many concurrent Dependabot PRs causing workflow failures
```yaml
# ❌ Bad: All ecosystems update on Monday
schedule:
  interval: "weekly"
  day: "monday"
```

**Solution**: Stagger updates across the week
```yaml
# ✅ Good: Spread updates across different days
- day: "monday"    # Airflow dependencies
- day: "tuesday"   # API dependencies
- day: "wednesday" # Bot dependencies
- day: "thursday"  # Docker images
- day: "friday"    # GitHub Actions
```

### **Managing Multiple PRs**

Use the management script:
```bash
chmod +x scripts/manage-dependabot-prs.sh
./scripts/manage-dependabot-prs.sh
```

**Manual Management**:
```bash
# List all dependabot PRs
gh pr list --author dependabot --state open

# Auto-merge passing PRs
gh pr merge --auto --squash <pr-number>

# Close outdated PRs
gh pr close <pr-number> --comment "Superseded by newer update"
```

### **Dependabot Configuration Best Practices**

1. **Group Updates**: Use `groups` to bundle related updates
2. **Limit PRs**: Set `open-pull-requests-limit` to 1-2 per ecosystem
3. **Stagger Schedules**: Spread updates across different days
4. **Auto-Merge**: Enable for patch/minor updates
5. **Review Majors**: Require manual review for major version updates

1. **Commit Often**: Push feature branches regularly
2. **Write Tests**: Aim for >80% code coverage
3. **Code Review**: Always have another person review
4. **Use Semantic Commits**: `feat:`, `fix:`, `docs:`, `refactor:`
5. **Tag Releases**: Use semantic versioning (v1.0.0)
6. **Document Changes**: Update README & changelog
7. **Keep Secrets Safe**: Never commit sensitive data
8. **Monitor Costs**: GitHub Actions has free limits (2,000 minutes/month)

---

## 📈 Performance Tips

### Faster CI/CD:

1. **Use Caching**:
   ```yaml
   - uses: actions/setup-python@v4
     with:
       cache: 'pip'
   ```

2. **Parallel Jobs**: Independent jobs run simultaneously

3. **Skip Unnecessary Jobs**:
   ```yaml
   if: github.event_name == 'pull_request'
   ```

4. **Use Smaller Images**: Optimize Dockerfile for layer caching

---

## 🔗 Useful Commands

```bash
# See all workflows
gh workflow list

# Trigger workflow manually
gh workflow run pipeline.yml

# View workflow run
gh run list
gh run view <run-id>

# View logs
gh run view <run-id> --log
```

---

## 📚 References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Workflow Syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)
- [Docker Actions](https://github.com/docker/build-push-action)
- [Git Flow Branching Model](https://nvie.com/posts/a-successful-git-branching-model/)
