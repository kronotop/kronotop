# Security scanning

Trivy scans the Maven dependencies, the Docker image built from `main` and the published Docker image. Results go
to Security > Code scanning under the `trivy-fs`, `trivy-image` and `trivy-image-published` categories. Findings
do not fail the build.

## Workflows

- `trivy-db-update.yaml`: downloads the Trivy DBs and saves them to the Actions cache as
  `cache-trivy-YYYY-MM-DD`. Runs daily at 08:00 UTC and on demand.
- `trivy.yaml`: runs `trivy fs .` and `trivy image` on push and pull requests to `main`, daily at 09:00 UTC,
  and on demand. It scans `ghcr.io/kronotop/kronotop:latest` only on the daily run and on demand. It reads the
  DBs from the cache and never downloads them.

## First-time setup

Run `Update Trivy DB cache` by hand once. The scans fail until a `cache-trivy-*` entry exists.

## Ignoring a finding

Create `.trivyignore.yaml` at the repository root and pass it to both scan steps with the `trivyignores` input.
Give every entry a reason and an expiry date:

```yaml
vulnerabilities:
  - id: CVE-2024-12345
    statement: The vulnerable code path is not used.
    expired_at: 2027-01-01
```

## Upgrading trivy-action

The update workflow must use the same cache key prefix (`cache-trivy-`) and directory (`.cache/trivy`) that
`trivy-action` restores from. The `oras pull` tags (`trivy-db:2`, `trivy-java-db:1`) must match the DB schema
version. Check both in the trivy-action README after every upgrade.

## Troubleshooting

If a scan fails because the DB is missing, run `Update Trivy DB cache` by hand and run the scan again.
