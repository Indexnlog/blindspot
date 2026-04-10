## Data Layout

This repository can run with local sample data, but large working datasets should live outside the repo.

Recommended setup:

```bash
BLINDSPOT_DATA_ROOT=D:/blindspot-data
```

With that setting:

- DART input files are read from `D:/blindspot-data/dart_subsidiaries`
- OpenCorporates results are written to `D:/blindspot-data/opencorporates`

Repository-local `data/` is best treated as:

- sample inputs
- small checked-in outputs
- development fallback when the external drive is not attached

For large production runs, prefer the external data root so the git repository stays small.
