## Continuous Integration

This repository uses **GitHub Actions** to run the test suite on every push and pull request.

Workflow:
- Install dependencies from `requirements.txt`
- Run `python -m pytest`

You can see the latest build status in the **Actions** tab on GitHub.

## Analytics & Reports

Once the pipeline has loaded data into `warehouse.db`, you can generate analytics reports:

```bash
python -m src.run_analytics
# or
python -m src.run_analytics --limit 5
