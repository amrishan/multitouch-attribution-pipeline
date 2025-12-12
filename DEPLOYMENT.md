
# Deployment Guide: Databricks Community Edition (Free Tier)

Since you are using the **Free Edition** (Community Edition), the `databricks bundle deploy` command (CLI) often won't work because API access is restricted.

The best way to run this code is using **Databricks Git Folders (formerly Repos)**.

## Prerequisites
1.  A [Databricks Community Edition](https://community.cloud.databricks.com/login.html) account.
2.  The code pushed to your GitHub repository: `https://github.com/amrishan/multitouch-attribution-pipeline`.

## Step 1: Configure Git in Databricks
1.  Log in to Databricks.
2.  Go to **Settings** (User Icon top right) -> **User Settings**.
3.  Go to the **Linked Accounts** or **Git Integration** tab.
4.  Select **GitHub**.
5.  If using a private repo, you'll need a [GitHub Personal Access Token](https://github.com/settings/tokens) (Classic) with `repo` permissions. If public, you might just need to link the username.

## Step 2: Clone the Repository
1.  Click **Workspace** in the left sidebar.
2.  Click the **Home** icon (or your username folder).
3.  Right-click "Repos" (or "Git Folders") and select **Create** > **Git Folder (Repo)**.
4.  **Git URL**: Paste your repo URL:
    `https://github.com/amrishan/multitouch-attribution-pipeline.git`
5.  **Git Provider**: GitHub.
6.  Click **Create Repo**.

## Step 3: Run the Pipeline
The code handles library installation automatically using the first cell: `%pip install -e ../`.

1.  Navigate into the cloned folder: `multitouch-attribution-pipeline`.
2.  Open `notebooks/01_data_generation`.
3.  **Attach to Cluster**: If you don't have one, create a standard cluster (ex: Runtime 13.3 LTS ML).
4.  **Run All**: Click "Run All".
    *   *What this does*: It installs the `src/multitouch` package and generates the CSV data.
5.  Open `notebooks/02_ingest` and Run All.
    *   *What this does*: Ingests the CSVs into Delta Tables.
6.  Open `notebooks/03_prep` and Run All.
    *   *What this does*: Processes the data into the Gold Layer.
7.  Open `notebooks/04_markov` and Run All.
    *   *What this does*: Runs the attribution model.

## Troubleshooting "Free Tier" Limitations
*   **No Jobs?**: The Free Tier usually doesn't allow creating automated "Jobs" (Workflows) with the scheduler. You have to run the notebooks manually as described above.
*   **Cluster Limits**: You usually get one cluster. Ensure it is running before executing notebooks.
