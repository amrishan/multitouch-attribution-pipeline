

class ProjectConfig:
    def __init__(self, project_directory: str, database_name: str):
        # Strip trailing slash
        self.project_directory = project_directory.rstrip("/")
        self.database_name = database_name

    @property
    def data_gen_path(self) -> str:
        # Local file API: Needs /dbfs prefix
        # If input already has /dbfs, don't add it again
        if self.project_directory.startswith("/dbfs"):
            return f"{self.project_directory}/raw/attribution_data.csv"
        return f"/dbfs{self.project_directory}/raw/attribution_data.csv"

    @property
    def raw_data_path(self) -> str:
        # Spark API: Needs dbfs: prefix
        # If input starts with /dbfs, strip it to get the 'logical' path
        clean_path = self.project_directory
        if clean_path.startswith("/dbfs"):
            clean_path = clean_path.replace("/dbfs", "", 1)
        
        return f"dbfs:{clean_path}/raw"

    @property
    def bronze_tbl_path(self) -> str:
        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/bronze"
    
    @property
    def gold_user_journey_tbl_path(self) -> str:
        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_user_journey"

    @property
    def gold_attribution_tbl_path(self) -> str:
        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_attribution"

    @property
    def gold_ad_spend_tbl_path(self) -> str:
        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_ad_spend"
