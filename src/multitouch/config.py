



class ProjectConfig:
    def __init__(self, project_directory: str, database_name: str):
        # Strip trailing slash
        self.project_directory = project_directory.rstrip("/")
        self.database_name = database_name


    @property
    def is_workspace_path(self) -> bool:
        return self.project_directory.startswith("/Workspace") or self.project_directory.startswith("/workspace")

    @property
    def is_volume_path(self) -> bool:
        return self.project_directory.startswith("/Volumes")


    @property
    def data_gen_path(self) -> str:
        # If Volume path, return as is (PYTHON open() needs strict path)
        if self.is_volume_path:
            return f"{self.project_directory}/raw/attribution_data.csv"

        # If Workspace path, return as is (Python os APIs work directly)
        if self.is_workspace_path:
            return f"{self.project_directory}/raw/attribution_data.csv"
            
        # Local file API: Needs /dbfs prefix
        # If input already has /dbfs, don't add it again
        if self.project_directory.startswith("/dbfs"):
            return f"{self.project_directory}/raw/attribution_data.csv"
        return f"/dbfs{self.project_directory}/raw/attribution_data.csv"


    @property
    def raw_data_path(self) -> str:
        # If Volume path, return with dbfs: prefix for Spark
        if self.is_volume_path:
             return f"dbfs:{self.project_directory}/raw"

        # If Workspace path, return as FILE path for Spark
        if self.is_workspace_path:
            return f"file:{self.project_directory}/raw/attribution_data.csv"
            
        # Spark API: Needs dbfs: prefix
        # If input starts with /dbfs, strip it to get the 'logical' path
        clean_path = self.project_directory
        if clean_path.startswith("/dbfs"):
            clean_path = clean_path.replace("/dbfs", "", 1)
        
        return f"dbfs:{clean_path}/raw"

    @property
    def bronze_tbl_path(self) -> str:
        if self.is_volume_path:
            return f"dbfs:{self.project_directory}/bronze"
            
        if self.is_workspace_path:
             return f"{self.project_directory}/bronze"

        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/bronze"
    
    @property
    def gold_user_journey_tbl_path(self) -> str:
        if self.is_volume_path:
            return f"{self.project_directory}/gold_user_journey"

        if self.is_workspace_path:
             return f"{self.project_directory}/gold_user_journey"

        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_user_journey"

    @property
    def gold_attribution_tbl_path(self) -> str:
        if self.is_volume_path:
            return f"{self.project_directory}/gold_attribution"

        if self.is_workspace_path:
             return f"{self.project_directory}/gold_attribution"

        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_attribution"

    @property
    def gold_ad_spend_tbl_path(self) -> str:
        if self.is_volume_path:
            return f"{self.project_directory}/gold_ad_spend"

        if self.is_workspace_path:
             return f"{self.project_directory}/gold_ad_spend"

        clean_path = self.project_directory.replace("/dbfs", "", 1) if self.project_directory.startswith("/dbfs") else self.project_directory
        return f"dbfs:{clean_path}/gold_ad_spend"
