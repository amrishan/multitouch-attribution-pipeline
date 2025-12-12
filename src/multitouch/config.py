
class ProjectConfig:
    def __init__(self, project_directory: str, database_name: str):
        self.project_directory = project_directory
        self.database_name = database_name

    @property
    def data_gen_path(self) -> str:
        # Note: Local file API on Databricks uses /dbfs prefix for DBFS paths
        return f"/dbfs{self.project_directory}/raw/attribution_data.csv"

    @property
    def raw_data_path(self) -> str:
        return f"dbfs:{self.project_directory}/raw"

    @property
    def bronze_tbl_path(self) -> str:
        return f"dbfs:{self.project_directory}/bronze"
    
    @property
    def gold_user_journey_tbl_path(self) -> str:
        return f"dbfs:{self.project_directory}/gold_user_journey"

    @property
    def gold_attribution_tbl_path(self) -> str:
        return f"dbfs:{self.project_directory}/gold_attribution"

    @property
    def gold_ad_spend_tbl_path(self) -> str:
        return f"dbfs:{self.project_directory}/gold_ad_spend"
