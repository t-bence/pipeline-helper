"""
pipeline-helper

A small utility that helps manage tables in pipelines
"""

class PipelineHelper:

    def __init__(self, checkpoint_root: str) -> None:
        self.checkpoint_root = checkpoint_root

    def create_table(self, table_name: str) -> Table:
        return Table(table_name, self.checkpoint_root)

class Table:
    def __init__(self, name: str, checkpoint_root: str) -> None:
        self.name = name
        self.checkpointPath = checkpoint_root + name

    def wipe(self) -> None:
        # delete data
        # spark.sql(f"DROP TABLE IF EXISTS {self.name}")
        # delete checkpoint
        # dbutils.fs.rm(self.checkpointPath, recurse=True)

