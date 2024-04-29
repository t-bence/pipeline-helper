from .helper import PipelineHelper

helper = PipelineHelper(
    checkpoint_root = "dbfs:/tmp/checkpoints/bubi_projekt/"
)

stations_silver = helper.create_table("stations_silver")

(spark
    .writeStream
    .checkpointPath(stations_silver.checkpointPath)
    .saveAsTable(stations_silver.tableName)
)

stations_silver.wipe()

