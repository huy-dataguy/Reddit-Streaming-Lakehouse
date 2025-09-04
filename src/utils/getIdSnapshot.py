def getIdSnapshot(spark, path):
    snapshots = spark.sql(f"SELECT snapshot_id FROM {path}.snapshots ORDER BY committed_at DESC").take(2)
    startSnap = snapshots[0]['snapshot_id'] if len(snapshots) > 0 else None
    endSnap = snapshots[1]['snapshot_id'] if len(snapshots) > 1 else None
    return [startSnap, endSnap]

