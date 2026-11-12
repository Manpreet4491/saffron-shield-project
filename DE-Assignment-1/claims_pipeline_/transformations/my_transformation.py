from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Frozen evaluation date anchor
FROZEN_TRUTH_DATE = F.lit("2026-06-30").cast("date")

catalog = spark.conf.get("catalog")


@dp.temporary_view()
def validated_claims():
    # Load raw reference tables & bronze claims
    bronze_claims = spark.table(f"{catalog}.bronze.claims")
    raw_hospitals = spark.table(f"{catalog}.raw.hospitals")
    raw_members = spark.table(f"{catalog}.raw.members")
    raw_plans = spark.table(f"{catalog}.raw.plans")

    # 1. Deduplicate bronze claims (latest ingested record per claim - resubmissions
    # are judged on their latest version)
    claim_window = Window.partitionBy("claim_id").orderBy(F.col("_ingest_ts").desc())
    latest_claims = (
        bronze_claims
        .withColumn("_rn", F.row_number().over(claim_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 2. Referential integrity validation targets (Q-A)
    valid_hospitals = raw_hospitals.select("hospital_id").distinct()
    valid_members = raw_members.select("member_id").distinct()

    # Left joins to flag missing references (keeps every claim for review)
    claims_validated_refs = (
        latest_claims
        .join(valid_hospitals.withColumn("_hospital_match", F.lit(True)), "hospital_id", "left")
        .join(valid_members.withColumn("_member_match", F.lit(True)), "member_id", "left")
        .withColumn("_missing_hospital", F.col("_hospital_match").isNull())
        .withColumn("_missing_member", F.col("_member_match").isNull())
        .drop("_hospital_match", "_member_match")
    )

    # 3. Define Rule Logic (Q-A to Q-E)
    # Bronze columns are STRING; cast for date/numeric comparisons
    admission_dt = F.to_date(F.col("admission_date"))
    discharge_dt = F.to_date(F.col("discharge_date"))
    billed_amt = F.col("billed_amount").cast("double")
    approved_amt = F.col("approved_amount").cast("double")

    rule_q_a = F.when(
        F.col("_missing_hospital") | F.col("_missing_member"),
        F.lit("Q-A")
    ).otherwise(None)

    rule_q_b = F.when(
        (discharge_dt < admission_dt) |
        (admission_dt > FROZEN_TRUTH_DATE) |
        (discharge_dt > FROZEN_TRUTH_DATE),
        F.lit("Q-B")
    ).otherwise(None)

    rule_q_c = F.when(
        (billed_amt <= 0) | (approved_amt > billed_amt),
        F.lit("Q-C")
    ).otherwise(None)

    # Expected status derived from the claim's own fields
    expected_status = (
        F.when(F.col("rejection_code").isNotNull(), F.lit("rejected"))
        .when(F.col("settled_ts").isNotNull(), F.lit("settled"))
        .when(F.col("approved_amount").isNotNull(), F.lit("approved"))
        .otherwise(F.lit("submitted"))
    )
    rule_q_d = F.when(
        F.col("status") != expected_status,
        F.lit("Q-D")
    ).otherwise(None)

    rule_q_e = F.when(
        F.col("room_days").cast("int") != F.datediff(discharge_dt, admission_dt),
        F.lit("Q-E")
    ).otherwise(None)

    # 4. Apply rules & build warning flags + failed_rules array
    return (
        claims_validated_refs
        .withColumn("passes_q_a", rule_q_a.isNull())
        .withColumn("passes_q_b", rule_q_b.isNull())
        .withColumn("passes_q_c", rule_q_c.isNull())
        .withColumn("passes_q_d", rule_q_d.isNull())
        .withColumn("passes_q_e", rule_q_e.isNull())
        .withColumn(
            "failed_rules",
            F.array_compact(F.array(rule_q_a, rule_q_b, rule_q_c, rule_q_d, rule_q_e))
        )
        .drop("_missing_hospital", "_missing_member")
    )


# 5. Output split
# Quarantine Output: every failing claim, tagged with every rule it broke
@dp.materialized_view()
def claims_quarantine():
    return spark.table("validated_claims").filter(F.size(F.col("failed_rules")) > 0)


# Clean Output
@dp.materialized_view()
def claims_clean():
    return (
        spark.table("validated_claims")
        .filter(F.size(F.col("failed_rules")) == 0)
        .drop("failed_rules", "passes_q_a", "passes_q_b", "passes_q_c", "passes_q_d", "passes_q_e", "_ingest_ts", "_source_file")
    )