from conftest import load_module

silver_mod = load_module("notebooks/02_silver.py", "pipeline_e2e_silver")


def test_record_computes_pass_rate_and_failed():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("deduplicacao", total=1000, passed=990)

    metric = qm.metrics["deduplicacao"]
    assert metric["total"] == 1000
    assert metric["passed"] == 990
    assert metric["failed"] == 10
    assert metric["pass_rate"] == 99.0


def test_record_handles_zero_total_without_dividing_by_zero():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("check_vazio", total=0, passed=0)

    assert qm.metrics["check_vazio"]["pass_rate"] == 0.0


def test_assert_pass_rate_no_failure_when_above_threshold():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("check", total=100, passed=95)
    qm.assert_pass_rate("check", min_rate=0.90)

    assert qm.failures == []


def test_assert_pass_rate_records_failure_when_below_threshold():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("check", total=100, passed=80)
    qm.assert_pass_rate("check", min_rate=0.90)

    assert len(qm.failures) == 1
    assert "check" in qm.failures[0]


def test_assert_pass_rate_missing_check_counts_as_zero():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.assert_pass_rate("nunca_registrado", min_rate=0.50)

    assert len(qm.failures) == 1


def test_summary_true_when_no_failures():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("check", total=100, passed=100)
    qm.assert_pass_rate("check", min_rate=0.90)

    assert qm.summary() is True


def test_summary_false_when_any_failure():
    qm = silver_mod.QualityMetrics("silver.orders")
    qm.record("check", total=100, passed=10)
    qm.assert_pass_rate("check", min_rate=0.90)

    assert qm.summary() is False
