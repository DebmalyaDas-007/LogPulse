from spark_jobs.rules import brute_force, error_burst, latency_spike, traffic_surge


def test_latency_spike_is_strictly_above_three_sigma():
    assert latency_spike(131, 100, 10)["anomaly_type"] == "latency_spike"
    assert latency_spike(130, 100, 10) is None


def test_brute_force_uses_strict_threshold():
    assert brute_force(101)["severity"] == "critical"
    assert brute_force(100) is None


def test_error_burst_threshold_and_minimum_volume():
    assert error_burst(2, 100)["anomaly_type"] == "error_burst"
    assert error_burst(1, 100) is None
    assert error_burst(0, 0) is None
    assert error_burst(2, 3, min_requests=20) is None  # too little traffic to judge


def test_traffic_surge_needs_a_baseline_and_strict_multiplier():
    assert traffic_surge(51, 10)["anomaly_type"] == "traffic_surge"
    assert traffic_surge(50, 10) is None
    assert traffic_surge(1000, 0) is None
