from test_pipline.clean import fix_mojibake, safe_date, safe_id

def test_fix_mojibake_simple():
    assert fix_mojibake("Journal \\xc3\\xb1") == "Journal ñ"

def test_safe_date():
    assert safe_date("2020-01-02") == "2020-01-02"
    assert safe_date("") is None

def test_safe_id():
    assert safe_id("12") == 12
    assert safe_id("NCT0001") == "NCT0001"
    assert safe_id("") is None
