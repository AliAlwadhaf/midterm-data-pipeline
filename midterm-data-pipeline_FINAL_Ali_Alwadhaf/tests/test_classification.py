import sys
sys.path.insert(0,'.')
from src.quality_rules import clean_record
def test_valid_record():
 r={"order_id":"o1","order_date":"2025-01-31T10:00:00","customer_id":"c1","customer_phone":"701234567","customer_email":"a@example.com","delivery_cost":"2000.0","payment_amount":"2000.0","currency":"YER","payment_status":"تم الدفع","total_amount":"10000.0","items_json":"[{\"qty\":1,\"unit_price\":10000.0}]"}
 x=clean_record(r); assert x['quality_status']=='valid'
def test_bad_email_quarantine():
 r={"order_id":"o1","customer_id":"c1","customer_phone":"12345","customer_email":"user-without-domain","delivery_cost":"2000","payment_amount":"2000","currency":"YER","total_amount":"10000","items_json":"[]"}
 x=clean_record(r); assert x['quality_status']=='quarantined'
