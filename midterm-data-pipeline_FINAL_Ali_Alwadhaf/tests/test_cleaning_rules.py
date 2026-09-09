import sys
sys.path.insert(0,'.')
from src.quality_rules import clean_record
def base(**kw):
 r={"order_id":"طلب-1","order_date":"2025-01-31T10:00:00","status":"مؤكد","customer_id":"عميل-1","customer_name":"علي","customer_phone":"701234567","customer_email":"a@example.com","city":"عدن","district":"كريتر","delivery_type":"عادي","delivery_cost":"٥٠٠٠٫٠","payment_method":"بطاقة","payment_status":"مدفوع","payment_amount":"5,000 ريال","currency":"ريال يمني","total_amount":"10000","items_json":"[{\"sku\":\"S1\",\"qty\":1,\"unit_price\":10000}]"}; r.update(kw); return r
def test_arabic_currency_and_status():
 x=clean_record(base()); assert x['delivery_cost']==5000.0 and x['currency']=='YER' and x['payment_status']=='تم الدفع' and x['quality_status']=='corrected'
def test_corrupt_json_quarantine():
 x=clean_record(base(items_json='not-json')); assert x['quality_status']=='quarantined' and 'JSON_ITEMS_CORRUPTED' in x['error_codes']
def test_missing_id_quarantine():
 x=clean_record(base(order_id='')); assert 'ID_ORDER_MISSING' in x['error_codes']
