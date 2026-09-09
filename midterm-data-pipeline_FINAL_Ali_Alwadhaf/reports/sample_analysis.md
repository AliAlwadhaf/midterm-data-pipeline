# تقرير اختبار العينة

## ملخص

تم اختبار ملف `orders_sample_1000.csv` الذي يحتوي على 1,000 سجل. حجم الملف النهائي المسجل هو 0.412 MB، ولذلك يختار Router مسار Python Batch لأن الحجم أقل من الحد 200 MB.

في آخر تشغيل ناجح للعينة كانت النتائج:

- Valid: 10
- Corrected: 868
- Quarantined: 122
- المجموع: 1,000

وهذا يحقق اتساق عدد السجلات: 10 + 868 + 122 = 1,000.

## أهم أخطاء الجودة في التشغيل النهائي

| رمز الخطأ | العدد |
|---|---:|
| `ERRORS_CONFLICTING_MULTIPLE` | 5 |
| `ID_CUSTOMER_MISSING` | 15 |
| `INVALID_EMAIL` | 14 |
| `JSON_ITEMS_CORRUPTED` | 13 |
| `PRICE_UNKNOWN` | 17 |
| `DATE_IMPOSSIBLE_INVALID` | 35 |
| `ID_ORDER_DUPLICATE` | 5 |
| `UNKNOWN_CURRENCY` | 4 |
| `VALUE_NEGATIVE_AMBIGUOUS` | 4 |
| `ID_ORDER_MISSING` | 10 |
| `INVALID_PHONE` | 10 |
| `ITEMS_EMPTY` | 10 |

## القواعد المطبقة

يطبق البرنامج أكثر من ثماني قواعد، من بينها:

- إزالة المسافات الزائدة
- التحقق من `order_id`
- التحقق من `customer_id`
- تطبيع الأرقام
- التعامل مع العملة
- تطبيع الهاتف
- التحقق من البريد الإلكتروني
- توحيد التاريخ
- توحيد حالة الدفع
- تحليل `items_json`
- إعادة حساب الإجمالي عند صلاحية بيانات العناصر
- كشف التكرار
- تسجيل الأخطاء المتعددة المتعارضة

## ELT

يصل كل سجل أولًا إلى `orders_raw` قبل تطبيق قواعد الجودة. ثم يُصنف إلى `valid` أو `corrected` أو `quarantined`.

السجلات المصححة تحتوي على `corrections` لتوفير أثر تدقيق للتعديل، بينما السجلات المعزولة تحتوي على `error_codes` و`error_details` وتحفظ في `orders_quarantine`.

## اختبار Idempotency

في التشغيل الأول النهائي للعينة:

- `count_inserted = 878`
- `count_updated = 0`
- `count_unchanged = 0`

وعند إعادة تشغيل نفس العينة:

- `count_inserted = 0`
- `count_updated = 0`
- `count_unchanged = 878`

وهذا دليل عملي على عدم إنشاء Business Records جديدة عند إعادة نفس الإدخال.

## اختبار الملف الكبير

تم تشغيل الملف الأصلي بنجاح باستخدام PySpark:

- 30,000,000 سجل
- 12,650.32 MB
- 99 input partitions
- 123,767 Valid
- 26,356,400 Corrected
- 3,519,833 Quarantined
- Throughput ≈ 2,321.84 rows/s

كما أن مجموع Valid + Corrected + Quarantined يساوي 30,000,000.
