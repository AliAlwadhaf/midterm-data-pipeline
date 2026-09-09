"""Deterministic cleaning and classification rules for the provided Yemeni orders CSV."""

import json
import re

from datetime import datetime
from decimal import Decimal, InvalidOperation


# =========================================================
# Arabic digits and decimal separators
# =========================================================

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩",
    "0123456789",
)

ARABIC_DECIMAL = str.maketrans(
    "٫٬",
    ".,",
)


# =========================================================
# Selected Arabic number words
# =========================================================

NUMBER_WORDS = {
    "ألفان": 2000,
    "الفان": 2000,
    "ألفين": 2000,
    "خمسة آلاف": 5000,
    "خمسه آلاف": 5000,
    "خمسة الاف": 5000,
}


# =========================================================
# Helper functions
# =========================================================

def _text(value):
    """Convert a value to trimmed text."""
    return "" if value is None else str(value).strip()


def _record_change(changes, field, original, corrected, rule):
    """Record a correction only when the value actually changed."""

    if str(original) != str(corrected):
        changes.append(
            {
                "field": field,
                "original_value": original,
                "corrected_value": corrected,
                "rule_code": rule,
            }
        )


def _number(value, field, changes):
    """
    Normalize a numeric value.

    Supports:
    - Arabic digits
    - Arabic decimal separator
    - Arabic/English currency text
    - selected Arabic number words
    """

    original = value

    s = (
        _text(value)
        .translate(ARABIC_DIGITS)
        .translate(ARABIC_DECIMAL)
    )

    if not s:
        return None

    # Arabic number words
    if s in NUMBER_WORDS:
        output = Decimal(NUMBER_WORDS[s])

        _record_change(
            changes,
            field,
            original,
            float(output),
            "NUMBER_WORD_TO_DIGITS",
        )

        return output

    # Remove known currency text
    s = re.sub(
        r"(?i)(ريال|لاير|yer)",
        "",
        s,
    )

    # Remove spaces
    s = s.replace(" ", "").strip()

    # Remove thousands separators
    s = s.replace(",", "")

    try:
        output = Decimal(s)

        if str(original) != str(output):
            _record_change(
                changes,
                field,
                original,
                float(output),
                "ARABIC_DIGITS_CURRENCY_NORMALIZATION",
            )

        return output

    except (InvalidOperation, ValueError):
        return None


# =========================================================
# Items JSON parsing
# =========================================================

def _parse_items_json(value, changes):
    """
    Safely parse items_json using deterministic normalization steps.

    The CSV may contain JSON with doubled quotes, escaped quotes,
    surrounding quotes, or Python-style single quotes.
    """

    original = _text(value)

    if not original:
        return None, "ITEMS_EMPTY", "items_json is empty"

    candidates = []

    def add_candidate(candidate, rule_code=None):
        if not isinstance(candidate, str):
            return
        candidate = candidate.strip()
        if candidate and all(candidate != x[0] for x in candidates):
            candidates.append((candidate, rule_code))

    # 1. Original value.
    add_candidate(original)

    # 2. CSV doubled quotation marks.
    normalized = original.replace('""', '"')
    add_candidate(normalized, "JSON_QUOTE_NORMALIZATION")

    # 3. Unescape JSON quotes.
    unescaped = normalized.replace('\\"', '"')
    add_candidate(unescaped, "JSON_ESCAPE_NORMALIZATION")

    # 4. Remove one outer pair of quotes if the complete JSON
    # payload was itself stored as a quoted string.
    if len(unescaped) >= 2 and unescaped[0] == '"' and unescaped[-1] == '"':
        unwrapped = unescaped[1:-1].replace('\\"', '"')
        add_candidate(
            unwrapped,
            "JSON_OUTER_QUOTE_NORMALIZATION",
        )

    # 5. Extract an embedded JSON array if extra surrounding
    # characters exist.
    left = unescaped.find("[")
    right = unescaped.rfind("]")
    if left >= 0 and right > left:
        add_candidate(
            unescaped[left:right + 1],
            "JSON_ARRAY_EXTRACTION",
        )

    # Try all JSON candidates.
    for candidate, rule_code in candidates:
        try:
            items = json.loads(candidate)

            if not isinstance(items, list):
                continue

            if candidate != original:
                _record_change(
                    changes,
                    "items_json",
                    original,
                    candidate,
                    rule_code or "JSON_NORMALIZATION",
                )

            return items, None, None

        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    # Last deterministic fallback for Python-style lists.
    try:
        import ast

        parsed = ast.literal_eval(normalized)

        if isinstance(parsed, list):
            corrected = json.dumps(
                parsed,
                ensure_ascii=False,
                separators=(",", ":"),
            )

            _record_change(
                changes,
                "items_json",
                original,
                corrected,
                "JSON_PYTHON_LITERAL_NORMALIZATION",
            )

            return parsed, None, None

    except (ValueError, SyntaxError, TypeError):
        pass

    return (
        None,
        "JSON_ITEMS_CORRUPTED",
        "items_json is not valid JSON",
    )

# =========================================================
# Main cleaning function
# =========================================================

def clean_record(raw):
    """
    Apply deterministic cleaning and classification rules.

    Returns a record with:

    - quality_status
    - corrections
    - error_codes
    - error_details
    """

    r = dict(raw)

    changes = []
    errors = []

    # =====================================================
    # Rule 1: Trim whitespace from text fields
    # =====================================================

    for field in r:

        if isinstance(r[field], str):

            old = r[field]

            r[field] = r[field].strip()

            _record_change(
                changes,
                field,
                old,
                r[field],
                "TRIM_WHITESPACE",
            )

    # =====================================================
    # Rule 2: Mandatory business keys
    # =====================================================

    if not r.get("order_id"):

        errors.append(
            (
                "ID_ORDER_MISSING",
                "order_id is empty",
            )
        )

    if not r.get("customer_id"):

        errors.append(
            (
                "ID_CUSTOMER_MISSING",
                "customer_id is empty",
            )
        )

    # =====================================================
    # Rule 3: Numeric normalization
    # =====================================================

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:

        value = _number(
            r.get(field),
            field,
            changes,
        )

        if value is None:

            errors.append(
                (
                    "PRICE_UNKNOWN",
                    f"{field} cannot be parsed as a number",
                )
            )

        else:

            r[field] = float(value)

            if value < 0:

                errors.append(
                    (
                        "VALUE_NEGATIVE_AMBIGUOUS",
                        f"{field} is negative",
                    )
                )

    # =====================================================
    # Rule 4: Currency standardization
    # =====================================================

    currency = _text(r.get("currency"))

    if currency in (
        "ريال يمني",
        "لاير يمني",
        "YER",
        "yer",
    ):

        _record_change(
            changes,
            "currency",
            currency,
            "YER",
            "CURRENCY_STANDARDIZATION",
        )

        r["currency"] = "YER"

    else:

        errors.append(
            (
                "UNKNOWN_CURRENCY",
                f"Unsupported currency: {currency}",
            )
        )

    # =====================================================
    # Rule 5: Phone normalization
    # =====================================================

    old_phone = r.get(
        "customer_phone",
        "",
    )

    phone = _text(old_phone)

    phone = phone.translate(
        ARABIC_DIGITS
    )

    # Keep digits only
    phone = re.sub(
        r"[^0-9]",
        "",
        phone,
    )

    _record_change(
        changes,
        "customer_phone",
        old_phone,
        phone,
        "PHONE_NORMALIZATION",
    )

    r["customer_phone"] = phone

    if len(phone) < 7:

        errors.append(
            (
                "INVALID_PHONE",
                "Phone has fewer than 7 digits",
            )
        )

    # =====================================================
    # Rule 6: Email normalization and validation
    # =====================================================

    old_email = r.get(
        "customer_email",
        "",
    )

    email = _text(old_email)

    fixed_email = (
        email
        .replace("@@", "@")
        .replace("..", ".")
    )

    _record_change(
        changes,
        "customer_email",
        old_email,
        fixed_email,
        "EMAIL_REPEATED_SYMBOLS",
    )

    r["customer_email"] = fixed_email

    if not re.fullmatch(
        r"[^@\s]+@[^@\s]+\.[^@\s]+",
        fixed_email,
    ):

        errors.append(
            (
                "INVALID_EMAIL",
                "Email is not safely valid",
            )
        )

    # =====================================================
    # Rule 7: Date normalization and validation
    # =====================================================

    old_date = r.get(
        "order_date",
        "",
    )

    try:

        date_value = datetime.fromisoformat(
            _text(old_date).replace(
                "Z",
                "+00:00",
            )
        )

        r["order_date"] = date_value.isoformat()

        _record_change(
            changes,
            "order_date",
            old_date,
            r["order_date"],
            "DATE_STANDARDIZATION",
        )

    except (
        ValueError,
        TypeError,
    ):

        errors.append(
            (
                "DATE_IMPOSSIBLE_INVALID",
                "Date cannot be parsed",
            )
        )

    # =====================================================
    # Rule 8: Payment status canonicalization
    # =====================================================

    aliases = {
        "مدفوع": "تم الدفع",
        "مؤكد ": "مؤكد",
        "مؤكد": "مؤكد",
        "تم الدفع": "تم الدفع",
    }

    old_status = r.get(
        "payment_status",
        "",
    )

    new_status = aliases.get(
        old_status,
        old_status,
    )

    _record_change(
        changes,
        "payment_status",
        old_status,
        new_status,
        "STATUS_CANONICALIZATION",
    )

    r["payment_status"] = new_status

    # =====================================================
    # Rule 9: Items JSON validation
    # =====================================================

    old_items = r.get(
        "items_json",
        "",
    )

    items, json_error_code, json_error_detail = _parse_items_json(
        old_items,
        changes,
    )

    if json_error_code is not None:

        errors.append(
            (
                json_error_code,
                json_error_detail,
            )
        )

    else:

        # -------------------------------------------------
        # JSON must contain a non-empty list
        # -------------------------------------------------

        if not isinstance(items, list) or not items:

            errors.append(
                (
                    "ITEMS_EMPTY",
                    "items_json contains no items",
                )
            )

        else:

            # -------------------------------------------------
            # Calculate total from items
            # -------------------------------------------------

            calculated_total = Decimal("0")

            for item in items:

                if not isinstance(
                    item,
                    dict,
                ):

                    raise_value = ValueError(
                        "Item is not an object"
                    )

                    errors.append(
                        (
                            "JSON_ITEMS_CORRUPTED",
                            str(raise_value),
                        )
                    )

                    break

                try:

                    quantity = Decimal(
                        str(
                            item.get(
                                "qty",
                                0,
                            )
                        )
                    )

                    raw_unit_price = item.get(
                        "unit_price",
                        item.get(
                            "price",
                            item.get(
                                "unitPrice",
                                0,
                            ),
                        ),
                    )

                    unit_price = Decimal(
                        str(raw_unit_price)
                    )

                    calculated_total += (
                        quantity * unit_price
                    )

                except (
                    InvalidOperation,
                    ValueError,
                    TypeError,
                ) as exc:

                    errors.append(
                        (
                            "PRICE_UNKNOWN",
                            f"Item price or quantity cannot be parsed: {exc}",
                        )
                    )

                    break

            # -------------------------------------------------
            # Compare calculated total with current total
            # -------------------------------------------------

            current_total = None

            if isinstance(
                r.get("total_amount"),
                (int, float),
            ):

                current_total = Decimal(
                    str(
                        r["total_amount"]
                    )
                )

            if (
                current_total is not None
                and calculated_total >= 0
                and current_total != calculated_total
            ):

                _record_change(
                    changes,
                    "total_amount",
                    r["total_amount"],
                    float(calculated_total),
                    "RECALCULATE_TOTAL_FROM_ITEMS",
                )

                r["total_amount"] = float(
                    calculated_total
                )

    # =====================================================
    # Rule 10: Multiple conflicting errors
    # =====================================================

    # Required by the project specification when a record
    # contains multiple quality problems.

    if len(errors) >= 2:

        existing_codes = {
            error[0]
            for error in errors
        }

        if "ERRORS_CONFLICTING_MULTIPLE" not in existing_codes:

            errors.append(
                (
                    "ERRORS_CONFLICTING_MULTIPLE",
                    f"Multiple errors detected: {len(errors)}",
                )
            )

    # =====================================================
    # Final classification
    # =====================================================

    if errors:

        status = "quarantined"

    elif changes:

        status = "corrected"

    else:

        status = "valid"

    r["quality_status"] = status

    r["corrections"] = changes

    r["error_codes"] = sorted(
        set(
            error[0]
            for error in errors
        )
    )

    r["error_details"] = [
        error[1]
        for error in errors
    ]

    return r