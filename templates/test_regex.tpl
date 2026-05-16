{
  "ssn": "{{regex "\\d{3}-\\d{2}-\\d{4}"}}",
  "phone": "{{regex "\\(\\d{3}\\) \\d{3}-\\d{4}"}}",
  "zip_code": "{{regex "\\d{5}"}}",
  "license_plate": "{{regex "[A-Z]{3}-\\d{4}"}}",
  "hex_color": "{{regex "#[0-9A-F]{6}"}}",
  "username": "{{regex "[a-z]{5,10}"}}",
  "product_code": "{{regex "[A-Z]{2}\\d{3}[A-Z]"}}",
  "timestamp": "{{now}}"
}