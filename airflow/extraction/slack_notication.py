import requests
import json
from datetime import datetime

url = "https://hooks.slack.com/services/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"
output_name = datetime.now().strftime("%Y-%m-%d")

payload = json.dumps({
  "text": f"Báo cáo ngày {output_name} : https://lookerstudio.google.com/s/slZaEIEj6aw"
})
headers = {
  'Content-type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)