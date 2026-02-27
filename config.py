import os


API_TOKEN = (os.getenv("8511008734:AAGaxwKQYQAFQD-EFRCp5IbpNf-Uxt91NYI") or "").strip()
LZT_API_KEY = (os.getenv("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiJ9.eyJzdWIiOjYyOTA2MzYsImlzcyI6Imx6dCIsImlhdCI6MTc3MTYwMzgxNCwianRpIjoiOTM0MzY3Iiwic2NvcGUiOiJiYXNpYyByZWFkIHBvc3QgY29udmVyc2F0ZSBwYXltZW50IGludm9pY2UgY2hhdGJveCBtYXJrZXQiLCJleHAiOjE5MjkyODM4MTR9.C1pcTXAoG5AhQfSK9k3iXxGjCG7m2NN2qfIUAloUaUzrr8hrgb5qi9HGX-Tz4Ax3YUgJn469ClaaJcu-ElYkApHy9Wi8VvQyOnoSavALTuiKyZUGIRZq_-kpr1qr8hrdqWuuRvnhlxp169ABWD_Ong0nV61N_CmeeIR9iPsEVtw") or "").strip()

# Legacy constants kept for backward compatibility with older deployments.
LZT_URL = (os.getenv("LZT_URL") or "https://api.lzt.market/mihoyo?per_page=69&order_by=date_to_down").strip()
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))
