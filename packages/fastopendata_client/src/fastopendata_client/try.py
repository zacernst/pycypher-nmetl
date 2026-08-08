import requests

FORM = {"username": "johndoe", "password": "imapassword"}
response: requests.Response = requests.post("http://127.0.0.1:8000/token", data=FORM)

print(response.text)
