import os
from mistralai import Mistral
import os

api_key = os.environ.get("MISTRAL_API_KEY")
if not api_key:
    raise ValueError("API key not found. Make sure the environment variable is set correctly.")

model = "mistral-large-latest"

client = Mistral(api_key=api_key)

chat_response = client.chat.complete(
    model=model,
    messages=[
        {
            "role": "user",
            "content": "Write a sql code which will bring data from table retail_data and calculated number of total sales by ship_modes. Output should be only a script",
        },
    ]
)
print(chat_response.choices[0].message.content)
