import os
import weaviate

client = weaviate.Client(os.getenv("WEAVIATE_URL", "http://localhost:8080"))

result = client.query.get(
    "Review",
    ["asin", "reviewText_cleaned", "overall"]
).with_where({
    "path": ["asin"],
    "operator": "Equal",
    "valueString": "B000E5KJDE"
}).do()

print(result)