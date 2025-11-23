from src.utils.storage_r2 import upload_us, list_us

# Upload a test file
upload_us("test_upload.txt", "test/test_upload.txt")
print("Upload complete.")

# List what's in the 'test/' folder
objects = list_us("test/")
print("Objects in R2 test folder:")
for obj in objects:
    print(obj["Key"], obj["Size"])


