import os

class Config:
    API_ID = os.getenv('API_ID', '25293525')
    API_HASH = os.getenv('API_HASH', '2fc1293e908b08187d519f79bb046018')
    PHONE_NUMBER = os.getenv('PHONE_NUMBER', '+13853495776')

config = Config()
