import pandas as pd
import re
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import boto3
from bs4 import BeautifulSoup
import re
load_dotenv(dotenv_path='creds.env')

class CreateEmailForwardingDataset():
    def __init__(self):
        super(CreateEmailForwardingDataset, self).__init__()
        dict = self.getText()
        self.uploadToCSV(dict)

    def getText(self):
        from_address_line = []
        transaction_date_line = []
        subject_line = []
        email_address_line = []
        html_path_ereceipt = []

        print("\n\nDataset are creating:",)
        path,filename_keys = self.downloadKey()
        for filename in filename_keys:
            text = self.cleanText(path,filename)
            from_address_pattern = re.compile(r'\-{1,}\s+From:.*\<(.*)\>\s+Date|Begin forwarded message:\s+\s+.*From:.*\<(.*)\>\s+.*Date|message:\s+On\s.*(?:AM|PM)\,\s(.*)\swrote')
            transaction_date_pattern = re.compile(r'\-{1,}\s+From:.*\s+Date\:\s(.*)\s\s+Subject|Begin forwarded message:\s+\s+.*From:.*\<.*\>\s+.*Date\:\s(.*(?:AM|PM))|message:\s+On\s(.*(?:AM|PM))\,\s.*\swrote')
            subject_pattern = re.compile(r'\-{1,}\s+From:.*\s+Date.*\s+Subject\:\s(.*)\s\s+|Begin forwarded message:\s+\s+.*From:.*\<.*\>\s+.*Date.*\s+.*To.*\s.*Subject\:\s(.*)\s|Subject:\sFw\:\s(.*\s+.*)')
            email_address_pattern = re.compile(r'\-{1,}\s+From:.*\s+Date.*\s+Subject.*\s+To.*\<(.*)\>|Begin forwarded message:\s+\s+.*From:.*\<.*\>\s+.*Date.*\s+.*To\:.*\<(.*)\s?\>|Begin forwarded message:\s+\s+.*From:.*\<.*\>\s+.*Date.*\s+.*To\:\s(.*)\s?|Date:.*\s+From:\s.*\<(.*)\s?\>')

            match_from_address = re.search(from_address_pattern, text)

            match_transaction_date = re.search(transaction_date_pattern, text)

            match_subject = re.search(subject_pattern, text)

            match_email_address = re.search(email_address_pattern, text)

            if match_from_address and match_transaction_date and match_subject and match_email_address:
                html_path_ereceipt.append(path + '/' + filename)
                if match_from_address.group(1):
                    from_address_line.append(match_from_address.group(1))
                if match_from_address.group(2):
                    from_address_line.append(match_from_address.group(2))
                if match_from_address.group(3):
                    from_address_line.append(match_from_address.group(3))
                if match_transaction_date.group(1):
                    transaction_date_line.append(match_transaction_date.group(1))
                if match_transaction_date.group(2):
                    transaction_date_line.append(match_transaction_date.group(2))
                if match_transaction_date.group(3):
                    transaction_date_line.append(match_transaction_date.group(3))
                if match_subject.group(1):
                    subject_line.append(match_subject.group(1))
                if match_subject.group(2):
                    subject_line.append(match_subject.group(2))
                if match_subject.group(3):
                    subject_line.append(match_subject.group(3))
                if match_email_address.group(1):
                    email_address_line.append(match_email_address.group(1))
                if match_email_address.group(2):
                    email_address_line.append(match_email_address.group(2))
                if match_email_address.group(3):
                    email_address_line.append(match_email_address.group(3))
                if match_email_address.group(4):
                    email_address_line.append(match_email_address.group(4))
                dict = {'html_path': html_path_ereceipt,
                    'from_address': from_address_line,
                    'transaction_date': transaction_date_line,
                    'subject': subject_line,
                    'email_address': email_address_line
                    }
                print("dict",dict)

        return dict

    def cleanText(self, path, filename):

        try:
            ACCESS_KEY_ID = os.getenv('ACCESS_KEY_ID')
            SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')
            s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
            email_object= s3.get_object( Bucket='tmp-nmr-receipthog-ereceipt-forward-test', Key= path+'/'+filename)
            ereceipthtml = email_object['Body'].read().decode('utf-8')

            return ereceipthtml

        except Exception as e:
            print(f'Error: {e}')
            return e

    def downloadKey(self):
        # Create Session
        session = boto3.Session(
            aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
        )
        bucket_key = []

        s3 = session.resource('s3')

        your_bucket = s3.Bucket('tmp-nmr-receipthog-ereceipt-forward-test')

        for s3_object in your_bucket.objects.all():

            path, filename = os.path.split(s3_object.key)
            bucket_key.append(filename)

        return path,bucket_key

    def uploadToCSV(self, dict):
        df = pd.DataFrame(dict)
        print("\n\nDataset are created",)
        df.to_csv("email_forwarding_dataset.csv", mode="w", index=[0])


if __name__ == "__main__":
    CreateEmailForwardingDataset()
