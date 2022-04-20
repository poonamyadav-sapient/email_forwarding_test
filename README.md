# email_forwarding_test
Write a script to parse forwarded email test receipts and store key data fields


#**Installation steps:**

Create a virtual environment by using command pipenv shell.
pipenv shell --python 3.7
 
 Run the command `pipenv install` to install all the packages.
 pip install -r requirements.txt
 
 **#Credentials require for accessing html page email forwarding from aws:**
 
Create a creds.env file in the same directory and add the below credentials in it-

`username=` SSO username

`password=` SSO password

`ACCESS_KEY_ID=` AWS access key id

`SECRET_ACCESS_KEY=` ASW access secret key


**Follow below command to use the script:**

To create csv file over email forwarding run below command

`python create_email_forwarding_dataset.py`
