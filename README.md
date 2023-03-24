# aws-cdk-glue

## Steps

### Install AWS CDK
Prerequisites: npm should be installed on the machine.

Please follow this documentation to install the AWS CDK: https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html

### Build the application

- Change to the cdk directory

`cd cdk/`

- Create a python virtual environment

`python -m venv .venv`

- Activate the virtual environment

`source .venv/bin/activate`

- Install all the required packages specified in the requirements.txt file:

`pip install -r requirements.txt`


- Run the following commands to build the application

`cdk synth`

`cdk bootstrap`

### Deploy the application
- Deploy the application using the following command:

`cdk deploy`

This will set up all the components needed for Glue job in AWS.
Run the following command in the terminal to run the glue crawler:

`aws glue start-crawler --name <crawler-name>`

This will populate all schema information in the Glue catalog database.
