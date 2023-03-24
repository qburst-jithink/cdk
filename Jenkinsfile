pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building...'
                sh '''#!/bin/bash
                python -m pip install virtualenv
                cd $WORKSPACE
                python -m venv .venv
                ls -la
                source .venv/bin/activate
                pip install -r ./requirements.txt
                cdk destroy
                cdk synth
                cdk deploy'''
            }
        }
    }
}
